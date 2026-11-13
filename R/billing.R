# Always fetch the current subscription under a transaction-scoped lock.
stripe_id <- function(x) if (is.list(x)) x$id %||% "" else x %||% ""
stripe_fetch_subscription <- function(id) {
  stopifnot(grepl("^sub_[A-Za-z0-9]+$",id))
  r <- httr::GET(paste0("https://api.stripe.com/v1/subscriptions/",id),
    httr::authenticate(Sys.getenv("STRIPE_SECRET_KEY"),""),httr::timeout(15),
    query=list("expand[]"="latest_invoice"),
    httr::add_headers(`Stripe-Version`="2025-03-31.basil"))
  httr::stop_for_status(r)
  httr::content(r,as="parsed",type="application/json")
}
stripe_access <- function(sub,price,now=as.numeric(Sys.time())) {
  items <- Filter(function(i) identical(stripe_id(i$price),price),sub$items$data %||% list())
  if (length(items)!=1L) return(list(matching=FALSE,active=FALSE,expires=NA_real_))
  expiry <- as.numeric(items[[1]]$current_period_end %||% sub$current_period_end %||% NA_real_)
  paid_invoice <- is.list(sub$latest_invoice) && identical(sub$latest_invoice$status,"paid")
  list(matching=TRUE,active=identical(sub$status,"active") && paid_invoice && is.finite(expiry) && expiry>now,expires=expiry)
}
process_stripe_event <- function(event, reconcile=FALSE) {
  supported <- c("checkout.session.completed","checkout.session.async_payment_succeeded","invoice.paid",
    "invoice.payment_failed","customer.subscription.updated","customer.subscription.deleted")
  if (!event$type %in% supported) return(FALSE)
  if (!nzchar(Sys.getenv("STRIPE_PRICE_ID")) || !nzchar(Sys.getenv("STRIPE_SECRET_KEY"))) stop("Billing configuration missing")
  o <- event$data$object
  sid <- if (startsWith(event$type,"customer.subscription.")) o$id else stripe_id(o$subscription %||% o$parent$subscription_details$subscription)
  if (!nzchar(sid)) return(FALSE)
  if (!identical(isTRUE(event$livemode),startsWith(Sys.getenv("STRIPE_SECRET_KEY"),"sk_live_"))) stop("Stripe mode mismatch")
  .with_conn({
    DBI::dbWithTransaction(con, {
      DBI::dbGetQuery(con,"SELECT pg_advisory_xact_lock(hashtext($1))",params=list(sid))
      done <- if(reconcile) data.frame() else DBI::dbGetQuery(con,"SELECT 1 FROM stripe_events WHERE event_id=$1",params=list(event$id))
      if (nrow(done)) FALSE else {
        sub <- stripe_fetch_subscription(sid)
        if(reconcile && (!identical(sub$id,sid) || !is.logical(sub$livemode) || length(sub$livemode)!=1L ||
            !identical(sub$livemode,startsWith(Sys.getenv("STRIPE_SECRET_KEY"),"sk_live_")))) stop("Subscription identity or mode mismatch")
        # Reconciliation uses the fresh invoice, never a synthetic payment assertion.
        if(reconcile) o <- sub$latest_invoice
        a <- stripe_access(sub,Sys.getenv("STRIPE_PRICE_ID"))
        linked <- if(reconcile) DBI::dbGetQuery(con,"SELECT id FROM users WHERE stripe_subscription_id=$1",params=list(sid)) else data.frame()
        if(nrow(linked)>1L) stop("Subscription linked to multiple users")
        if (a$matching || (reconcile && nrow(linked)==1L)) {
          uid <- sub$metadata$user_id %||% if(startsWith(event$type,"checkout.")) o$client_reference_id else NULL
          if(reconcile && nrow(linked)==1L && !is.null(uid) && !identical(tolower(uid),tolower(linked$id[[1]]))) stop("Subscription user mismatch")
          if (!is.null(uid) && !grepl("^[a-fA-F0-9-]{36}$",uid)) stop("Invalid user reference")
          u <- if(is.null(uid)) DBI::dbGetQuery(con,"SELECT * FROM users WHERE stripe_subscription_id=$1 FOR UPDATE",params=list(sid)) else
            DBI::dbGetQuery(con,"SELECT * FROM users WHERE id=$1 FOR UPDATE",params=list(uid))
          if(nrow(u)!=1L) stop("Subscription user not yet linked")
          old <- u$stripe_subscription_id[[1]] %||% ""
          replace_old <- FALSE
          if(nzchar(old) && !identical(old,sid) && !is.null(uid) && isTRUE(a$active)) {
            previous <- stripe_fetch_subscription(old)
            replace_old <- previous$status %in% c("canceled","incomplete_expired")
          }
          if (!nzchar(old) || identical(old,sid) || replace_old) {
            manual <- u$manual_access_expires_at[[1]]
            m <- !is.na(manual) && manual>Sys.time()
            expiry <- if(is.finite(a$expires)) as.POSIXct(a$expires,origin="1970-01-01",tz="UTC") else Sys.time()
            if(m) expiry <- max(expiry,manual)
            active <- isTRUE(a$active) || m
            DBI::dbExecute(con,"UPDATE users SET stripe_customer_id=$1,stripe_subscription_id=$2,stripe_status=$3,
              stripe_cancel_at_period_end=$4,membership_active=$5,membership_expires_at=$6,
              membership_activated_at=CASE WHEN $5 THEN coalesce(membership_activated_at,now()) ELSE membership_activated_at END,
              trial_expires_at=CASE WHEN $5 THEN NULL ELSE trial_expires_at END WHERE id=$7",
              params=list(stripe_id(sub$customer),sid,sub$status,isTRUE(sub$cancel_at_period_end),active,format(expiry,"%Y-%m-%d %H:%M:%OS%z",tz="UTC"),u$id[[1]]))
            paid <- isTRUE(a$active) && (((reconcile || identical(event$type,"invoice.paid")) && as.numeric(o$amount_paid %||% 0)>0) ||
              (startsWith(event$type,"checkout.") && identical(o$payment_status,"paid") && as.numeric(o$amount_total %||% 0)>0))
            ref <- u$referred_by[[1]]
            if(paid && !is.na(ref) && nzchar(ref) && !identical(ref,u$id[[1]])) {
              r <- DBI::dbGetQuery(con,"SELECT id,username,email FROM users WHERE id::text=$1",params=list(ref))
              if(nrow(r) && !identical(tolower(r$username[[1]]),tolower(Sys.getenv("DEFAULT_REFERRER_USERNAME"))) &&
                !identical(tolower(r$email[[1]]),tolower(Sys.getenv("SUPER_ADMIN_EMAIL")))) {
                amount <- as.numeric(Sys.getenv("REFERRAL_COMMISSION_AMOUNT","9.00"))
                if(!is.finite(amount) || amount<0) stop("Invalid commission")
                n <- DBI::dbExecute(con,"INSERT INTO referral_commissions(user_id,referrer_id,payment_id,amount) VALUES($1,$2,$3,$4)
                  ON CONFLICT(user_id) DO NOTHING",params=list(u$id[[1]],ref,o$id,amount))
                if(n==1L) DBI::dbExecute(con,"UPDATE users SET referral_wallet=coalesce(referral_wallet,0)+$1 WHERE id::text=$2",params=list(amount,ref))
              }
            }
          }
        }
        if(!reconcile) DBI::dbExecute(con,"INSERT INTO stripe_events(event_id,event_type,subscription_id) VALUES($1,$2,$3)",params=list(event$id,event$type,sid))
        TRUE
      }
    })
  })
}
verify_stripe_signature <- function(payload_str,sig_header,secret,now=as.numeric(Sys.time())) {
  if(is.null(sig_header) || !nzchar(sig_header) || !nzchar(secret)) return(FALSE)
  parts <- trimws(strsplit(sig_header,",",fixed=TRUE)[[1]])
  t <- sub("^t=","",grep("^t=",parts,value=TRUE))
  v <- sub("^v1=","",grep("^v1=",parts,value=TRUE))
  if(length(t)!=1L || !length(v)) return(FALSE)
  stamp <- suppressWarnings(as.numeric(t))
  if(!is.finite(stamp) || abs(now-stamp)>300) return(FALSE)
  expected <- charToRaw(digest::hmac(secret,paste0(t,".",payload_str),algo="sha256",serialize=FALSE))
  matched <- FALSE
  for(sig in v) {
    b <- charToRaw(sig)
    if(length(b)!=length(expected)) next
    d <- 0L
    for(i in seq_along(b)) d <- bitwOr(d,bitwXor(as.integer(b[i]),as.integer(expected[i])))
    matched <- matched || d==0L
  }
  matched
}
handle_stripe_webhook <- function(req) {
  response <- function(status,text) shiny::httpResponse(status,"text/plain",text,list("Cache-Control"="no-store"))
  if(!identical(req$REQUEST_METHOD,"POST")) return(response(405,"Method Not Allowed"))
  secret <- Sys.getenv("STRIPE_WEBHOOK_SECRET")
  if(!nzchar(secret)) return(response(503,"Webhook not configured"))
  tryCatch({
    payload <- rawToChar(req$rook.input$read(1048577L))
    if(nchar(payload,type="bytes")>1048576L) return(response(413,"Payload too large"))
    if(!verify_stripe_signature(payload,req$HTTP_STRIPE_SIGNATURE,secret)) return(response(401,"Invalid Signature"))
    event <- jsonlite::fromJSON(payload,simplifyVector=FALSE)
    if(!is.character(event$id) || length(event$id)!=1L || !is.character(event$type) || length(event$type)!=1L) return(response(400,"Invalid event"))
    process_stripe_event(event)
    response(200,"OK")
  },error=function(e) {
    message("[Stripe] Processing failed: ",conditionMessage(e))
    response(500,"Processing failed; retry required")
  })
}
