# Only GET requests to Stripe. No charges, cancellation or invoice mutations.
stripe_list_subscriptions <- function(cursor=NULL) {
  query <- list(status="all",limit=100)
  if(!is.null(cursor)) query$starting_after <- cursor
  r <- httr::GET("https://api.stripe.com/v1/subscriptions",
    httr::authenticate(Sys.getenv("STRIPE_SECRET_KEY"),""),httr::timeout(15),
    query=query,httr::add_headers(`Stripe-Version`="2025-03-31.basil"))
  httr::stop_for_status(r)
  httr::content(r,as="parsed",type="application/json")
}
stripe_reconciliation_check <- function(ok,detail) {
  .with_conn(DBI::dbExecute(con,"INSERT INTO operational_checks(name,ok,checked_at,detail)
    VALUES('stripe_reconciliation',$1,now(),$2) ON CONFLICT(name) DO UPDATE
    SET ok=excluded.ok,checked_at=excluded.checked_at,detail=excluded.detail",params=list(ok,detail)))
}
stripe_reconcile_once <- function(budget_seconds=240,max_pages=100L) {
  key <- Sys.getenv("STRIPE_SECRET_KEY")
  if(!grepl("^sk_(test|live)_[A-Za-z0-9]+$",key) || !nzchar(Sys.getenv("STRIPE_PRICE_ID"))) stop("Billing configuration missing")
  started <- Sys.time()
  budget <- function() {
    if(as.numeric(difftime(Sys.time(),started,units="secs"))>budget_seconds) stop("Reconciliation time budget exceeded")
  }
  # A transaction-scoped lock also works through transaction-pooling proxies.
  .with_conn(DBI::dbWithTransaction(con, {
    acquired <- DBI::dbGetQuery(con,"SELECT pg_try_advisory_xact_lock(hashtext('puga-stripe-reconciliation-v1')) AS acquired")$acquired[[1]]
    if(!isTRUE(acquired)) list(skipped=TRUE) else {
      stripe_reconciliation_check(FALSE,"Reconciliación de Stripe en curso; revisar si no termina.")
      users <- DBI::dbGetQuery(con,"SELECT id,stripe_subscription_id FROM users")
      known <- unique(users$stripe_subscription_id[!is.na(users$stripe_subscription_id) & nzchar(users$stripe_subscription_id)])
      seen <- character(); checked <- 0L; failed <- 0L
      apply_one <- function(sid) {
        budget()
        DBI::dbGetQuery(con,"SELECT 1") # Keep the lock transaction alive during remote reads.
        if(!grepl("^sub_[A-Za-z0-9]+$",sid)) {failed <<- failed+1L; return(invisible(NULL))}
        tryCatch({
          process_stripe_event(list(type="customer.subscription.updated",livemode=startsWith(key,"sk_live_"),
            data=list(object=list(id=sid))),reconcile=TRUE)
          checked <<- checked+1L
        },error=function(e) {failed <<- failed+1L})
      }
      # Known links are always checked, even if absent from Stripe's list (e.g. test clocks).
      for(sid in known) {apply_one(sid); seen <- c(seen,sid)}
      cursor <- NULL; complete <- FALSE
      for(page_number in seq_len(max_pages)) {
        budget()
        DBI::dbGetQuery(con,"SELECT 1")
        page <- stripe_list_subscriptions(cursor)
        if(!is.list(page$data) || !is.logical(page$has_more) || length(page$has_more)!=1L) stop("Invalid subscription page")
        ids <- vapply(page$data,function(s) s$id %||% "",character(1))
        if(any(!grepl("^sub_[A-Za-z0-9]+$",ids)) || anyDuplicated(ids)) stop("Invalid subscription page IDs")
        for(sub in page$data) {
          uid <- sub$metadata$user_id %||% ""
          if(!sub$id %in% seen && uid %in% users$id) {apply_one(sub$id); seen <- c(seen,sub$id)}
        }
        if(!isTRUE(page$has_more)) {complete <- TRUE; break}
        if(!length(ids) || identical(tail(ids,1),cursor)) stop("Pagination did not advance")
        cursor <- tail(ids,1)
      }
      if(!complete) stop("Reconciliation page budget exceeded")
      detail <- if(failed) sprintf("Reconciliación de Stripe incompleta: %d comprobadas, %d con error. Revisar facturación.",checked,failed) else
        sprintf("Reconciliación de Stripe correcta: %d suscripciones comprobadas.",checked)
      stripe_reconciliation_check(failed==0L,detail)
      list(skipped=FALSE,checked=checked,failed=failed)
    }
  }))
}
