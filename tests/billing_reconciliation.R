# Included by database.R against the disposable database, never production.
source("R/billing_reconciliation.R")
subscription$livemode <- FALSE
subscription$latest_invoice <- list(id="in_fixture",status="paid",amount_paid=2999)
third <- "00000000-0000-0000-0000-000000000040"
DBI::dbExecute(con,"INSERT INTO users(id,username,email,active,email_verified,referred_by) VALUES($1,'missed_checkout','missed@example.invalid',true,true,$2)",params=list(third,ref))
new_sub <- subscription;new_sub$id <- "sub_discovered";new_sub$metadata$user_id <- third
registry <- list(sub_fixture=subscription,sub_discovered=new_sub)
stripe_fetch_subscription <- function(id) {
  if(is.null(registry[[id]])) stop("simulated lookup failure")
  registry[[id]]
}
pages <- character()
stripe_list_subscriptions <- function(cursor=NULL) {
  pages <<- c(pages,cursor %||% "first")
  if(is.null(cursor)) list(data=list(registry$sub_fixture),has_more=TRUE) else
    list(data=list(registry$sub_discovered),has_more=FALSE)
}
DBI::dbExecute(con,"UPDATE users SET membership_active=false WHERE id=$1",params=list(uid))
result <- stripe_reconcile_once()
check(result$failed==0L && identical(pages,c("first","sub_fixture")),"reconciliation paginates and checks linked subscriptions")
check(query("SELECT membership_active FROM users WHERE id=$1",uid)$membership_active[[1]],"missed recovery repaired from current Stripe state")
check(query("SELECT stripe_subscription_id FROM users WHERE id=$1",third)$stripe_subscription_id[[1]]=="sub_discovered","missing initial webhook recovered using metadata")
check(query("SELECT referral_wallet FROM users WHERE id=$1",ref)$referral_wallet[[1]]==18,"missed initial payment credits commission once")
stripe_reconcile_once()
check(query("SELECT referral_wallet FROM users WHERE id=$1",ref)$referral_wallet[[1]]==18,"repeated reconciliation cannot duplicate commissions")
check(nrow(query("SELECT * FROM stripe_events WHERE event_id LIKE 'reconcile%'"))==0L,"reconciliation does not create synthetic webhook events")
registry$sub_fixture$status <- "past_due"
stripe_reconcile_once()
check(!query("SELECT membership_active FROM users WHERE id=$1",uid)$membership_active[[1]],"missed failure suspends unpaid subscription")
DBI::dbExecute(con,"UPDATE users SET manual_access_expires_at=now()+interval '3 days' WHERE id=$1",params=list(uid))
stripe_reconcile_once()
check(query("SELECT membership_active FROM users WHERE id=$1",uid)$membership_active[[1]],"manual access survives reconciliation of unpaid subscription")
DBI::dbExecute(con,"UPDATE users SET manual_access_expires_at=NULL WHERE id=$1",params=list(uid))
registry$sub_fixture$status <- "active"
registry$sub_fixture$items$data[[1]]$price$id <- "price_other"
stripe_reconcile_once()
check(!query("SELECT membership_active FROM users WHERE id=$1",uid)$membership_active[[1]],"changed price cannot retain paid access")
registry$sub_fixture <- subscription
registry$sub_fixture$latest_invoice$status <- "open"
stripe_reconcile_once()
check(!query("SELECT membership_active FROM users WHERE id=$1",uid)$membership_active[[1]],"active subscription with unpaid invoice cannot grant access")
registry$sub_fixture <- subscription
registry$sub_fixture$metadata$user_id <- third
result <- stripe_reconcile_once()
check(result$failed==1L && !query("SELECT membership_active FROM users WHERE id=$1",uid)$membership_active[[1]],"conflicting user metadata fails without granting access")
check(!query("SELECT ok FROM operational_checks WHERE name='stripe_reconciliation'")$ok[[1]],"partial failure raises operational alert")
registry$sub_fixture <- subscription
registry$sub_fixture$livemode <- TRUE
result <- stripe_reconcile_once()
check(result$failed==1L,"wrong Stripe mode rejected")
registry$sub_fixture <- subscription
stripe_reconcile_once()
stripe_fetch_subscription <- function(id) stop("simulated network failure")
result <- stripe_reconcile_once()
check(result$failed==2L && query("SELECT membership_active FROM users WHERE id=$1",uid)$membership_active[[1]],"network failure preserves local access and reports errors")
stripe_fetch_subscription <- function(id) registry[[id]]
# Invalid pagination cannot falsely report a completed sweep.
stripe_list_subscriptions <- function(cursor=NULL) list(data=list(),has_more=TRUE)
check(inherits(try(stripe_reconcile_once(),silent=TRUE),"try-error"),"non-advancing pagination fails visibly")
stripe_list_subscriptions <- function(cursor=NULL) list(data=list(),has_more=FALSE)
stripe_reconcile_once()
check(query("SELECT ok FROM operational_checks WHERE name='stripe_reconciliation'")$ok[[1]],"recovered sweep clears operational alert")
# A second process cannot acquire the reconciliation session lock.
if(.Platform$OS.type=="unix") {
  DBI::dbGetQuery(con,"SELECT pg_advisory_lock(hashtext('puga-stripe-reconciliation-v1'))")
  blocked <- parallel::mclapply(1:2,function(i) {
    con <<- DBI::dbConnect(if(test_driver=="RPostgres") RPostgres::Postgres() else RPostgreSQL::PostgreSQL(),host="127.0.0.1",port=55432,dbname="postgres",user="agent")
    on.exit(DBI::dbDisconnect(con))
    stripe_reconcile_once()$skipped
  },mc.cores=2L)[[1]]
  DBI::dbGetQuery(con,"SELECT pg_advisory_unlock(hashtext('puga-stripe-reconciliation-v1'))")
  check(isTRUE(blocked),"concurrent reconciliation skips locked sweep")
}
cat("BILLING RECONCILIATION SUITE OK\n")
