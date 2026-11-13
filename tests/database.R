# Isolated disposable DB only. Never use production credentials here.
stopifnot(identical(Sys.getenv("PUGA_TEST_DATABASE"),"local-disposable"))
test_driver <- Sys.getenv("PUGA_TEST_DRIVER","RPostgreSQL")
library(test_driver,character.only=TRUE)
library(sodium)
source("R/helpers.R")
source("R/security.R")
source("R/billing.R")
source("R/job_capacity.R")
con <- DBI::dbConnect(if(test_driver=="RPostgres") RPostgres::Postgres() else RPostgreSQL::PostgreSQL(),host="127.0.0.1",port=55432,dbname="postgres",user="agent")
for(e in parse("R/db.R")) {
  if(is.call(e) && identical(e[[1]],as.name("<-")) && is.symbol(e[[2]]) && is.call(e[[3]]) && identical(e[[3]][[1]],as.name("function"))) eval(e)
}
.with_conn <- function(expr) {
  env <- new.env(parent=parent.frame());env$con <- con
  eval(substitute(expr),env)
}
check <- function(value,name) {stopifnot(isTRUE(value));cat("PASS:",name,"\n")}
query <- function(sql,...) {
  params <- list(...)
  if (length(params)) DBI::dbGetQuery(con,sql,params=params) else DBI::dbGetQuery(con,sql)
}
uid <- "00000000-0000-0000-0000-000000000001"
ref <- "00000000-0000-0000-0000-000000000002"
DBI::dbExecute(con,"INSERT INTO users(id,username,email,active,email_verified,referred_by) VALUES($1,'fixture','fixture@example.invalid',true,true,$2)",params=list(uid,ref))
DBI::dbExecute(con,"INSERT INTO users(id,username,email,active,email_verified) VALUES($1,'referrer','referrer@example.invalid',true,true)",params=list(ref))
check(nrow(db_find_user_by_token(paste(rep("a",128),collapse="")))==1L,"migration preserves existing cookie sessions")
check(db_reset_token_valid(paste(rep("b",96),collapse="")),"migration preserves existing reset links")
.app_base_url <- function() "https://fixture.example.invalid"
DBI::dbExecute(con,"UPDATE users SET email_verified=false WHERE id=$1",params=list(uid))
db_resend_verification("fixture")
check(query("SELECT count(*) AS n FROM email_outbox")$n[[1]]==1L,"verification resend queued once")
body <- query("SELECT body FROM email_outbox LIMIT 1")$body[[1]]
vtoken <- sub(".*verify=","",body)
check(db_verify_email(vtoken),"resent verification token accepted")
t <- db_issue_session_token(uid)
check(nrow(db_find_user_by_token(t))==1L,"issued session accepted")
check(query("SELECT token FROM sessions WHERE user_id=$1",uid)$token[[1]]!=t,"session stored as hash")
DBI::dbExecute(con,"UPDATE users SET active=false WHERE id=$1",params=list(uid))
check(is.null(db_find_user_by_token(t)),"disabled account denied")
DBI::dbExecute(con,"UPDATE users SET active=true WHERE id=$1",params=list(uid))
handoff <- db_create_login_handoff(uid)
fresh <- db_redeem_login_handoff(handoff)
check(is.character(fresh) && nchar(fresh)==128L,"handoff issues session")
check(is.null(db_redeem_login_handoff(handoff)),"handoff single use")
check(is.null(db_find_user_by_token(t)),"new login revokes previous session")
reset <- db_create_reset_token(uid)
check(db_reset_token_valid(reset),"reset token valid")
check(db_reset_password_with_token(reset,"fixture secure password"),"password reset succeeds")
check(!db_reset_password_with_token(reset,"another fixture password"),"reset token cannot be reused")
check(is.null(db_find_user_by_token(fresh)),"password reset revokes sessions")
t <- db_issue_session_token(uid)
db_set_password("fixture","second secure password")
check(is.null(db_find_user_by_token(t)),"password change revokes sessions")

Sys.setenv(STRIPE_SECRET_KEY="sk_test_fixture",STRIPE_PRICE_ID="price_fixture",REFERRAL_COMMISSION_AMOUNT="9.00")
subscription <- list(id="sub_fixture",customer="cus_fixture",status="active",latest_invoice=list(status="paid"),metadata=list(user_id=uid),
  cancel_at_period_end=FALSE,items=list(data=list(list(price=list(id="price_fixture"),current_period_end=as.numeric(Sys.time())+86400))))
stripe_fetch_subscription <- function(id) subscription
event <- list(id="evt_fixture",type="checkout.session.completed",livemode=FALSE,
  data=list(object=list(id="cs_fixture",subscription="sub_fixture",payment_status="paid",amount_total=2999,client_reference_id=uid)))
check(process_stripe_event(event),"checkout processed")
check(!process_stripe_event(event),"duplicate event ignored")
check(query("SELECT membership_active FROM users WHERE id=$1",uid)$membership_active[[1]],"paid access active")
check(query("SELECT referral_wallet FROM users WHERE id=$1",ref)$referral_wallet[[1]]==9,"commission credited once")
event$id <- "evt_failure";event$type <- "invoice.payment_failed"
subscription$status <- "past_due"
process_stripe_event(event)
check(!query("SELECT membership_active FROM users WHERE id=$1",uid)$membership_active[[1]],"failed subscription suspended")
event$id <- "evt_recovery";event$type <- "invoice.paid";event$data$object$amount_paid <- 2999
subscription$status <- "active"
process_stripe_event(event)
check(query("SELECT membership_active FROM users WHERE id=$1",uid)$membership_active[[1]],"recovered subscription restored")
check(query("SELECT referral_wallet FROM users WHERE id=$1",ref)$referral_wallet[[1]]==9,"recovery cannot pay second commission")
event$id <- "evt_retry"
stripe_fetch_subscription <- function(id) stop("simulated network failure")
try(process_stripe_event(event),silent=TRUE)
check(nrow(query("SELECT event_id FROM stripe_events WHERE event_id=$1",event$id))==0L,"failed event rolled back for retry")
# Old subscription event must not disable the current subscription.
stripe_fetch_subscription <- function(id) {
  x <- subscription
  if(id=="sub_old") {x$id <- id;x$status <- "canceled"}
  x
}
event$id <- "evt_old"; event$type <- "customer.subscription.deleted";event$data$object$id <- "sub_old"
process_stripe_event(event)
check(query("SELECT membership_active FROM users WHERE id=$1",uid)$membership_active[[1]],"old subscription cannot revoke current access")
source("tests/session_activity.R")
source("tests/billing_reconciliation.R")
if(.Platform$OS.type=="unix") {
  handoff <- db_create_login_handoff(uid)
  connect <- function() DBI::dbConnect(if(test_driver=="RPostgres") RPostgres::Postgres() else RPostgreSQL::PostgreSQL(),host="127.0.0.1",port=55432,dbname="postgres",user="agent")
  results <- parallel::mclapply(1:2,function(i) {
    con <<- connect()
    on.exit(DBI::dbDisconnect(con))
    !is.null(db_redeem_login_handoff(handoff))
  },mc.cores=2)
  check(sum(unlist(results))==1L,"concurrent handoff redeemed exactly once")
  reset <- db_create_reset_token(uid)
  results <- parallel::mclapply(1:2,function(i) {
    con <<- connect()
    on.exit(DBI::dbDisconnect(con))
    db_reset_password_with_token(reset,paste0("fixture password ",i))
  },mc.cores=2)
  check(sum(unlist(results))==1L,"concurrent reset succeeds exactly once")
}
# Runtime can mutate application data but cannot change schema or migration history.
DBI::dbExecute(con,"CREATE ROLE puga_runtime_fixture NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT")
DBI::dbExecute(con,"GRANT USAGE ON SCHEMA public TO puga_runtime_fixture")
DBI::dbExecute(con,"GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO puga_runtime_fixture")
DBI::dbExecute(con,"GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO puga_runtime_fixture")
DBI::dbExecute(con,"SET ROLE puga_runtime_fixture")
check(query("SELECT count(*) AS n FROM users")$n[[1]]>0,"runtime can read users")
check(inherits(try(DBI::dbExecute(con,"ALTER TABLE users ADD COLUMN forbidden_test boolean"),silent=TRUE),"try-error"),"runtime cannot alter users")
# An eight-client burst must admit exactly two distinct users, one job per user.
DBI::dbExecute(con,"RESET ROLE")
for(i in 103:110) DBI::dbExecute(con,"INSERT INTO users(id,username,active,email_verified) VALUES($1,$2,true,true)",params=list(sprintf("00000000-0000-0000-0000-%012d",i),paste0("load_fixture_",i)))
if(.Platform$OS.type=="unix") {
  results <- parallel::mclapply(103:110,function(i) {
    con <<- connect();on.exit(DBI::dbDisconnect(con))
    DBI::dbExecute(con,"SET ROLE puga_runtime_fixture")
    puga_job_acquire(sprintf("00000000-0000-0000-0000-%012d",i))
  },mc.cores=8)
  check(sum(!vapply(results,is.null,logical(1)))==2L,"eight concurrent requests admit exactly two analysis jobs")
  check(query("SELECT count(*) AS n FROM analysis_jobs")$n[[1]]==2L,"capacity shared across connections")
  check(all(abs(query("SELECT extract(epoch FROM (expires_at-started_at)) AS lease_seconds FROM analysis_jobs")$lease_seconds-3660)<1),"database job lease lasts 61 minutes")
  occupied <- query("SELECT user_id,token FROM analysis_jobs LIMIT 1")
  check(is.null(puga_job_acquire(occupied$user_id[[1]])),"same user cannot start a second job")
  puga_job_release(occupied$token[[1]])
  t <- puga_job_acquire(occupied$user_id[[1]])
  check(is.character(t),"completion releases capacity")
  DBI::dbExecute(con,"UPDATE analysis_jobs SET expires_at=now()-interval '1 second'")
  check(is.character(puga_job_acquire(uid)),"expired worker lease recovers capacity")
}
DBI::dbDisconnect(con)
cat("DATABASE SUITE OK\n")
