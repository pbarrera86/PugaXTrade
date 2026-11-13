source("R/session_activity.R")
now <- 0
activity <- puga_session_activity(function() now)
now <- 1800
check(activity$idle()==1800,"idle clock reaches exactly 30 minutes without early expiry")
now <- 1801
check(activity$idle()>1800,"session expires only after more than 30 idle minutes")
activity$touch();now <- 3000
activity$start("accepted_analysis")
now <- 4200
check(activity$idle()==0,"accepted analysis keeps session active without browser interaction")
activity$finish("accepted_analysis")
now <- 6000
check(activity$idle()==1800,"full idle allowance starts after analysis completion")
now <- 6001
check(activity$idle()>1800,"completed analysis cannot keep session alive forever")
activity$start("extremes");now <- 6100;activity$finish("extremes")
check(activity$idle()==0,"cancellation or error resets inactivity from task completion")
activity$start("stalled");now <- 9200
check(activity$idle()>1800,"expired analysis lease cannot prevent inactivity logout")
activity$finish(NULL)
check(activity$idle()>1800,"empty cleanup does not renew the idle deadline")
lookup <- db_find_user_by_token
tryCatch({
  db_find_user_by_token <- function(token) stop("simulated temporary database outage")
  check(db_session_status("fixture")$status=="unavailable","transport failure distinguished from revocation")
  db_find_user_by_token <- function(token) NULL
  check(db_session_status("fixture")$status=="invalid","revoked or expired session remains invalid")
  db_find_user_by_token <- function(token) data.frame(id="fixture")
  check(db_session_status("fixture")$status=="valid","valid session lookup retained")
},finally={db_find_user_by_token <- lookup})
cat("SESSION ACTIVITY SUITE OK\n")
