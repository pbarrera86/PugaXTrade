# Server-owned idle clock. Only human input and accepted analysis jobs renew it.
# A bounded job deadline prevents a stalled task from keeping a session forever.
puga_session_activity <- function(clock=function() as.numeric(Sys.time()), lease_seconds=puga_job_lease_seconds) {
  last <- clock()
  jobs <- numeric()
  touch <- function() {last <<- clock(); invisible(NULL)}
  idle <- function() {
    now <- clock()
    expired <- jobs[jobs<=now]
    if(length(expired)) last <<- max(last,expired)
    jobs <<- jobs[jobs>now]
    if(length(jobs)) last <<- now
    max(0,now-last)
  }
  list(touch=touch,idle=idle,
    start=function(token) {
      if(!is.null(token)) {touch(); jobs[token] <<- clock()+lease_seconds}
      invisible(NULL)
    },
    finish=function(token) {
      if(!is.null(token) && token %in% names(jobs)) {
        jobs <<- jobs[names(jobs)!=token]
        touch()
      }
      invisible(NULL)
    })
}
