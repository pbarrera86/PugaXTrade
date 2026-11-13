puga_max_tickers <- 7300L
puga_ticker_count_allowed <- function(tickers) length(tickers) <= puga_max_tickers

# Shared across Shiny processes and both analysis modules; compatible with
# transaction-pooling PostgreSQL endpoints. Worker timeout precedes lease expiry.
puga_job_timeout_ms <- 1200000L
puga_job_acquire <- function(user_id) {
  .with_conn(DBI::dbWithTransaction(con, {
    DBI::dbGetQuery(con, "SELECT pg_advisory_xact_lock(7412831)")
    DBI::dbExecute(con, "DELETE FROM analysis_jobs WHERE expires_at<=now()")
    own <- DBI::dbGetQuery(con, "SELECT 1 FROM analysis_jobs WHERE user_id=$1", params=list(user_id))
    free <- DBI::dbGetQuery(con, "SELECT s FROM generate_series(1,2) s WHERE NOT EXISTS (SELECT 1 FROM analysis_jobs j WHERE j.slot=s) ORDER BY s LIMIT 1")
    if (nrow(own) || !nrow(free)) NULL else {
      token <- paste0(sample(c(letters, LETTERS, 0:9), 48L, replace=TRUE), collapse="")
      DBI::dbExecute(con, "INSERT INTO analysis_jobs(token,user_id,slot,expires_at) VALUES($1,$2,$3,now()+interval '21 minutes')",
        params=list(token,user_id,free$s[[1]]))
      token
    }
  }))
}
puga_job_release <- function(token) {
  if (!is.null(token)) .with_conn(DBI::dbExecute(con, "DELETE FROM analysis_jobs WHERE token=$1", params=list(token)))
  invisible(NULL)
}
