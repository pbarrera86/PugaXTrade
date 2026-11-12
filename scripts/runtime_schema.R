# Read-only startup for a runtime role without DDL/ownership privileges.
source("R/db.R")
tryCatch({
  expected <- basename(list.files("migrations", pattern="[.]sql$"))
  applied <- .with_conn(DBI::dbGetQuery(con,"SELECT version FROM schema_migrations"))$version
  if (length(setdiff(expected, applied))) stop("Database migrations must be applied before runtime startup")
  .with_conn(DBI::dbGetQuery(con,"SELECT 1 FROM users, sessions, email_outbox, analysis_jobs, ticker_cache LIMIT 0"))
  cat("Runtime schema verified without DDL.\n")
}, finally=pool_close())
