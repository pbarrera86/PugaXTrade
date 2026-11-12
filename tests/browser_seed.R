# Fixture restricted to the disposable CI database; never run against deployed data.
stopifnot(identical(Sys.getenv("PUGA_TEST_DATABASE"), "local-disposable"),
          identical(Sys.getenv("PGHOST"), "127.0.0.1"),
          identical(Sys.getenv("PGPORT"), "55432"),
          identical(Sys.getenv("PGDATABASE"), "postgres"))
con <- DBI::dbConnect(RPostgres::Postgres())
tryCatch({
  DBI::dbExecute(con, "INSERT INTO users(id,username,email,password_hash,active,email_verified,trial_expires_at)
    VALUES($1,$2,$3,$4,true,true,now()+interval '7 days')",
    params=list("00000000-0000-0000-0000-000000000099", "ci_browser_fixture",
                "ci-browser@example.invalid", sodium::password_store("Browser fixture password 2026")))
}, finally=DBI::dbDisconnect(con))
