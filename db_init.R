con <- NULL

tryCatch({
  source("R/db.R")
  con <- pool_init()

  # Leer schema.sql preservando saltos de línea para no romper comentarios con --
  sql_text <- paste(readLines("schema.sql", warn = FALSE), collapse = "\n")
  sql_stmts <- strsplit(sql_text, ";", fixed = TRUE)[[1]]

  for (stmt in sql_stmts) {
    stmt <- trimws(stmt)
    if (nzchar(stmt)) {
      DBI::dbExecute(con, stmt)
    }
  }

  cat("Schema OK. Ejecutando migracion de datos...\n")

  null_refs <- DBI::dbGetQuery(
    con,
    "SELECT id
     FROM users
     WHERE referral_code IS NULL OR referral_code = ''
     ORDER BY created_at ASC"
  )

  if (nrow(null_refs) > 0) {
    cat("Migrando ", nrow(null_refs), " usuarios sin codigo de referido...\n", sep = "")

    for (uid in null_refs$id) {
      new_code <- db_generate_next_ref_code(con)

      chk <- DBI::dbGetQuery(
        con,
        "SELECT 1 FROM users WHERE referral_code = $1",
        params = list(new_code)
      )

      if (nrow(chk) > 0) {
        num <- suppressWarnings(as.integer(substring(new_code, 4)))
        if (is.na(num)) num <- 0
        new_code <- sprintf("ref%03d", num + 1)
      }

      DBI::dbExecute(
        con,
        "UPDATE users SET referral_code = $1 WHERE id = $2",
        params = list(new_code, uid)
      )
    }
  }

  cat("Migracion de datos completada.\n")

}, error = function(e) {
  cat("Error en db_init/migracion:", conditionMessage(e), "\n")
}, finally = {
  if (!is.null(con)) {
    tryCatch(
      pool::poolReturn(con),
      error = function(e) {
        cat("Aviso al devolver conexion al pool:", conditionMessage(e), "\n")
      }
    )
  }

  if (exists("pool_close", mode = "function")) {
    tryCatch(
      pool_close(),
      error = function(e) {
        cat("Aviso al cerrar pool:", conditionMessage(e), "\n")
      }
    )
  }
})
