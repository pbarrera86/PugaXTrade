# R/db.R — Pool Postgres (Neon) + helpers
# - pool_global acorde a app.R (onStop() lo cierra)
# - poolCheckout()/poolReturn() en TODOS los helpers
# - pool_init() devuelve una CONEXIÓN checkout (compatible con Admin en app.R)
# - sodium guarda/verifica STRINGS (password_store/verify)

suppressWarnings({
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(sodium)
  library(lubridate)
})

# =========================
# Pool global
# =========================
pool_global <- NULL

# Operador seguro: si a es NULL, length 0, NA, o string vacío -> usa b
# Se define aquí para que db.R funcione standalone (db_init.R lo usa sin helpers.R).
# En el contexto de app.R, helpers.R ya lo definió antes — el guard evita sobreescribir.
if (!exists("%||%", envir = .GlobalEnv, inherits = FALSE)) {
  `%||%` <- function(a, b) {
    if (is.null(a) || length(a) == 0) return(b)
    if (all(is.na(a))) return(b)
    if (is.character(a)) {
      a1 <- a[[1]]
      if (is.na(a1) || !nzchar(a1)) return(b)
    }
    a
  }
}

sql_safe <- function(x) if (is.null(x)) NA_character_ else as.character(x)

# Construye el pool
.make_pool <- function() {
  if (!requireNamespace("pool", quietly = TRUE)) {
    stop("Falta el paquete 'pool'. Instálalo con install.packages('pool').")
  }

  # Permite DATABASE_URL (Neon URI) o variables PG*
  conn_url <- Sys.getenv("DATABASE_URL", unset = Sys.getenv("NEON_URL", ""))

  parse_url <- function(u) {
    out <- list()
    if (!nzchar(u)) {
      return(out)
    }
    m <- regexec("^postgres(?:ql)?://([^:]+):([^@]+)@([^:/]+)(?::(\\d+))?/([^?]+)(?:\\?(.*))?$", u, perl = TRUE)
    g <- regmatches(u, m)
    if (length(g) == 0 || length(g[[1]]) < 6) {
      return(out)
    }
    gg <- g[[1]]
    out$user <- gg[2]
    out$password <- gg[3]
    out$host <- gg[4]
    out$port <- gg[5]
    out$dbname <- gg[6]
    if (length(gg) >= 7 && nzchar(gg[7])) {
      for (kv in strsplit(gg[7], "&")[[1]]) {
        kvp <- strsplit(kv, "=", fixed = TRUE)[[1]]
        if (length(kvp) == 2) out[[kvp[1]]] <- kvp[2]
      }
    }
    out
  }

  cfg <- if (nzchar(conn_url)) parse_url(conn_url) else list()

  host <- cfg$host %||% Sys.getenv("PGHOST")
  hostaddr <- Sys.getenv("PGHOSTADDR", "") # IPv4 directa opcional
  port <- as.integer((cfg$port %||% Sys.getenv("PGPORT", "5432")))
  dbname <- cfg$dbname %||% Sys.getenv("PGDATABASE")
  user <- cfg$user %||% Sys.getenv("PGUSER")
  pass <- cfg$password %||% Sys.getenv("PGPASSWORD")
  sslmode <- cfg$sslmode %||% Sys.getenv("PGSSLMODE", "require")

  if (!nzchar(host) && !nzchar(hostaddr)) stop("Config DB incompleta: define PGHOST (o PGHOSTADDR).")
  if (nzchar(host) && grepl("xxxx", host, fixed = TRUE)) stop("PGHOST tiene placeholder (ep-xxxx...). Cámbialo por tu host real.")
  if (!nzchar(dbname) || !nzchar(user) || !nzchar(pass)) stop("Faltan PGDATABASE/PGUSER/PGPASSWORD.")

  args <- list(
    drv         = RPostgres::Postgres(),
    dbname      = dbname,
    user        = user,
    password    = pass,
    port        = port,
    sslmode     = sslmode,
    idleTimeout = 300,
    minSize     = 2,
    maxSize     = 10 # Adjust based on server capacity
  )
  if (nzchar(hostaddr)) {
    args$hostaddr <- hostaddr
    if (nzchar(host)) args$host <- host
  } else {
    args$host <- host
  }

  p <- do.call(pool::dbPool, args)

  # Alias seguro para que app.R pueda llamar poolReturn(con) sin library(pool)
  if (!exists("poolReturn", mode = "function", inherits = TRUE)) {
    poolReturn <<- function(con) pool::poolReturn(con)
  }
  p
}

# Verifica si el pool es funcional haciendo un checkout/return de prueba
pool_is_ok <- function(p) {
  if (is.null(p) || !DBI::dbIsValid(p)) {
    return(FALSE)
  }
  ok <- TRUE
  tryCatch(
    {
      con <- pool::poolCheckout(p)
      pool::poolReturn(con)
    },
    error = function(e) {
      ok <<- FALSE
    }
  )
  ok
}

# Devuelve pool_global o lo recrea si falla el ping
get_pool <- function() {
  if (!pool_is_ok(pool_global)) {
    # Intentar cerrar si estaba roto para no dejar recursos colgados
    if (!is.null(pool_global)) try(pool::poolClose(pool_global), silent = TRUE)
    pool_global <<- .make_pool()
  }
  pool_global
}

# Legacy wrapper (opcional por si app.R lo llama directo al inicio fuera de helpers)
.pool_start <- function() {
  get_pool()
}

# Cierra el pool_global (app.R ya lo hace en onStop(); lo dejamos por si lo ocupas manualmente)
pool_close <- function() {
  if (!is.null(pool_global) && DBI::dbIsValid(pool_global)) {
    pool::poolClose(pool_global)
    pool_global <<- NULL
  }
}

# =========================
# ¡IMPORTANTE!
# pool_init() devuelve una CONEXIÓN "checkout"
# (Así lo espera tu app.R en el panel Admin)
# =========================
pool_init <- function() {
  p <- get_pool()
  con <- pool::poolCheckout(p)
  return(con)
}

# Helper interno para envolver con checkout/return automático
.with_conn <- function(expr) {
  p <- get_pool()
  con <- pool::poolCheckout(p)
  on.exit(try(pool::poolReturn(con), silent = TRUE), add = TRUE)

  # Crear un entorno donde 'con' exista y el resto venga del parent.frame()
  env <- new.env(parent = parent.frame())
  env$con <- con

  eval(substitute(expr), envir = env)
}

# =========================
# UTILIDADES
# =========================
# =========================
# HELPER: Generar siguiente código refXXX
# =========================
db_generate_next_ref_code <- function(con) {
  # Buscamos todos los códigos que cumplan patrón 'ref' + dígitos
  # Nota: esto puede ser lento si hay millones de usuarios, pero para esta escala está bien.
  # Una secuencia SQL pura sería mejor, pero esto mantiene la lógica en R.

  # 1. Obtener todos los ref...
  df <- DBI::dbGetQuery(con, "select referral_code from users where referral_code like 'ref%'")

  if (nrow(df) == 0) {
    return("ref001")
  }

  codes <- df$referral_code[!is.na(df$referral_code)]
  # Extraer parte numérica
  nums <- suppressWarnings(as.integer(substring(codes, 4)))
  nums <- nums[!is.na(nums)]

  # Filter out timestamp-based codes (very large numbers)
  nums <- nums[nums < 1000000]

  if (length(nums) == 0) {
    return("ref001")
  }

  next_val <- max(nums) + 1
  sprintf("ref%03d", next_val)
}

rnd_token <- function(n = 32L) {
  # sodium::random() usa /dev/urandom — criptográficamente seguro
  raw_bytes <- sodium::random(n)
  paste0(sprintf("%02x", as.integer(raw_bytes)), collapse = "")
}

# =========================
# USERS
# =========================
db_get_user_by_email <- function(email) {
  .with_conn({
    DBI::dbGetQuery(con, "select * from users where lower(email)=lower($1) limit 1", params = list(email))
  })
}

db_get_user_by_username <- function(username) {
  .with_conn({
    DBI::dbGetQuery(con, "select * from users where lower(username)=lower($1) limit 1", params = list(username))
  })
}

# Usado por app.R tras el pago
db_get_user_by_id <- function(user_id) {
  .with_conn({
    DBI::dbGetQuery(con, "select * from users where id=$1 limit 1", params = list(user_id))
  })
}

# Inserta usuario con password_hash "placeholder" (string de sodium) o real si se provee
db_register_user <- function(username, email, name, country, phone, referred_by = NULL, password = NULL) {
  .with_conn({
    # Autogenerar username si viene vacío
    if (is.null(username) || !nzchar(username)) {
      uname <- sub("@.*$", "", email)
      i <- 0
      candidate <- uname
      repeat {
        q <- DBI::dbGetQuery(con, "select 1 from users where lower(username)=lower($1) limit 1", params = list(candidate))
        if (nrow(q) == 0) {
          username <- candidate
          break
        }
        i <- i + 1
        candidate <- paste0(uname, i)
      }
    }

    # Duplicados
    dup <- DBI::dbGetQuery(con,
      "select 1 from users where lower(username)=lower($1) or lower(email)=lower($2) limit 1",
      params = list(username, email)
    )
    if (nrow(dup) > 0) {
      return(list(ok = FALSE, message = "Usuario o correo ya existe."))
    }

    # INICIO TRANSACCION
    tryCatch(
      {
        DBI::dbBegin(con)
      },
      error = function(e) warning("Could not start transaction: ", e$message)
    )

    on.exit(try(DBI::dbRollback(con), silent = TRUE), add = TRUE) # Rollback si falla antes de commit

    # Generate Referral Code (Sequential refXXX)
    if (is.null(referred_by) || !nzchar(as.character(referred_by)) || is.na(referred_by)) {
      default_ref_username <- Sys.getenv("DEFAULT_REFERRER_USERNAME", "")
      if (nzchar(default_ref_username)) {
        admin_u <- DBI::dbGetQuery(con, "select id from users where lower(username)=lower($1) limit 1",
          params = list(default_ref_username))
        if (nrow(admin_u) > 0) {
          referred_by <- admin_u$id[[1]]
        }
      }
    }

    ref_code <- NULL
    for (k in 1:3) {
      candidate <- db_generate_next_ref_code(con)
      q <- DBI::dbGetQuery(con, "select 1 from users where referral_code=$1 limit 1", params = list(candidate))
      if (nrow(q) == 0) {
        ref_code <- candidate
        break
      }
      Sys.sleep(0.1)
    }

    if (is.null(ref_code)) {
      ref_code <- paste0("ref", as.integer(Sys.time()))
    }

    # Validate referred_by (must exist)
    valid_referrer <- NA_character_
    if (!is.null(referred_by)) {
      r <- DBI::dbGetQuery(con, "select id from users where id::text=$1 limit 1", params = list(as.character(referred_by)))
      if (nrow(r) > 0) valid_referrer <- as.character(r$id[[1]])
    }

    trial <- Sys.Date() + 7

    # Verification token
    ver_token <- rnd_token(32)

    # Hash: usar el real si viene, si no placeholder
    final_hash <- if (!is.null(password) && nzchar(password)) {
      sodium::password_store(password)
    } else {
      sodium::password_store(rnd_token(16))
    }

    u <- DBI::dbGetQuery(con,
      "insert into users(
         username, email, name, country, phone, password_hash,
         created_at, trial_expires_at, active, membership_active,
         referral_code, referred_by, referral_wallet,
         email_verified, verification_token
       )
       values($1,$2,$3,$4,$5,$6,now(),$7,true,false,$8,$9,0.0, false, $10)
       returning *",
      params = list(
        username, email, name, country,
        ifelse(is.na(phone), "", phone),
        final_hash,
        trial,
        ref_code,
        valid_referrer,
        ver_token
      )
    )

    DBI::dbCommit(con)
    list(ok = TRUE, user = u)
  })
}

# Guarda HASH como STRING
db_set_password <- function(username, new_password) {
  .with_conn({
    ph <- sodium::password_store(new_password) # string listo para DB
    DBI::dbExecute(con,
      "update users set password_hash=$1 where lower(username)=lower($2)",
      params = list(ph, username)
    )
    invisible(TRUE)
  })
}

# Login (verifica con sodium::password_verify(hash_string, pass_plano))
db_login <- function(user_or_email, password) {
  .with_conn({
    u <- DBI::dbGetQuery(con, "
      select * from users
      where (lower(username)=lower($1) or lower(email)=lower($1)) and active = true
      limit 1", params = list(user_or_email))
    if (nrow(u) == 0) {
      return(list(ok = FALSE, message = "Usuario no encontrado o inactivo."))
    }

    hash <- u$password_hash[[1]]
    if (is.na(hash) || !is.character(hash) || !nzchar(hash)) {
      return(list(ok = FALSE, message = "Contraseña incorrecta."))
    }
    pw_ok <- tryCatch(sodium::password_verify(hash, password), error = function(e) {
      message("[DB] password_verify error (hash corrupto?): ", e$message)
      FALSE
    })
    if (!isTRUE(pw_ok)) {
      return(list(ok = FALSE, message = "Contraseña incorrecta."))
    }

    if (!isTRUE(u$email_verified[[1]])) {
      return(list(ok = FALSE, message = "Correo no verificado. Revisa tu bandeja de entrada."))
    }

    DBI::dbExecute(con, "update users set last_login_at=now() where id=$1", params = list(u$id[[1]]))
    list(ok = TRUE, user = u)
  })
}

db_reload_user <- function(user_id) {
  .with_conn({
    DBI::dbGetQuery(con, "select * from users where id=$1 limit 1", params = list(user_id))
  })
}

db_ensure_referral_code <- function(user_id) {
  .with_conn({
    u <- DBI::dbGetQuery(con, "select referral_code from users where id=$1", params = list(user_id))
    if (nrow(u) > 0 && (is.null(u$referral_code[[1]]) || is.na(u$referral_code[[1]]) || !nzchar(u$referral_code[[1]]))) {
      # Generate unique code (Sequential)
      ref_code <- db_generate_next_ref_code(con)
      # Reintentar hasta 5 veces si hay colisión
      for (.retry in 1:5) {
        q <- DBI::dbGetQuery(con, "select 1 from users where referral_code=$1 limit 1", params = list(ref_code))
        if (nrow(q) == 0) break
        num <- suppressWarnings(as.integer(substring(ref_code, 4)))
        if (is.na(num)) num <- 0
        ref_code <- sprintf("ref%03d", num + .retry)
      }
      DBI::dbExecute(con, "update users set referral_code=$1 where id=$2", params = list(ref_code, user_id))
      return(ref_code)
    }
    if (nrow(u) > 0) {
      return(u$referral_code[[1]])
    }
    NULL
  })
}


db_get_user_by_referral_code <- function(code) {
  if (is.null(code) || !nzchar(code)) {
    return(NULL)
  }
  .with_conn({
    DBI::dbGetQuery(con, "select * from users where referral_code=$1 limit 1", params = list(code))
  })
}

db_add_commission <- function(referrer_id, amount) {
  .with_conn({
    DBI::dbExecute(con,
      "update users set referral_wallet = referral_wallet + $1 where id = $2",
      params = list(amount, referrer_id)
    )
    invisible(TRUE)
  })
}

db_get_referred_users <- function(referrer_id) {
  .with_conn({
    DBI::dbGetQuery(con,
      "select username, created_at, membership_active
       from users where referred_by = $1 order by created_at desc",
      params = list(referrer_id)
    )
  })
}

is_super_admin <- function(user_row) {
  if (is.null(user_row) || nrow(user_row) == 0) return(FALSE)
  super_email <- Sys.getenv("SUPER_ADMIN_EMAIL", "")
  if (!nzchar(super_email)) return(FALSE)
  identical(tolower(user_row$email[[1]]), tolower(super_email))
}

db_is_admin <- function(user_row) {
  isTRUE(user_row$is_admin[[1]])
}
db_membership_is_active <- function(user_row) {
  active <- isTRUE(user_row$membership_active[[1]])
  if (!active) {
    return(FALSE)
  }

  # Check expiration if present
  # If column doesn't exist in row (old session object?), assume active if boolean is true (fallback)
  if (!"membership_expires_at" %in% names(user_row)) {
    return(TRUE)
  }

  exp_val <- user_row$membership_expires_at[[1]]
  if (is.null(exp_val) || is.na(exp_val)) {
    return(TRUE)
  } # No expiration = Unlimited/Stripe Auto

  # Check if expired
  # Postgre timestamptz comes as POSIXct in R usually
  return(exp_val > Sys.time())
}

db_trial_days_left <- function(user_row) {
  if (db_is_admin(user_row)) {
    return(NA_integer_)
  }
  if (db_membership_is_active(user_row)) {
    return(NA_integer_)
  }
  if (is.null(user_row$trial_expires_at) || is.na(user_row$trial_expires_at[[1]])) {
    return(NA_integer_)
  }
  as.integer(as.Date(user_row$trial_expires_at[[1]]) - Sys.Date())
}

db_membership_activate <- function(user_id, stripe_customer_id = NULL, stripe_subscription_id = NULL) {
  .with_conn({
    # Check if already active to avoid double commission
    curr <- DBI::dbGetQuery(con, "select membership_active, referred_by from users where id=$1", params = list(user_id))
    already_active <- FALSE
    referrer_id <- NULL
    if (nrow(curr) > 0) {
      if (isTRUE(curr$membership_active[[1]])) already_active <- TRUE
      referrer_id <- curr$referred_by[[1]]
    }

    DBI::dbExecute(con, "
      update users set
        membership_active = true,
        membership_activated_at = now(),
        trial_expires_at = null,
        stripe_customer_id = coalesce($1, stripe_customer_id),
        stripe_subscription_id = coalesce($2, stripe_subscription_id)
      where id = $3",
      params = list(stripe_customer_id, stripe_subscription_id, user_id)
    )

    # Commission Logic (30% of 29.99 = ~9.00 USD)
    # Only if NOT already active and HAS a referrer
    if (!already_active && !is.null(referrer_id) && !is.na(referrer_id)) {
      # Check if referrer is Admin (pedrobp86) -> No commission for admin self-assignment logic?
      # User said: "no se asigna comisión" if default admin.
      # So we check if referrer is 'pedrobp86'
      ref_u <- DBI::dbGetQuery(con, "select username, email from users where id=$1", params = list(referrer_id))
      if (nrow(ref_u) > 0) {
        default_ref_username <- Sys.getenv("DEFAULT_REFERRER_USERNAME", "")
        r_name <- ref_u$username[[1]]
        r_email <- ref_u$email[[1]]
        super_email <- Sys.getenv("SUPER_ADMIN_EMAIL", "")
        # No pagar comisión al referidor por defecto (admin) para evitar auto-beneficio
        is_default_ref <- (nzchar(default_ref_username) && tolower(r_name) == tolower(default_ref_username)) ||
                          (nzchar(super_email) && tolower(r_email) == tolower(super_email))
        if (!is_default_ref) {
          commission_amount <- as.numeric(Sys.getenv("REFERRAL_COMMISSION_AMOUNT", "9.00"))
          message(sprintf("[Commission] Paying %.2f to %s for user %s", commission_amount, r_name, user_id))
          DBI::dbExecute(con, "
            UPDATE users
            SET referral_wallet = COALESCE(referral_wallet, 0) + $1
            WHERE id = $2",
            params = list(commission_amount, referrer_id)
          )
        }
      }
    }

    invisible(TRUE)
  })
}

# =========================
# PASSWORD RESET
# =========================
db_create_reset_token <- function(user_id, ttl_hours = 48) {
  .with_conn({
    token <- rnd_token(48)
    DBI::dbExecute(con,
      "insert into password_resets(token,user_id,created_at,expires_at,used_at)
       values($1,$2,now(), now() + make_interval(hours => $3), null)",
      params = list(token, user_id, as.integer(ttl_hours))
    )
    token
  })
}

db_reset_token_valid <- function(token) {
  .with_conn({
    t <- DBI::dbGetQuery(con, "
      select * from password_resets
       where token=$1 and used_at is null and expires_at > now()
       limit 1", params = list(token))
    nrow(t) > 0
  })
}

db_reset_password_with_token <- function(token, new_password) {
  .with_conn({
    t <- DBI::dbGetQuery(con, "select * from password_resets where token=$1 limit 1", params = list(token))
    if (nrow(t) == 0) {
      return(FALSE)
    }
    if (!is.na(t$used_at[[1]]) || as.POSIXct(t$expires_at[[1]]) < Sys.time()) {
      return(FALSE)
    }
    ph <- sodium::password_store(new_password)
    DBI::dbExecute(con, "update users set password_hash=$1 where id=$2", params = list(ph, t$user_id[[1]]))
    DBI::dbExecute(con, "update password_resets set used_at=now() where token=$1", params = list(token))
    TRUE
  })
}

db_verify_email <- function(token) {
  .with_conn({
    u <- DBI::dbGetQuery(con, "select id from users where verification_token=$1 limit 1", params = list(token))
    if (nrow(u) == 0) {
      return(FALSE)
    }
    DBI::dbExecute(con, "update users set email_verified=true, verification_token=NULL where id=$1", params = list(u$id[[1]]))
    TRUE
  })
}

# =========================
# SESIONES (cookies)
# =========================
db_issue_session_token <- function(user_id, days = 7) {
  .with_conn({
    tok <- rnd_token(64)
    DBI::dbExecute(con, "delete from sessions where user_id=$1", params = list(user_id))
    DBI::dbExecute(con,
      "insert into sessions(token,user_id,expires_at) values($1,$2, now() + make_interval(days => $3))",
      params = list(tok, user_id, as.integer(days))
    )
    tok
  })
}

db_find_user_by_token <- function(tok) {
  if (is.null(tok) || !nzchar(tok)) {
    return(NULL)
  }
  .with_conn({
    q <- DBI::dbGetQuery(con, "
      select u.* from sessions s join users u on u.id = s.user_id
       where s.token=$1 and s.expires_at > now() limit 1", params = list(tok))
    if (nrow(q) == 0) {
      return(NULL)
    }
    q
  })
}

db_delete_token <- function(tok) {
  if (is.null(tok) || !nzchar(tok)) {
    return(invisible())
  }
  .with_conn({
    DBI::dbExecute(con, "delete from sessions where token=$1", params = list(tok))
    invisible(TRUE)
  })
}

# Limpia sesiones expiradas — llamar periódicamente (ej: en onSessionEnded o cron)
# =========================
# RATE LIMITING DE LOGIN
# =========================
# Registra un intento (éxito o fallo)
db_record_login_attempt <- function(identifier, success = FALSE) {
  tryCatch(.with_conn({
    DBI::dbExecute(con,
      "INSERT INTO login_attempts(identifier, attempted_at, success) VALUES($1, now(), $2)",
      params = list(tolower(trimws(identifier)), success)
    )
  }), error = function(e) invisible(NULL))
}

# Devuelve TRUE si el identificador está bloqueado (5+ fallos en 15 minutos)
db_is_rate_limited <- function(identifier, max_failures = 5L, window_minutes = 15L) {
  tryCatch(.with_conn({
    res <- DBI::dbGetQuery(con,
      "SELECT COUNT(*) as n FROM login_attempts
       WHERE identifier = $1
         AND success = false
         AND attempted_at > now() - make_interval(mins => $2)",
      params = list(tolower(trimws(identifier)), as.integer(window_minutes))
    )
    isTRUE(res$n[[1]] >= max_failures)
  }), error = function(e) FALSE)
}

# Limpia intentos antiguos (llamar periódicamente)
db_cleanup_login_attempts <- function(days = 1L) {
  tryCatch(.with_conn({
    DBI::dbExecute(con,
      "DELETE FROM login_attempts WHERE attempted_at < now() - make_interval(days => $1)",
      params = list(as.integer(days))
    )
  }), error = function(e) invisible(NULL))
}

db_find_user_by_stripe_customer <- function(customer_id) {
  if (is.null(customer_id) || !nzchar(customer_id)) return(NULL)
  .with_conn({
    DBI::dbGetQuery(con,
      "SELECT * FROM users WHERE stripe_customer_id = $1 LIMIT 1",
      params = list(customer_id)
    )
  })
}

db_membership_deactivate <- function(user_id) {
  .with_conn({
    DBI::dbExecute(con,
      "UPDATE users SET
         membership_active = false,
         membership_expires_at = now()
       WHERE id = $1",
      params = list(user_id)
    )
    invisible(TRUE)
  })
}

db_cleanup_expired_sessions <- function() {
  .with_conn({
    n <- DBI::dbExecute(con, "DELETE FROM sessions WHERE expires_at < now()")
    if (n > 0) message(sprintf("[DB] Limpiadas %d sesiones expiradas.", n))
    invisible(n)
  })
}

# Limpia tokens de reset de contraseña expirados o usados
db_cleanup_expired_reset_tokens <- function() {
  .with_conn({
    n <- DBI::dbExecute(con, "DELETE FROM password_resets WHERE expires_at < now() OR used_at IS NOT NULL")
    if (n > 0) message(sprintf("[DB] Limpiados %d tokens de reset expirados.", n))
    invisible(n)
  })
}

db_update_user_stripe_ids <- function(user_id, customer_id, subscription_id) {
  .with_conn({
    DBI::dbExecute(con, "update users set stripe_customer_id=$1, stripe_subscription_id=$2 where id=$3",
      params = list(customer_id, subscription_id, user_id)
    )
    invisible(TRUE)
  })
}

db_manual_activate <- function(user_id, days_duration) {
  .with_conn({
    # Update both trial_expires_at (for legacy support) AND membership_expires_at
    # We also set membership_active = TRUE now, because we have a valid expiration date.
    DBI::dbExecute(con,
      "update users set
         active = true,
         membership_active = true,
         membership_activated_at = now(),
         trial_expires_at = now() + make_interval(days => $1),
         membership_expires_at = now() + make_interval(days => $1)
       where id = $2",
      params = list(as.integer(days_duration), user_id)
    )
    invisible(TRUE)
  })
}

db_manual_deactivate <- function(user_id) {
  .with_conn({
    DBI::dbExecute(con,
      "update users set
         membership_active = false,
         trial_expires_at = now() - interval '1 day'
       where id = $1",
      params = list(user_id)
    )
    invisible(TRUE)
  })
}

db_delete_user <- function(user_id) {
  .with_conn({
    u_id_str <- as.character(user_id)
    # 1. Limpiar referencias de invitados (cast explícito a uuid si aplica, o texto según columna)
    # Usamos cast ::uuid para asegurar compatibilidad total en Linux
    DBI::dbExecute(con, "update users set referred_by = NULL where referred_by = $1", params = list(u_id_str))

    # 2. Borrado explícito de tablas vinculadas
    DBI::dbExecute(con, "delete from sessions where user_id = $1::uuid", params = list(u_id_str))
    DBI::dbExecute(con, "delete from password_resets where user_id = $1::uuid", params = list(u_id_str))

    # 3. Limpiar intentos de login (busca por email y username del usuario)
    u_data <- DBI::dbGetQuery(con, "select username, email from users where id = $1::uuid", params = list(u_id_str))
    if (nrow(u_data) > 0) {
      DBI::dbExecute(con, "delete from login_attempts where lower(identifier) = lower($1) or lower(identifier) = lower($2)",
        params = list(u_data$username[[1]], u_data$email[[1]]))
    }

    # 3. Borrado final del usuario
    DBI::dbExecute(con, "delete from users where id = $1::uuid", params = list(u_id_str))
    return(TRUE)
  })
}

# =========================
# CACHING (Analysis)
# =========================
db_get_ticker_cache <- function(ticker) {
  .with_conn({
    # Fetch if newer than 24 hours
    row <- DBI::dbGetQuery(con,
      "select data_json from cached_analysis where ticker=$1 and last_updated > now() - interval '24 hours'",
      params = list(ticker)
    )
    if (nrow(row) > 0) {
      return(row$data_json[[1]])
    }
    NULL
  })
}

db_get_ticker_cache_batch <- function(tickers) {
  if (length(tickers) == 0) {
    return(list())
  }
  .with_conn({
    # Note: Using IN with many parameters is safer than raw string building
    # For very large lists (>1000), we might need chunks, but for 500 it's fine.
    placeholders <- paste0("$", seq_along(tickers), collapse = ",")
    query <- sprintf("
      SELECT ticker, data_json
      FROM cached_analysis
      WHERE ticker IN (%s) AND last_updated > now() - interval '24 hours'
    ", placeholders)

    res <- DBI::dbGetQuery(con, query, params = as.list(tickers))

    # Convert to list for easy lookup
    if (nrow(res) == 0) {
      return(list())
    }
    out <- setNames(as.list(res$data_json), res$ticker)
    out
  })
}

db_save_ticker_cache <- function(ticker, data_str) {
  .with_conn({
    # Upsert (Insert or Update)
    DBI::dbExecute(con,
      "INSERT INTO cached_analysis (ticker, data_json, last_updated)
       VALUES ($1, $2, now())
       ON CONFLICT (ticker)
       DO UPDATE SET data_json = EXCLUDED.data_json, last_updated = EXCLUDED.last_updated",
      params = list(ticker, data_str)
    )
    invisible(TRUE)
  })
}

db_clear_ticker_cache <- function() {
  .with_conn({
    # Limpia ambas tablas de caché
    DBI::dbExecute(con, "TRUNCATE TABLE cached_analysis")
    DBI::dbExecute(con, "DROP TABLE IF EXISTS ticker_cache") # Elimina institucional para forzar normalización
    invisible(TRUE)
  })
}

db_save_ticker_cache_batch <- function(items) {
  if (length(items) == 0) {
    return(invisible(TRUE))
  }
  .with_conn({
    DBI::dbBegin(con)
    tryCatch(
      {
        for (tk in names(items)) {
          DBI::dbExecute(con,
            "INSERT INTO cached_analysis (ticker, data_json, last_updated)
           VALUES ($1, $2, now())
           ON CONFLICT (ticker)
           DO UPDATE SET data_json = EXCLUDED.data_json, last_updated = EXCLUDED.last_updated",
            params = list(tk, items[[tk]])
          )
        }
        DBI::dbCommit(con)
      },
      error = function(e) {
        try(DBI::dbRollback(con), silent = TRUE)
        message("Error in batch cache save: ", e$message)
      }
    )
    invisible(TRUE)
  })
}

# =========================
# MIGRACIONES AUTOMÁTICAS
# =========================
db_ensure_schema <- function() {
  # Esta función verifica si existen las columnas de Stripe.
  # Si no, las crea.
  tryCatch(
    {
      .with_conn({
        # 1. Verificar si existe la tabla users
        exists <- DBI::dbGetQuery(con, "SELECT to_regclass('public.users') as t")
        if (is.na(exists$t) || is.null(exists$t)) {
          # Si no existe la tabla, normalmente aquí iría el CREATE TABLE completo.
          # Asumimos que la tabla base existe por diseño actual.
          return(invisible(FALSE))
        }

        # --- Tabla de Caché ---
        has_cache <- DBI::dbGetQuery(con, "SELECT to_regclass('public.cached_analysis') as t")
        if (is.na(has_cache$t) || is.null(has_cache$t)) {
          message("Creando tabla cached_analysis...")
          DBI::dbExecute(con, "
            CREATE TABLE cached_analysis (
              ticker TEXT PRIMARY KEY,
              data_json JSONB,
              last_updated TIMESTAMP DEFAULT now()
            )
          ")
        }


        # 2. Verificar columnas
        # stripe_customer_id
        has_cust <- DBI::dbGetQuery(
          con,
          "SELECT column_name FROM information_schema.columns
         WHERE table_name='users' AND column_name='stripe_customer_id'"
        )

        if (nrow(has_cust) == 0) {
          message("Agregando columna stripe_customer_id...")
          DBI::dbExecute(con, "ALTER TABLE users ADD COLUMN stripe_customer_id TEXT DEFAULT NULL")
        }

        # stripe_subscription_id
        has_sub <- DBI::dbGetQuery(
          con,
          "SELECT column_name FROM information_schema.columns
         WHERE table_name='users' AND column_name='stripe_subscription_id'"
        )

        if (nrow(has_sub) == 0) {
          message("Agregando columna stripe_subscription_id...")
          DBI::dbExecute(con, "ALTER TABLE users ADD COLUMN stripe_subscription_id TEXT DEFAULT NULL")
        }

        # referral_code (unique string)
        has_ref <- DBI::dbGetQuery(
          con,
          "SELECT column_name FROM information_schema.columns
           WHERE table_name='users' AND column_name='referral_code'"
        )

        if (nrow(has_ref) == 0) {
          message("Agregando columna referral_code...")
          DBI::dbExecute(con, "ALTER TABLE users ADD COLUMN referral_code TEXT UNIQUE DEFAULT NULL")
        }

        # referred_by (integer fk)
        has_ref_by <- DBI::dbGetQuery(
          con,
          "SELECT column_name FROM information_schema.columns
           WHERE table_name='users' AND column_name='referred_by'"
        )

        if (nrow(has_ref_by) == 0) {
          message("Agregando columna referred_by...")
          DBI::dbExecute(con, "ALTER TABLE users ADD COLUMN referred_by TEXT DEFAULT NULL")
        } else {
          # Si ya existe pero por error se creó como INTEGER, intentar convertir a TEXT
          # (Solo si detectamos que id es tipo texto/uuid)
          res_type <- DBI::dbGetQuery(con, "SELECT data_type FROM information_schema.columns WHERE table_name='users' AND column_name='referred_by'")
          if (nrow(res_type) > 0 && res_type$data_type[[1]] == "integer") {
            message("Corrigiendo tipo de referred_by a TEXT...")
            DBI::dbExecute(con, "ALTER TABLE users ALTER COLUMN referred_by TYPE TEXT")
          }
        }

        # referral_wallet (numeric, default 0)
        has_wallet <- DBI::dbGetQuery(
          con,
          "SELECT column_name FROM information_schema.columns
           WHERE table_name='users' AND column_name='referral_wallet'"
        )

        if (nrow(has_wallet) == 0) {
          message("Agregando columna referral_wallet...")
          DBI::dbExecute(con, "ALTER TABLE users ADD COLUMN referral_wallet NUMERIC DEFAULT 0.0")
        }

        # email_verified (boolean, default false - BUT true for existing users to avoid locking them out)
        has_ver <- DBI::dbGetQuery(
          con,
          "SELECT column_name FROM information_schema.columns
           WHERE table_name='users' AND column_name='email_verified'"
        )

        if (nrow(has_ver) == 0) {
          message("Agregando columna email_verified...")
          # Default TRUE for existing users, future users will be FALSE via registration logic
          DBI::dbExecute(con, "ALTER TABLE users ADD COLUMN email_verified BOOLEAN DEFAULT TRUE")
        }

        # verification_token (text)
        has_vtok <- DBI::dbGetQuery(
          con,
          "SELECT column_name FROM information_schema.columns
           WHERE table_name='users' AND column_name='verification_token'"
        )

        if (nrow(has_vtok) == 0) {
          message("Agregando columna verification_token...")
          DBI::dbExecute(con, "ALTER TABLE users ADD COLUMN verification_token TEXT DEFAULT NULL")
        }

        # membership_expires_at (timestamptz)
        has_mexp <- DBI::dbGetQuery(
          con,
          "SELECT column_name FROM information_schema.columns
           WHERE table_name='users' AND column_name='membership_expires_at'"
        )

        if (nrow(has_mexp) == 0) {
          message("Agregando columna membership_expires_at...")
          DBI::dbExecute(con, "ALTER TABLE users ADD COLUMN membership_expires_at TIMESTAMPTZ DEFAULT NULL")
        }

        # --- MIGRACIÓN: Asignar refXXX a usuarios antiguos que tengan NULL ---
        # Buscamos IDs con ref_code nulo
        null_refs <- DBI::dbGetQuery(con, "SELECT id FROM users WHERE referral_code IS NULL OR referral_code = '' ORDER BY created_at ASC")
        if (nrow(null_refs) > 0) {
          message("Migrando ", nrow(null_refs), " usuarios sin código de referido...")
          for (uid in null_refs$id) {
            new_code <- db_generate_next_ref_code(con)
            # Validar colisión
            chk <- DBI::dbGetQuery(con, "SELECT 1 FROM users WHERE referral_code=$1", params = list(new_code))
            if (nrow(chk) > 0) {
              # Force next
              # Extraemos número y sumamos
              num <- as.integer(substring(new_code, 4))
              new_code <- sprintf("ref%03d", num + 1)
            }
            DBI::dbExecute(con, "UPDATE users SET referral_code=$1 WHERE id=$2", params = list(new_code, uid))
          }
        }
      })
    },
    error = function(e) {
      warning("Error en db_ensure_schema: ", e$message)
    }
  )
}

# Asegura que la tabla login_attempts existe (se llama desde db_ensure_schema o por separado)
db_ensure_login_attempts_table <- function() {
  tryCatch(.with_conn({
    DBI::dbExecute(con, "
      CREATE TABLE IF NOT EXISTS login_attempts (
        id BIGSERIAL PRIMARY KEY,
        identifier TEXT NOT NULL,
        attempted_at TIMESTAMPTZ DEFAULT now(),
        success BOOLEAN DEFAULT false
      )
    ")
    DBI::dbExecute(con, "
      CREATE INDEX IF NOT EXISTS idx_login_attempts_identifier
      ON login_attempts(identifier, attempted_at)
    ")
  }), error = function(e) message("[DB] Error creando login_attempts: ", e$message))
}
