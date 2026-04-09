suppressWarnings({
  library(DBI)
  library(dplyr)
  library(yaml)
  library(httr)
})

# -------- Utilidades --------
# %||% se define en R/helpers.R (versión robusta). No redefinir aquí.
.nz <- function(x) {
  is.character(x) && length(x) > 0 && nzchar(x[1])
}
.auth_now <- function() {
  format(Sys.time(), "%Y-%m-%d %H:%M:%S")
}

# -------- Config --------
.auth_load_cfg <- function() {
  # 1. Environment variables takes priority
  read_env <- function(key) {
    v <- Sys.getenv(key, unset = NA_character_)
    if (!is.na(v) && nzchar(v)) v else NULL
  }

  cfg_env <- list(
    host         = read_env("SMTP_HOST") %||% read_env("EMAIL_HOST"),
    port         = read_env("SMTP_PORT") %||% read_env("EMAIL_PORT"),
    user         = read_env("SMTP_USER") %||% read_env("EMAIL_USER"),
    pass         = read_env("SMTP_PASS") %||% read_env("EMAIL_PASS") %||% read_env("SMTP_PASSWORD"),
    from         = read_env("SMTP_FROM") %||% read_env("EMAIL_FROM"),
    admin_email  = read_env("ADMIN_EMAIL") %||% read_env("SMTP_ADMIN")
  )

  # Remove NULLs
  cfg_env <- cfg_env[!vapply(cfg_env, is.null, logical(1))]

  # 2. smtp.yml as fallback
  path <- "smtp.yml"
  cfg_file <- list()
  if (file.exists(path)) {
    cfg_file <- tryCatch(yaml::read_yaml(path), error = function(e) {
      message("Error reading smtp.yml: ", e$message)
      list()
    })
  }

  # Merge: Env vars override file (manual merge to avoid dependency on utils::modifyList)
  cfg <- as.list(cfg_file)
  for (nm in names(cfg_env)) {
    cfg[[nm]] <- cfg_env[[nm]]
  }
  cfg
}

.app_base_url <- function() {
  # IMPORTANTE: Prioridad 1 -> Lo que el usuario escribió manualmente en smtp.yml (app_base_url)
  cfg <- .auth_load_cfg()
  if (.nz(cfg$app_base_url)) {
    # Asegurarnos de que no termine en slash (limpiar url)
    return(sub("/+$", "", cfg$app_base_url))
  }
  
  # Prioridad 2 -> Variables de entorno alojadas (PUBLIC_BASE_URL)
  env_public <- Sys.getenv("PUBLIC_BASE_URL", unset = NA_character_)
  if (!is.na(env_public) && nzchar(env_public)) {
    return(sub("/+$", "", env_public))
  }
  
  # Prioridad 3 -> Fallbacks genéricos
  env_app <- Sys.getenv("APP_BASE_URL", unset = NA_character_)
  if (!is.na(env_app) && nzchar(env_app)) {
    return(sub("/+$", "", env_app))
  }
  
  "https://app.pugaxtrade.com"
}

auth_build_reset_link <- function(token) {
  paste0(.app_base_url(), "?reset=", token)
}

# -------- Emails (sin emayili — usa curl del sistema para evitar conflictos de xfun) --------

.email_admin_to <- function(cfg) {
  admin <- cfg$admin_email %||% Sys.getenv("SUPER_ADMIN_EMAIL", "")
  if (!nzchar(admin)) {
    message("[Email] ADVERTENCIA: No hay admin_email ni SUPER_ADMIN_EMAIL configurado. Emails de admin no se enviarán.")
    return(NULL)
  }
  admin
}

# Construye el contenido MIME plano del correo
.build_mime_message <- function(from, to, subject, body_text) {
  paste0(
    "From: ", from, "\r\n",
    "To: ", to, "\r\n",
    "Subject: ", subject, "\r\n",
    "MIME-Version: 1.0\r\n",
    "Content-Type: text/plain; charset=UTF-8\r\n",
    "Content-Transfer-Encoding: 8bit\r\n",
    "\r\n",
    body_text, "\r\n"
  )
}

# Envía de forma aislada (Process Isolation) para evitar crashes del proceso Shiny
.fire_and_forget_email <- function(to_email, subject, body_text) {
  if (is.null(to_email) || !nzchar(to_email)) return(invisible(FALSE))
  cfg <- .auth_load_cfg()
  if (is.null(cfg$host) || !nzchar(cfg$host)) return(invisible(FALSE))

  from_addr  <- cfg$from  %||% "no-reply@pugax.trade"
  host       <- cfg$host
  port       <- cfg$port  %||% "587"
  user       <- cfg$user  %||% ""
  pass       <- cfg$pass  %||% ""
  pass_clean <- gsub("\\s+", "", pass)

  # Mime Body (RFC 5322)
  mime_body <- .build_mime_message(from_addr, to_email, subject, body_text)

  # Para el "MAIL FROM" (envelope), Gmail rechaza espacios o corchetes:
  envelope_from <- if (grepl("<.*>", from_addr)) sub(".*<(.*)>.*", "\\1", from_addr) else from_addr
  
  is_windows <- .Platform$OS.type == "windows"

  if (is_windows) {
    # ── MODO WINDOWS (Nativo R) ──────────────────────────────────────────────
    # Reportado como estable en escritorio; usamos tryCatch silencioso
    if (!requireNamespace("curl", quietly = TRUE)) return(invisible(FALSE))
    scheme <- if (as.character(port) == "465") "smtps://" else "smtp://"
    try({
      curl::send_mail(
        mail_from = envelope_from, mail_rcpt = trimws(to_email),
        message = mime_body, smtp_server = paste0(scheme, host, ":", port),
        username = user, password = pass_clean,
        use_ssl = "force", ssl_verifypeer = 1L, timeout = 10L, verbose = FALSE
      )
    }, silent = TRUE)
  } else {
    # ── MODO LINUX/DOCKER (Process Isolation) ────────────────────────────────
    # La solución definitiva para evitar "Disconnected from the server" (segfaults)
    # Usamos el comando 'curl' del sistema en SEGUNDO PLANO (wait = FALSE)
    
    tmp_msg <- tempfile(fileext = ".eml")
    writeLines(mime_body, tmp_msg, useBytes = TRUE)
    
    scheme <- if (as.character(port) == "465") "smtps://" else "smtp://"
    url <- paste0(scheme, host, ":", port)
    
    # Argumentos para curl del sistema
    args <- c(
      "--url", shQuote(url),
      "--ssl-reqd",
      "--user", shQuote(paste0(user, ":", pass_clean)),
      "--mail-from", shQuote(envelope_from),
      "--mail-rcpt", shQuote(trimws(to_email)),
      "--upload-file", shQuote(tmp_msg),
      "--silent", "--show-error"
    )
    
    # Invocamos de forma asíncrona. Si falla curl, R ni se entera. No hay desconexión.
    try({
      system2("curl", args = args, wait = FALSE, stdout = FALSE, stderr = FALSE)
      # Nota: el archivo temporal será limpiado por el sistema o podemos intentar borrarlo en R
      # pero como es wait=FALSE, no podemos borrarlo inmediatamente. El SO lo limpia en /tmp.
    }, silent = TRUE)
  }
  
  invisible(TRUE)
}


auth_send_welcome_credentials <- function(user_row, username, plain_password = NULL) {
  
  # SEGURIDAD CRÍTICA: Nunca se envían contraseñas en texto plano.
  body <- paste0(
    "¡Bienvenido/a a PugaX Trade!\n\n",
    "Tu cuenta fue creada exitosamente.\n\n",
    "Usuario: ", username, "\n",
    "Contraseña: (Oculta por seguridad)\n\n",
    "Accede a la app cuando gustes: ", .app_base_url(), "\n\n",
    "Saludos,\nEquipo PugaX\n"
  )

  .fire_and_forget_email(user_row$email[[1]], "Bienvenido/a – PugaX Trade", body)
}

auth_notify_admin_new_user <- function(user_row, plain_password = NULL) {
  cfg <- .auth_load_cfg()
  
  body <- paste0(
    "Nuevo usuario (", .auth_now(), "):\n\n",
    "Usuario: ", user_row$username[[1]], "\n",
    "Correo: ", user_row$email[[1]], "\n",
    "Nombre: ", user_row$name[[1]], "\n",
    "País: ", user_row$country[[1]], "\n",
    "Tel: ", user_row$phone[[1]], "\n",
    "\n(Contraseña no incluida por seguridad)\n"
  )

  .fire_and_forget_email(.email_admin_to(cfg), "Nuevo registro – PugaX", body)
}

auth_send_reset_email <- function(user_row, reset_link) {
  body <- paste0(
    "Hola ", user_row$name[[1]], ",\n\n",
    "Para restablecer tu contraseña usa este enlace:\n",
    reset_link, "\n\n",
    "Si no solicitaste el cambio, ignora este correo.\n\n",
    "Equipo PugaX\n"
  )

  .fire_and_forget_email(user_row$email[[1]], "Restablecer contraseña - PugaX", body)
}

auth_send_verification_email <- function(user_row, token) {
  verify_link <- paste0(.app_base_url(), "?verify=", token)

  safe_val <- function(x) {
    if (is.null(x) || length(x) == 0 || is.na(x)) {
      return("")
    }
    as.character(x)
  }

  body <- paste0(
    "Hola ", safe_val(user_row$name[[1]]), ",\n\n",
    "Gracias por registrarte en PugaX Trade.\n",
    "Por favor verifica tu correo electrónico haciendo clic en el siguiente enlace:\n\n",
    verify_link, "\n\n",
    "Si no creaste esta cuenta, puedes ignorar este mensaje.\n\n",
    "Saludos,\nEquipo PugaX\n"
  )

  message("[Auth] Enviando email de verificación a: ", user_row$email[[1]])
  .fire_and_forget_email(user_row$email[[1]], "Verifica tu cuenta – PugaX Trade", body)
}

auth_notify_admin_password_change <- function(user_row) {
  cfg <- .auth_load_cfg()
  body <- paste0(
    "Aviso de seguridad (", .auth_now(), "):\n\n",
    "El usuario ", user_row$username[[1]], " ha cambiado su contraseña.\n",
    "Correo: ", user_row$email[[1]], "\n\n",
    "Si consideras esto sospechoso, revisa el panel de administración.\n"
  )

  .fire_and_forget_email(.email_admin_to(cfg), "Cambio de contraseña – PugaX App", body)
}

auth_notify_admin_account_deleted <- function(user_id, username, user_email) {
  cfg <- .auth_load_cfg()
  body <- paste0(
    "Aviso de Admin (", .auth_now(), "):\n\n",
    "Se ha ELIMINADO la cuenta del usuario:\n",
    "ID: ", user_id, "\n",
    "Usuario: ", username, "\n",
    "Correo: ", user_email, "\n\n",
    "Acción realizada por un administrador.\n"
  )

  .fire_and_forget_email(.email_admin_to(cfg), "Usuario Eliminado – PugaX App", body)
}

auth_notify_admin_action <- function(action_name, details_text) {
  cfg <- .auth_load_cfg()
  body <- paste0(
    "Registro de Actividad Admin (", .auth_now(), "):\n\n",
    "Acción: ", action_name, "\n",
    "Detalles:\n", details_text, "\n\n",
    "Esta acción fue ejecutada desde el Panel de Administración.\n"
  )

  .fire_and_forget_email(.email_admin_to(cfg), paste0("Admin Log: ", action_name), body)
}

auth_send_account_deleted_email <- function(user_email, user_name) {
  body <- paste0(
    "Hola ", user_name, ",\n\n",
    "Te informamos que tu cuenta en PugaX Trade ha sido eliminada permanentemente por el administrador.\n",
    "Si consideras que esto es un error, por favor ponte en contacto con el soporte.\n\n",
    "PugaX App: https://app.pugaxtrade.com\n\n",
    "Saludos,\nEquipo PugaX\n"
  )

  .fire_and_forget_email(user_email, "Cuenta Eliminada – PugaX Trade", body)
  list(ok = TRUE)
}

auth_send_payment_success <- function(user_row) {
  body <- paste0(
    "Hola ", user_row$name[[1]], ",\n\n",
    "¡Pago completado! Tu membresía ha sido activada.\n\n",
    "Accede: https://app.pugaxtrade.com\n\n",
    "Gracias por tu confianza.\n",
    "Equipo PugaX\n"
  )
  .fire_and_forget_email(user_row$email[[1]], "Pago completado – PugaX Trade", body)
}

auth_send_generic_email <- function(to_email, subject, body_text) {
  .fire_and_forget_email(to_email, subject, body_text)
  list(ok = TRUE)
}

# -------- Stripe config/helpers --------
stripe_cfg <- function() {
  cfgf <- .auth_load_cfg()
  read_env <- function(key, fallback = "") {
    v <- Sys.getenv(key, unset = NA_character_)
    if (!is.na(v) && nzchar(v)) {
      return(v)
    }
    if (is.null(fallback)) "" else as.character(fallback)
  }
  secret <- read_env("STRIPE_SECRET_KEY", cfgf$stripe_secret_key)
  publishable <- read_env("STRIPE_PUBLISHABLE_KEY", cfgf$stripe_publishable_key)
  price_id <- read_env("STRIPE_PRICE_ID", cfgf$stripe_price_id)
  app_base <- .app_base_url()
  allow_raw <- read_env("STRIPE_ALLOW_TEST", cfgf$stripe_allow_test %||% "0")
  allow_test <- tolower(as.character(allow_raw)) %in% c("1", "true", "yes", "t")

  list(
    secret       = if (nzchar(secret)) secret else NULL,
    publishable  = if (nzchar(publishable)) publishable else NULL,
    price_id     = if (nzchar(price_id)) price_id else NULL,
    app_base_url = app_base,
    allow_test   = allow_test
  )
}

stripe_mode_from_secret <- function(secret) {
  if (is.null(secret) || !nzchar(secret)) {
    return("unknown")
  }
  if (grepl("^sk_live_", secret)) {
    return("live")
  }
  if (grepl("^sk_test_", secret)) {
    return("test")
  }
  "unknown"
}

# -------- Stripe Checkout --------
auth_create_checkout_session <- function(user_row) {
  cfg <- stripe_cfg()
  if (is.null(cfg$secret) || is.null(cfg$price_id) || is.null(cfg$publishable)) {
    miss <- c()
    if (is.null(cfg$secret)) miss <- c(miss, "STRIPE_SECRET_KEY")
    if (is.null(cfg$publishable)) miss <- c(miss, "STRIPE_PUBLISHABLE_KEY")
    if (is.null(cfg$price_id)) miss <- c(miss, "STRIPE_PRICE_ID")
    return(list(ok = FALSE, message = paste("Stripe no configurado. Faltan:", paste(miss, collapse = ", "))))
  }

  if (grepl("^sk_test_", cfg$secret) && !isTRUE(cfg$allow_test)) {
    return(list(ok = FALSE, message = "Modo TEST detectado. Activa STRIPE_ALLOW_TEST=1 para pruebas."))
  }

  success_url <- paste0(cfg$app_base_url, "?paid=1&session_id={CHECKOUT_SESSION_ID}")
  cancel_url <- paste0(cfg$app_base_url, "?paid=0")

  res <- try(httr::POST(
    "https://api.stripe.com/v1/checkout/sessions",
    httr::authenticate(cfg$secret, ""),
    httr::add_headers(`Idempotency-Key` = paste0("ik_", as.integer(Sys.time()), "_", sample(1000:9999, 1))),
    body = list(
      mode = "subscription",
      success_url = success_url,
      cancel_url = cancel_url,
      client_reference_id = as.character(user_row$id[[1]]),
      "line_items[0][price]" = cfg$price_id,
      "line_items[0][quantity]" = 1,
      customer_email = user_row$email[[1]],
      "metadata[username]" = user_row$username[[1]],
      "metadata[env]" = if (cfg$allow_test) "test" else "live",
      "subscription_data[description]" = "Puga Inversor – Membresía Anual",
      "allow_promotion_codes" = "true",
      "automatic_tax[enabled]" = "true"
    ),
    encode = "form"
  ), silent = TRUE)

  if (inherits(res, "try-error")) {
    return(list(ok = FALSE, message = "No se pudo crear la sesión de pago (Stripe)."))
  }
  if (httr::status_code(res) >= 300) {
    ct <- try(httr::content(res, as = "text", encoding = "UTF-8"), silent = TRUE)
    return(list(ok = FALSE, message = paste("Stripe error:", if (is.character(ct)) ct else httr::status_code(res))))
  }
  ct <- httr::content(res)
  if (is.null(ct$url)) {
    return(list(ok = FALSE, message = "Stripe respondió sin URL."))
  }
  list(ok = TRUE, url = ct$url, id = ct$id %||% NA_character_)
}

auth_check_checkout_paid <- function(session_id, retries = 6, wait_seconds = 1) {
  cfg <- stripe_cfg()
  if (is.null(cfg$secret)) {
    return(list(ok = FALSE, message = "Stripe no configurado."))
  }
  if (is.null(session_id) || !nzchar(session_id)) {
    return(list(ok = FALSE, message = "Falta session_id."))
  }

  email <- NA_character_
  customer <- NA_character_
  subscription <- NA_character_
  for (i in seq_len(retries)) {
    res <- try(httr::GET(
      paste0("https://api.stripe.com/v1/checkout/sessions/", session_id),
      httr::authenticate(cfg$secret, "")
    ), silent = TRUE)
    if (!inherits(res, "try-error") && httr::status_code(res) %in% 200:299) {
      ct <- httr::content(res)
      customer <- ct$customer %||% NA_character_
      subscription <- ct$subscription %||% NA_character_
      if (!is.null(ct$customer_details) && !is.null(ct$customer_details$email)) email <- ct$customer_details$email
      if (is.null(email) || !nzchar(email)) email <- ct$customer_email %||% NA_character_
      status <- ct$status %||% ""
      payment_status <- ct$payment_status %||% ""
      if (identical(status, "complete") || identical(payment_status, "paid")) {
        return(list(ok = TRUE, paid = TRUE, email = email, customer = customer, subscription = subscription))
      }
    }
    Sys.sleep(wait_seconds)
  }
  list(ok = TRUE, paid = FALSE, email = email, customer = customer, subscription = subscription)
}

# -------- Subscriptions (para "Renovación automática") --------
.stripe_subscription_get <- function(sub_id) {
  cfg <- stripe_cfg()
  if (is.null(cfg$secret) || !.nz(sub_id)) {
    return(NULL)
  }
  res <- try(httr::GET(
    paste0("https://api.stripe.com/v1/subscriptions/", sub_id),
    httr::authenticate(cfg$secret, "")
  ), silent = TRUE)
  if (inherits(res, "try-error") || !(httr::status_code(res) %in% 200:299)) {
    return(NULL)
  }
  try(httr::content(res), silent = TRUE)
}

.stripe_subscription_set_cancel_at_period_end <- function(sub_id, flag) {
  cfg <- stripe_cfg()
  if (is.null(cfg$secret) || !.nz(sub_id)) {
    return(FALSE)
  }
  res <- try(httr::POST(
    paste0("https://api.stripe.com/v1/subscriptions/", sub_id),
    httr::authenticate(cfg$secret, ""),
    body = list(cancel_at_period_end = tolower(as.character(flag))),
    encode = "form"
  ), silent = TRUE)
  if (inherits(res, "try-error") || !(httr::status_code(res) %in% 200:299)) {
    return(FALSE)
  }
  TRUE
}

auth_get_subscription_status <- function(user_row) {
  sub_id <- user_row$stripe_subscription_id[[1]]
  st <- .stripe_subscription_get(sub_id)
  if (is.null(st)) {
    return(NULL)
  }
  # auto-renew = TRUE si NO está marcado cancel_at_period_end
  list(auto_renew = isFALSE(isTRUE(st$cancel_at_period_end)))
}

auth_cancel_auto_renew <- function(user_row) {
  sub_id <- user_row$stripe_subscription_id[[1]]
  .stripe_subscription_set_cancel_at_period_end(sub_id, TRUE)
}

auth_reactivate_auto_renew <- function(user_row) {
  sub_id <- user_row$stripe_subscription_id[[1]]
  .stripe_subscription_set_cancel_at_period_end(sub_id, FALSE)
}

auth_renew_now <- function(user_row) {
  cfg <- stripe_cfg()
  if (is.null(cfg$secret)) {
    return(list(ok = FALSE, message = "Stripe no configurado."))
  }
  customer <- user_row$stripe_customer_id[[1]]
  if (!.nz(customer)) {
    return(list(ok = FALSE, message = "No hay cliente Stripe enlazado."))
  }

  res <- try(httr::POST(
    url = "https://api.stripe.com/v1/billing_portal/sessions",
    httr::authenticate(cfg$secret, ""),
    body = list(customer = customer, return_url = cfg$app_base_url),
    encode = "form"
  ), silent = TRUE)
  if (inherits(res, "try-error") || !(httr::status_code(res) %in% 200:299)) {
    return(list(ok = FALSE, message = "No se pudo crear la sesión del portal."))
  }
  js <- try(httr::content(res, as = "parsed", type = "application/json"), silent = TRUE)
  if (is.null(js$url)) {
    return(list(ok = FALSE, message = "Stripe no devolvió URL de portal."))
  }
  list(ok = TRUE, url = js$url)
}
