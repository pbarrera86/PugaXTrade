suppressWarnings({
  library(shiny)
  library(shinyWidgets)
  library(shinyjs)
  library(DT)

  library(dplyr)
  library(DBI)
  library(RPostgres)

  library(jsonlite)
  library(lubridate)
  library(httr)

  library(bslib)
  library(thematic)
  library(pool)
  library(promises)
  library(rlang) # explicitly load rlang
})

# %||% viene de R/helpers.R (sourced a continuación)


# Enable thematic for auto-theming plots
thematic_shiny(font = "auto")

# ----------------- PARALLEL SETUP -----------------
library(future)
library(furrr)

# Eliminar el límite duro de parallelly (300% CPU) que falla en máquinas con mc.cores=1
# Esto es seguro porque usamos multisession (workers Rscript separados, no mclapply)
options(parallelly.maxWorkers.localhost = Inf)

# Número de workers: mínimo 1, máximo 4 para no saturar máquinas pequeñas
n_cores <- max(1L, min(4L, parallel::detectCores(logical = TRUE) %||% 2L - 1L))
plan(multisession, workers = n_cores)
message(sprintf("[Parallel] Plan: multisession con %d workers", n_cores))

# ----------------- Cargar .Renviron del proyecto y del usuario -----------------
proj_env <- file.path(getwd(), ".Renviron")
if (file.exists(proj_env)) readRenviron(proj_env)
user_env <- file.path(path.expand("~"), ".Renviron")
if (file.exists(user_env)) readRenviron(user_env)

# --------- PARCHE DE COMPATIBILIDAD (PUBLIC -> PUBLISHABLE) + DIAGNÓSTICO -------
# Si alguien definió STRIPE_PUBLIC_KEY, mapea a STRIPE_PUBLISHABLE_KEY.
if (!nzchar(Sys.getenv("STRIPE_PUBLISHABLE_KEY")) && nzchar(Sys.getenv("STRIPE_PUBLIC_KEY"))) {
  Sys.setenv(STRIPE_PUBLISHABLE_KEY = Sys.getenv("STRIPE_PUBLIC_KEY"))
}
# Diagnóstico (aparece en la consola/Logs de shiny)
local({
  keys <- c("PUBLIC_BASE_URL", "STRIPE_SECRET_KEY", "STRIPE_PUBLISHABLE_KEY", "STRIPE_PRICE_ID")
  vals <- Sys.getenv(keys, NA_character_)
  lens <- nchar(ifelse(is.na(vals), "", vals))
  status <- ifelse(is.na(vals) | lens == 0, "MISSING", paste0("ok(", lens, ")"))
  message("[StripeDiag] ", paste0(keys, "=", status, collapse = " | "))
})

# ----------------- Constantes públicas -----------------
PUBLIC_BASE_URL <- Sys.getenv("PUBLIC_BASE_URL", "https://app.pugaxtrade.com")
# Sobrescritura maestra: Si en smtp.yml definiste un dominio custom (como app.pugaxtrade.com), este debe dominar incluso al environment de ShinyApps
if (file.exists("smtp.yml")) {
  try({
    yml_cfg <- yaml::read_yaml("smtp.yml")
    if (!is.null(yml_cfg$app_base_url) && nzchar(yml_cfg$app_base_url)) {
      PUBLIC_BASE_URL <- sub("/+$", "", yml_cfg$app_base_url)
    }
  }, silent = TRUE)
}
# Asignamos la variable real limpia
Sys.setenv(PUBLIC_BASE_URL = PUBLIC_BASE_URL)
# ----------------- THEME -----------------
my_theme <- bs_theme(
  version = 5,
  bootswatch = "cyborg",
  primary = "#0071e3",
  secondary = "#ffcc66",
  success = "#2ecc71",
  warning = "#ff9f0a",
  danger = "#ff6b6b",
  base_font = font_google("Inter"),
  heading_font = font_google("Outfit"),
  "card-border-radius" = "16px",
  "btn-border-radius" = "14px",
  "input-border-radius" = "14px"
)

# Helper centralizado para verificar super-admin se movió a R/db.R para visibilidad global modular.

# ----------------- Lectura de cookie HttpOnly desde HTTP header -----------------
# El browser envía automáticamente la cookie en cada request.
# La leemos del header HTTP_COOKIE para inyectar el token en el HTML inicial
# sin que JS pueda leer la cookie directamente (flag HttpOnly).
.parse_cookie_value <- function(cookie_header, name) {
  if (is.null(cookie_header) || !nzchar(cookie_header)) return("")
  pairs <- strsplit(cookie_header, ";\\s*")[[1]]
  for (pair in pairs) {
    kv <- strsplit(pair, "=", fixed = TRUE)[[1]]
    if (length(kv) >= 2 && trimws(kv[1]) == name) {
      # Solo aceptamos tokens hex (nuestro rnd_token genera solo hex)
      raw <- paste(kv[-1], collapse = "=")
      clean <- gsub("[^a-fA-F0-9]", "", utils::URLdecode(raw))
      return(clean)
    }
  }
  ""
}

# Detecta si la conexión es HTTPS (Dokploy usa reverse proxy con X-Forwarded-Proto)
.is_https <- function(req) {
  proto <- req$HTTP_X_FORWARDED_PROTO %||% ""
  identical(tolower(proto), "https") || identical(req$SERVER_PORT, "443")
}

# Construye la cabecera Set-Cookie con los flags correctos
.make_set_cookie <- function(name, value, max_age_seconds = NULL, clear = FALSE) {
  val <- if (isTRUE(clear)) "" else value
  age <- if (isTRUE(clear)) 0L else (max_age_seconds %||% 604800L)  # 7 días por defecto
  flags <- sprintf("%s=%s; Path=/; HttpOnly; SameSite=Lax; Max-Age=%d", name, val, as.integer(age))
  flags
}

# Endpoint: /auth/set-session — establece la cookie HttpOnly y redirige al app
.handle_set_session <- function(req) {
  q <- shiny::parseQueryString(req$QUERY_STRING %||% "")
  tok <- gsub("[^a-fA-F0-9]", "", q$tok %||% "")  # Sanitizar: solo hex
  if (!nzchar(tok)) {
    return(shiny::httpResponse(400, "text/plain", "Bad Request"))
  }
  # Validar que el token existe en la DB antes de setear la cookie
  u <- tryCatch(db_find_user_by_token(tok), error = function(e) NULL)
  if (is.null(u) || nrow(u) == 0) {
    return(shiny::httpResponse(401, "text/plain", "Token invalido o expirado"))
  }
  cookie <- .make_set_cookie("puga_auth", tok, max_age_seconds = 604800L)
  if (.is_https(req)) cookie <- paste0(cookie, "; Secure")
  redirect_url <- PUBLIC_BASE_URL
  shiny::httpResponse(302, "text/plain", "Redirecting...",
    list("Set-Cookie" = cookie, "Location" = redirect_url))
}

# Endpoint: /auth/logout — limpia la cookie HttpOnly y redirige al app
.handle_logout_cookie <- function(req) {
  clear_cookie <- .make_set_cookie("puga_auth", "", clear = TRUE)
  if (.is_https(req)) clear_cookie <- paste0(clear_cookie, "; Secure")
  shiny::httpResponse(302, "text/plain", "Logged out",
    list("Set-Cookie" = clear_cookie, "Location" = PUBLIC_BASE_URL))
}





# ----------------- Núcleo y Auth/DB -----------------
source("R/helpers.R", encoding = "UTF-8")
source("R/db.R", encoding = "UTF-8") # pool + helpers Postgres
source("R/finance_core.R", encoding = "UTF-8") # ANÁLISIS + Excel
source("R/institutional_core.R", encoding = "UTF-8")
# auth_helpers.R (Configuración SMTP y URL base)
tryCatch(source("R/auth_helpers.R", encoding = "UTF-8"), error = function(e) {
  message("Warning: Could not source R/auth_helpers.R: ", e$message)
})

source("R/admin_module.R", encoding = "UTF-8")
source("R/auth_module.R", encoding = "UTF-8")
source("R/institutional_ui_helpers.R", encoding = "UTF-8")
source("R/institutional_module.R", encoding = "UTF-8")
# standard_analysis_module.R eliminado — solo existe el módulo institucional
source("R/ui_modules.R", encoding = "UTF-8")
source("R/stripe_webhooks.R", encoding = "UTF-8")
# ----------------- Stripe helpers mejorados -----------------
stripe_has_env <- function() {
  all(nzchar(Sys.getenv(c("STRIPE_SECRET_KEY", "STRIPE_PUBLISHABLE_KEY", "STRIPE_PRICE_ID"))))
}
stripe_mode_from_key <- function() {
  sk <- Sys.getenv("STRIPE_SECRET_KEY", "")
  if (!nzchar(sk)) {
    return(NA_character_)
  }
  if (startsWith(sk, "sk_test_")) {
    return("test")
  }
  if (startsWith(sk, "sk_live_")) {
    return("live")
  }
  NA_character_
}
stripe_error_msg <- function(resp) {
  if (inherits(resp, "try-error")) {
    return("No se pudo contactar Stripe.")
  }
  js <- try(httr::content(resp, as = "parsed", type = "application/json"), silent = TRUE)
  if (is.list(js) && !is.null(js$error$message)) {
    return(js$error$message)
  }
  if (httr::status_code(resp) >= 400) {
    return(paste("HTTP", httr::status_code(resp)))
  }
  "Respuesta inválida de Stripe."
}
stripe_validate_price_exists <- function(price_id) {
  secret <- Sys.getenv("STRIPE_SECRET_KEY", "")
  if (!nzchar(secret) || !nzchar(price_id)) {
    return(list(ok = FALSE, message = "Faltan claves o PRICE."))
  }
  r <- try(httr::GET(
    paste0("https://api.stripe.com/v1/prices/", price_id),
    httr::add_headers(Authorization = paste("Bearer", secret))
  ), silent = TRUE)
  if (inherits(r, "try-error")) {
    return(list(ok = FALSE, message = "No se pudo contactar Stripe (price)."))
  }
  if (httr::status_code(r) == 200) {
    return(list(ok = TRUE))
  }
  list(ok = FALSE, message = stripe_error_msg(r))
}
stripe_validate_customer_exists <- function(customer_id) {
  secret <- Sys.getenv("STRIPE_SECRET_KEY", "")
  if (!nzchar(secret) || !nzchar(customer_id)) {
    return(list(ok = FALSE, message = "Falta customer o clave."))
  }
  r <- try(httr::GET(
    paste0("https://api.stripe.com/v1/customers/", customer_id),
    httr::add_headers(Authorization = paste("Bearer", secret))
  ), silent = TRUE)
  if (inherits(r, "try-error")) {
    return(list(ok = FALSE, message = "No se pudo contactar Stripe (customer)."))
  }
  if (httr::status_code(r) == 200) {
    return(list(ok = TRUE))
  }
  list(ok = FALSE, message = stripe_error_msg(r))
}

# === (Parche 1) Obtener sesión y guardar IDs en DB ===
stripe_get_session_info <- function(session_id) {
  secret <- Sys.getenv("STRIPE_SECRET_KEY", "")
  if (!nzchar(secret) || !nzchar(session_id)) {
    return(NULL)
  }
  url <- paste0(
    "https://api.stripe.com/v1/checkout/sessions/", session_id,
    "?expand[]=customer&expand[]=subscription"
  )
  r <- try(httr::GET(url, httr::add_headers(Authorization = paste("Bearer", secret))), silent = TRUE)
  if (inherits(r, "try-error") || httr::status_code(r) != 200) {
    return(NULL)
  }
  js <- try(httr::content(r, as = "parsed", type = "application/json"), silent = TRUE)
  if (inherits(js, "try-error") || is.null(js)) {
    return(NULL)
  }
  out <- list(customer_id = NA_character_, subscription_id = NA_character_)
  if (!is.null(js$customer)) {
    if (is.list(js$customer) && !is.null(js$customer$id)) out$customer_id <- js$customer$id
    if (is.character(js$customer)) out$customer_id <- js$customer
  }
  if (!is.null(js$subscription)) {
    if (is.list(js$subscription) && !is.null(js$subscription$id)) out$subscription_id <- js$subscription$id
    if (is.character(js$subscription)) out$subscription_id <- js$subscription
  }
  out
}

# db_update_user_stripe_ids — definida en R/db.R (usar esa versión, con .with_conn())

# --------- Checkout (con validación de PRICE y mensajes claros) ----------
stripe_checkout_fallback <- function(user_row) {
  # 1. Validación exhaustiva de entorno
  keys <- c("STRIPE_SECRET_KEY", "STRIPE_PUBLISHABLE_KEY", "STRIPE_PRICE_ID")
  vals <- Sys.getenv(keys, "")
  missing <- keys[!nzchar(vals)]
  if (length(missing) > 0) {
    return(list(ok = FALSE, message = paste("Configuración incompleta. Faltan:", paste(missing, collapse = ", "))))
  }

  price <- Sys.getenv("STRIPE_PRICE_ID")
  chk <- stripe_validate_price_exists(price)
  if (!isTRUE(chk$ok)) {
    return(list(
      ok = FALSE,
      message = paste("Error en Stripe (Price):", chk$message %||% "no encontrado.")
    ))
  }

  secret <- Sys.getenv("STRIPE_SECRET_KEY")
  email <- user_row$email[[1]]
  # IMPORTANTE: incluimos session_id para validación al volver
  success_url <- paste0(PUBLIC_BASE_URL, "?paid=1&session_id={CHECKOUT_SESSION_ID}")
  cancel_url <- paste0(PUBLIC_BASE_URL, "?paid=0")

  req <- try(httr::POST(
    url = "https://api.stripe.com/v1/checkout/sessions",
    httr::add_headers(Authorization = paste("Bearer", secret)),
    body = list(
      mode = "subscription",
      success_url = success_url,
      cancel_url = cancel_url,
      customer_email = email,
      client_reference_id = user_row$id[[1]],
      "line_items[0][price]" = price,
      "line_items[0][quantity]" = 1,
      allow_promotion_codes = "true",
      "automatic_tax[enabled]" = "true",
      "billing_address_collection" = "auto"
    ),
    encode = "form"
  ), silent = TRUE)

  if (inherits(req, "try-error")) {
    return(list(ok = FALSE, message = "No se pudo contactar Stripe (checkout)."))
  }

  js <- try(httr::content(req, as = "parsed", type = "application/json"), silent = TRUE)
  if (is.null(js$id) || is.null(js$url)) {
    return(list(ok = FALSE, message = stripe_error_msg(req)))
  }
  list(ok = TRUE, url = js$url, session_id = js$id)
}

# --------- Portal (con validación de CUSTOMER y mensajes claros) ----------
stripe_portal_fallback <- function(user_row) {
  if (!stripe_has_env()) {
    return(list(ok = FALSE, message = "Stripe no configurado."))
  }
  customer <- user_row$stripe_customer_id[[1]]

  if (is.na(customer) || !nzchar(customer)) {
    return(list(ok = FALSE, message = "Aún no tienes un cliente Stripe enlazado en este modo. Realiza un pago primero."))
  }

  chk <- stripe_validate_customer_exists(customer)
  if (!isTRUE(chk$ok)) {
    return(list(
      ok = FALSE,
      message = paste0(
        "El customer '", customer, "' no existe en el modo '", stripe_mode_from_key(),
        "' o pertenece a otra cuenta. Detalle: ", chk$message,
        " — Si cambiaste de TEST↔LIVE, borra los customer antiguos en DB y paga de nuevo."
      )
    ))
  }

  secret <- Sys.getenv("STRIPE_SECRET_KEY")
  req <- try(httr::POST(
    url = "https://api.stripe.com/v1/billing_portal/sessions",
    httr::add_headers(Authorization = paste("Bearer", secret)),
    body = list(customer = customer, return_url = PUBLIC_BASE_URL),
    encode = "form"
  ), silent = TRUE)

  if (inherits(req, "try-error")) {
    return(list(ok = FALSE, message = "No se pudo abrir el portal (red)."))
  }
  js <- try(httr::content(req, as = "parsed", type = "application/json"), silent = TRUE)
  if (is.null(js$url)) {
    return(list(ok = FALSE, message = stripe_error_msg(req)))
  }
  list(ok = TRUE, url = js$url)
}

stripe_set_cancel_at_period_end <- function(sub_id, flag) {
  if (!stripe_has_env()) {
    return(FALSE)
  }
  secret <- Sys.getenv("STRIPE_SECRET_KEY")
  req <- try(httr::POST(
    url = paste0("https://api.stripe.com/v1/subscriptions/", sub_id),
    httr::add_headers(Authorization = paste("Bearer", secret)),
    body = list(cancel_at_period_end = tolower(as.character(flag))),
    encode = "form"
  ), silent = TRUE)
  if (inherits(req, "try-error")) {
    return(FALSE)
  }
  st <- try(httr::content(req, as = "parsed", type = "application/json"), silent = TRUE)
  !is.null(st$id)
}

# ----------------- Secciones UI han sido extraídas a R/ui_modules.R -----------------

# ----------------- Top bar + Contenedor principal -----------------
main_ui <- function(id) {
  ns <- NS(id)
  tagList(
    div(
      class = "w-100 topbar",
      div(
        class = "container d-flex flex-wrap align-items-center justify-content-between", style = "padding:10px 12px;",
        tags$div(style = "font-weight:700;letter-spacing:-.2px;", "PugaX Trade Inteligente"),
        div(
          class = "d-flex flex-wrap align-items-center gap-2",
          uiOutput(ns("hello_user")),
          actionButton(ns("nav_analysis"), "Análisis", class = "btn btn-info btn-sm w-100-sm"),
          actionButton(ns("nav_account"), "Mi cuenta", class = "btn btn-primary btn-sm w-100-sm"),
          actionButton(ns("nav_referral"), "Invitar Amigos", class = "btn btn-success btn-sm w-100-sm", style = "background-color: #28a745; border-color: #28a745;"),
          actionButton(ns("nav_membership"), "Membresía", class = "btn btn-warning btn-sm w-100-sm"),
          uiOutput(ns("admin_btn")),
          actionButton(ns("logout"), tagList(icon("sign-out"), "Cerrar sesión"), class = "btn btn-primary btn-sm w-100-sm")
        )
      )
    ),
    div(class = "container", uiOutput(ns("section_ui")))
  )
}

# ----------------- Pantallas de autenticación -----------------
main_module <- function(id, user_reactive, on_logout = function() {}) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    section <- reactiveVal("analysis")


    # Trigger manual para tabla usuarios
    users_trigger <- reactiveVal(0)

    # Tarea en background para Análisis No Bloqueante
    # Usamos try para evitar crashes si la versión de Shiny es muy antigua
    # Inicializar módulos independientes
    # Solo módulo institucional — el estándar fue eliminado
    institutional_server("inst", user_reactive = user_reactive)


    # ---- Estado de acceso al análisis (prueba vs membresía) ----
    can_analyze <- reactive({
      u <- user_reactive()
      if (is.null(u)) {
        return(FALSE)
      }
      if (is_super_admin(u)) {
        return(TRUE)
      }
      if (db_is_admin(u)) {
        return(TRUE)
      }
      if (db_membership_is_active(u)) {
        return(TRUE)
      }
      left <- suppressWarnings(as.numeric(db_trial_days_left(u)[1]))
      isTRUE(!is.na(left) && left >= 0)
    })
    trial_left <- reactive({
      u <- user_reactive()
      if (is.null(u)) {
        return(NA_real_)
      }
      suppressWarnings(as.numeric(db_trial_days_left(u)[1]))
    })

    # ---- Nav + active styles ----
    mark_active <- function(which) {
      ids <- c("nav_analysis", "nav_account", "nav_membership", "nav_admin", "nav_referral")
      for (i in ids) shinyjs::removeClass(id = ns(i), class = "btn-nav-active")
      if (which %in% ids) shinyjs::addClass(id = ns(which), class = "btn-nav-active")
    }
    observe({
      if (isTRUE(can_analyze())) {
        section("analysis")
        mark_active("nav_analysis")
      } else {
        section("membership")
        mark_active("nav_membership")
      }
    })
    observeEvent(input$nav_analysis, {
      if (!isTRUE(can_analyze())) {
        section("membership")
        mark_active("nav_membership")
        showNotification("Tu prueba finalizó o no tienes membresía activa. Actívala en 'Membresía'.", type = "warning", duration = 6)
      } else {
        section("analysis")
        mark_active("nav_analysis")
      }
    })

    observeEvent(input$nav_account, {
      section("account")
      mark_active("nav_account")
    })
    observeEvent(input$nav_membership, {
      section("membership")
      mark_active("nav_membership")
    })
    observeEvent(input$nav_admin, {
      section("admin")
      mark_active("nav_admin")
    })
    observeEvent(input$nav_referral, {
      section("referral")
      mark_active("nav_referral")
    })

    output$hello_user <- renderUI({
      u <- user_reactive()
      if (is.null(u)) {
        return(NULL)
      }
      span(sprintf("Hola, %s", u$name[[1]]))
    })
    output$admin_btn <- renderUI({
      u <- user_reactive()
      if (is.null(u)) {
        return(NULL)
      }
      is_super <- is_super_admin(u)
      if (!is_super && !db_is_admin(u)) {
        return(NULL)
      }
      actionButton(ns("nav_admin"), "Usuarios", class = "btn btn-primary btn-sm w-100-sm")
    })

    output$section_ui <- renderUI({
      current_section <- section()

      switch(current_section,
        analysis   = institutional_ui(ns("inst")),
        account    = section_account_ui(ns),
        membership = section_membership_ui(ns),
        admin      = section_admin_ui(NS(ns("admin_mod"))),
        referral   = section_referral_ui(ns)
      )
    })


    # ----------------- Referral Logic -----------------
    # ----------------- Referral Logic -----------------
    observe({
      # Populate Referral Link box if code exists
      req(section() == "referral") # Rerun when tab opens
      u <- user_reactive()
      if (!is.null(u)) {
        code <- if (!is.null(u$referral_code)) u$referral_code[[1]] else NA_character_
        if (!is.null(code) && !is.na(code) && nzchar(code)) {
          link <- paste0(PUBLIC_BASE_URL, "?ref=", code)
          updateTextInput(session, "ref_link_box", value = link)
          shinyjs::disable("ref_gen_btn")
        } else {
          updateTextInput(session, "ref_link_box", value = "")
          shinyjs::enable("ref_gen_btn")
        }
      }
    })

    observeEvent(input$ref_gen_btn, {
      u <- user_reactive()
      if (is.null(u)) {
        return()
      }

      # Double check if code already exists
      code <- if (!is.null(u$referral_code)) u$referral_code[[1]] else NA_character_
      if (!is.null(code) && !is.na(code) && nzchar(code)) {
        showNotification("Ya tienes un enlace de referido generado.", type = "warning")
        shinyjs::disable("ref_gen_btn")
        return()
      }

      # Generate
      tryCatch(
        {
          db_ensure_referral_code(u$id[[1]])
          showNotification("¡Enlace generado exitosamente!", type = "message")

          # Refresh user data
          u_new <- db_get_user_by_id(u$id[[1]])
          session$userData$current_user(u_new)

          # UI update handled by the observer above
        },
        error = function(e) {
          showNotification("Error al generar enlace. Inténtalo de nuevo.", type = "error")
        }
      )
    })

    output$wallet_balance <- renderText({
      u <- user_reactive()
      if (is.null(u)) {
        return("$0.00")
      }
      bal <- u$referral_wallet[[1]]
      if (is.null(bal) || is.na(bal)) bal <- 0.0
      sprintf("$%.2f", as.numeric(bal))
    })

    output$referrals_tbl <- renderDT({
      u <- user_reactive()
      if (is.null(u)) {
        return(NULL)
      }
      df <- db_get_referred_users(u$id[[1]])
      if (nrow(df) == 0) {
        return(NULL)
      }
      DT::datatable(df, options = list(pageLength = 5, lengthChange = FALSE, searching = FALSE))
    })

    # (Parche 3) Deshabilitar "Renovar ahora" si no hay customer en este modo
    observe({
      u <- user_reactive()
      if (is.null(u)) {
        return()
      }
      has_customer <- !is.null(u$stripe_customer_id) && nzchar(u$stripe_customer_id[[1]])
      if (has_customer) shinyjs::enable(ns("mb_renew_now")) else shinyjs::disable(ns("mb_renew_now"))
    })

    # ----------------- Rellenar "Tus datos" -----------------
    output$account_data <- renderUI({
      u <- user_reactive()
      if (is.null(u)) {
        return(NULL)
      }
      tags$ul(
        tags$li(strong("Usuario: "), u$username[[1]]),
        tags$li(strong("Nombre: "), u$name[[1]]),
        tags$li(strong("Correo: "), u$email[[1]]),
        tags$li(strong("País: "), u$country[[1]]),
        if (!is.na(u$phone[[1]])) tags$li(strong("Teléfono: "), u$phone[[1]])
      )
    })

    # ----------------- Change Password Logic -----------------
    observeEvent(input$acc_save_pass, {
      u <- user_reactive()
      if (is.null(u)) {
        return()
      }

      old_p <- input$acc_old
      new_p1 <- input$acc_new1
      new_p2 <- input$acc_new2

      # 1. Validaciones básicas
      if (!nzchar(old_p) || !nzchar(new_p1) || !nzchar(new_p2)) {
        showNotification("Completa todos los campos de contraseña.", type = "warning")
        return()
      }
      if (new_p1 != new_p2) {
        showNotification("Las nuevas contraseñas no coinciden.", type = "error")
        return()
      }

      # 2. Nueva == Anterior (Requisito usuario)
      if (new_p1 == old_p) {
        showNotification("La nueva contraseña no puede ser igual a la anterior. Intenta agregar símbolos o números (ej: Puga#2025!).", type = "error", duration = 8)
        return()
      }

      # 3. Validar contraseña actual (usando db_login como helper)
      chk <- db_login(u$username[[1]], old_p)
      if (!isTRUE(chk$ok)) {
        showNotification("La contraseña actual es incorrecta.", type = "error")
        return()
      }

      # 4. Guardar
      tryCatch(
        {
          db_set_password(u$username[[1]], new_p1)

          # Send Email Notification — NUNCA incluir contraseña en texto plano
          f_generic <- get0("auth_send_generic_email", envir = .GlobalEnv, inherits = TRUE)
          if (is.function(f_generic)) {
            subj <- "Contraseña Actualizada - PugaX Trade"
            body <- paste0(
              "Hola ", u$name[[1]], ",\n\n",
              "Tu contraseña ha sido actualizada exitosamente.\n",
              "Si no realizaste este cambio, contacta al soporte inmediatamente.\n\n",
              "Saludos,\nEquipo PugaX"
            )
            tryCatch(f_generic(u$email[[1]], subj, body), error = function(e) message("Error in auth_send_generic_email: ", e$message))
          }

          # Notify Admin of password change
          f_admin_changed <- get0("auth_notify_admin_password_change", envir = .GlobalEnv, inherits = TRUE)
          if (is.function(f_admin_changed)) {
            tryCatch(f_admin_changed(u), error = function(e) message("Error in auth_notify_admin_password_change: ", e$message))
          }

          updateTextInput(session, "acc_old", value = "")
          updateTextInput(session, "acc_new1", value = "")
          updateTextInput(session, "acc_new2", value = "")
          showNotification("Contraseña guardada correctamente. Revisa tu correo.", type = "message")
        },
        error = function(e) {
          showNotification(paste("Error al guardar:", e$message), type = "error")
        }
      )
    })

    # Lógica de otros componentes...


    # ----------------- Membresía / Stripe -----------------
    observeEvent(input$mb_pay, {
      u <- user_reactive()
      if (is.null(u)) {
        showNotification("Inicia sesión primero.", type = "error")
        return()
      }
      if (is_super_admin(u)) {
        showNotification("Tu cuenta es perpetua (admin).", type = "message")
        return()
      }
      shinyjs::disable(ns("mb_pay"))
      on.exit(shinyjs::enable(ns("mb_pay")), add = TRUE)

      f_checkout <- get0("auth_create_checkout_session", envir = .GlobalEnv, inherits = TRUE)
      if (is.function(f_checkout)) {
        res <- try(f_checkout(u), silent = TRUE)
        if (inherits(res, "try-error") || !isTRUE(res$ok)) {
          res <- stripe_checkout_fallback(u)
        }
      } else {
        res <- stripe_checkout_fallback(u)
      }
      if (!isTRUE(res$ok)) {
        showNotification(res$message %||% "No se pudo iniciar el pago.", type = "error", duration = 8)
        return()
      }
      showModal(modalDialog(
        title = "Continuar al pago",
        div(
          class = "text-center",
          p("El enlace seguro de Stripe está listo."),
          tags$a(href = res$url, target = "_blank", class = "btn btn-success btn-lg w-100", "Ir a Pagar en Stripe (Nueva Pestaña)")
        ),
        easyClose = TRUE,
        footer = modalButton("Cerrar")
      ))
    })

    output$membership_state <- renderUI({
      u <- user_reactive()
      if (is.null(u)) {
        return(NULL)
      }
      if (is_super_admin(u)) {
        return(div(class = "text-success", "Membresía perpetua (admin)."))
      }
      if (db_is_admin(u)) {
        return(div(class = "text-success", "Eres administrador."))
      }
      if (db_membership_is_active(u)) {
        msg <- "Membresía activa."
        # Calculate days remaining if expiration exists
        if (!is.null(u$membership_expires_at) && !is.na(u$membership_expires_at[[1]])) {
          exp_date <- tryCatch(as.POSIXct(u$membership_expires_at[[1]]), error = function(e) NA)
          if (!is.na(exp_date)) {
            days <- difftime(exp_date, Sys.time(), units = "days")
            d <- ceiling(as.numeric(days))
            msg <- sprintf("Membresía activa (%d días restantes).", d)
          }
        }
        return(div(class = "text-success", msg))
      }
      left <- db_trial_days_left(u)
      left <- suppressWarnings(as.numeric(left[1]))
      if (!is.na(left)) {
        if (left < 0) {
          return(div(class = "text-danger", "Prueba finalizada."))
        }
        return(div(class = "text-warning", sprintf("Prueba activa: %d días restantes.", as.integer(left))))
      }
      div(class = "text-muted", "Sin membresía.")
    })

    output$renewal_state <- renderUI({
      u <- user_reactive()
      if (is.null(u)) {
        return(NULL)
      }
      if (exists("auth_get_subscription_status")) {
        st <- try(auth_get_subscription_status(u), silent = TRUE)
        if (!inherits(st, "try-error") && !is.null(st)) {
          auto <- isTRUE(st$auto_renew[1])
          if (auto) {
            return(div(class = "text-success", "Renovación automática: ACTIVA"))
          } else {
            return(div(class = "text-danger", "Renovación automática: DESACTIVADA"))
          }
        }
      }
      div(class = "text-muted", "(Sin datos de suscripción)")
    })

    observeEvent(input$mb_cancel_auto, {
      u <- user_reactive()
      if (is.null(u)) {
        return()
      }
      done <- FALSE
      if (exists("auth_cancel_auto_renew")) {
        ok <- try(auth_cancel_auto_renew(u), silent = TRUE)
        done <- isTRUE(ok)
      }
      if (!done) {
        sub_id <- u$stripe_subscription_id[[1]]
        if (nzchar(sub_id)) done <- isTRUE(stripe_set_cancel_at_period_end(sub_id, TRUE))
      }
      if (!done) {
        showNotification("No se pudo cancelar la renovación.", type = "error")
      } else {
        showNotification("Renovación automática cancelada.", type = "message")
        # Recargar datos del usuario para refrescar estado
        u_new <- try(db_get_user_by_id(u$id[[1]]), silent = TRUE)
        if (!inherits(u_new, "try-error") && !is.null(u_new)) {
          session$userData$current_user(u_new)
        }
      }
    })


    # Fin de inicialización modular.


    observeEvent(input$mb_reactivate_auto, {
      u <- user_reactive()
      if (is.null(u)) {
        return()
      }
      done <- FALSE
      if (exists("auth_reactivate_auto_renew")) {
        ok <- try(auth_reactivate_auto_renew(u), silent = TRUE)
        done <- isTRUE(ok)
      }
      if (!done) {
        sub_id <- u$stripe_subscription_id[[1]]
        if (nzchar(sub_id)) done <- isTRUE(stripe_set_cancel_at_period_end(sub_id, FALSE))
      }
      if (!done) {
        showNotification("No se pudo reactivar la renovación.", type = "error")
      } else {
        showNotification("Renovación automática reactivada.", type = "message")
        u_new <- try(db_get_user_by_id(u$id[[1]]), silent = TRUE)
        if (!inherits(u_new, "try-error") && !is.null(u_new)) {
          session$userData$current_user(u_new)
        }
      }
    })

    observeEvent(input$mb_renew_now, {
      u <- user_reactive()
      if (is.null(u)) {
        return()
      }

      # Intento helper (si existe)
      url <- NULL
      if (exists("auth_renew_now")) {
        ok <- try(auth_renew_now(u), silent = TRUE)
        if (is.list(ok) && isTRUE(ok$ok) && nzchar(ok$url)) {
          url <- ok$url
        } else if (inherits(ok, "try-error")) {
          showNotification("Error al invocar helper renew_now.", type = "error", duration = 6)
        }
      }

      # Fallback con validación explícita de customer/mode
      if (is.null(url)) {
        res <- stripe_portal_fallback(u)
        if (isTRUE(res$ok)) {
          url <- res$url
        } else {
          showNotification(res$message, type = "error", duration = 10)
          return()
        }
      }

      showModal(modalDialog(
        title = "Abrir Portal de Facturación",
        div(
          class = "text-center",
          p("El enlace al portal de Stripe está listo."),
          tags$a(href = url, target = "_blank", class = "btn btn-success btn-lg w-100", "Ir al Portal (Nueva Pestaña)")
        ),
        easyClose = TRUE,
        footer = modalButton("Cerrar")
      ))
    })
    admin_server("admin_mod", user_reactive, users_trigger)

    observeEvent(input$logout, {
      tok <- session$userData$session_token()
      # 1. Invalidar el token en DB (la cookie queda inválida aunque persista en el browser)
      if (!is.null(tok) && nzchar(tok)) try(db_delete_token(tok), silent = TRUE)
      session$userData$session_token(NULL)
      session$userData$current_user(NULL)
      # 2. Redirigir a /auth/logout para que el servidor borre la cookie HttpOnly
      session$sendCustomMessage("clearSessionCookie", list())
    })
  })
}

# ----------------- App raíz -----------------
ui <- function(req) {
  # ── Rutas HTTP especiales ────────────────────────────────────────────────────
  if (identical(req$PATH_INFO, "/webhook")) {
    return(handle_stripe_webhook(req))
  }
  if (identical(req$PATH_INFO, "/auth/set-session")) {
    return(.handle_set_session(req))
  }
  if (identical(req$PATH_INFO, "/auth/logout")) {
    return(.handle_logout_cookie(req))
  }

  # ── App normal: leer cookie HttpOnly del header HTTP ────────────────────────
  # El browser envía la cookie automáticamente. La leemos server-side para
  # inyectar el token en el HTML inicial sin que JS pueda leerla (flag HttpOnly).
  cookie_tok <- .parse_cookie_value(req$HTTP_COOKIE %||% "", "puga_auth")

  # HEAD dinámico: si hay cookie válida, inyecta script de auto-login
  dynamic_head <- tags$head(
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1"),
    tags$script(src = "script.js"),
    tags$link(rel = "stylesheet", type = "text/css", href = "styles.css"),
    if (nzchar(cookie_tok)) {
      # Inyectar token en el HTML para que Shiny lo reciba en shiny:connected
      # El token es solo caracteres hex (ya sanitizado arriba)
      tags$script(HTML(sprintf(
        '$(document).on("shiny:connected", function() {
           Shiny.setInputValue("cookie_auth", "%s", {priority: "event"});
         });',
        cookie_tok
      )))
    }
  )

  fluidPage(theme = my_theme, useShinyjs(), dynamic_head, uiOutput("root_ui"))
}

# Verificación de esquema una sola vez al inicio (no por sesión)
tryCatch(db_ensure_schema(), error = function(e) {
  message("Error ensuring schema: ", e$message)
})
tryCatch(db_ensure_login_attempts_table(), error = function(e) {
  message("Error ensuring login_attempts: ", e$message)
})

server <- function(input, output, session) {
  session$userData$current_user <- reactiveVal(NULL)
  session$userData$session_token <- reactiveVal(NULL)


  # Sesión desde cookie
  observeEvent(input$cookie_auth,
    {
      has_tok <- !is.null(input$cookie_auth) && nzchar(input$cookie_auth)
      if (is.null(session$userData$current_user()) && has_tok) {
        u <- db_find_user_by_token(input$cookie_auth)
        if (!is.null(u)) {
          # Lazy check/generate referral code on token lookup
          # [MANUAL] Deshabilitado autogeneración al login. Solo por botón manual.
          # db_ensure_referral_code(u$id[[1]])
          u <- db_get_user_by_id(u$id[[1]]) # refresh
          session$userData$current_user(u)
          session$userData$session_token(input$cookie_auth)
        }
      }
    },
    ignoreInit = FALSE,
    once = TRUE
  )

  # (Parche 2) Retorno de Stripe adaptado para Webhooks
  observe({
    q <- parseQueryString(session$clientData$url_search)
    if (!is.null(q$paid) && q$paid == "1" && !is.null(q$session_id)) {
      if (isTRUE(session$userData[["__stripe_done"]])) {
        return()
      }
      session$userData[["__stripe_done"]] <- TRUE

      u <- session$userData$current_user()
      if (!is.null(u)) {
        # Validamos si el Webhook ya activó la membresía por detrás
        u2 <- try(db_get_user_by_id(u$id[[1]]), silent = TRUE)
        if (!inherits(u2, "try-error") && !is.null(u2)) {
          session$userData$current_user(u2)
          if (isTRUE(u2$membership_active[[1]])) {
            showNotification("¡Pago completado! Tu cuenta está activada.", type = "message", duration = 6)
          } else {
            showNotification("¡Gracias por tu pago! Tu membresía se activará en breve en cuanto recibamos validación de Stripe.", type = "warning", duration = 10)
          }
        }
      }
      updateQueryString("", mode = "replace")
    }
  })

  output$root_ui <- renderUI({
    q <- parseQueryString(session$clientData$url_search)
    if (!is.null(q$reset) && nzchar(q$reset)) {
      return(reset_panel("auth", token = q$reset))
    }
    cu <- session$userData$current_user()
    if (is.null(cu)) login_panel("auth") else main_ui("main")
  })

  set_view <- function(v) {}
  on_success <- function(user_row) {
    # Lazy generate code on login
    try(db_ensure_referral_code(user_row$id[[1]]), silent = TRUE)
    u_refreshed <- try(db_get_user_by_id(user_row$id[[1]]), silent = TRUE)
    if (!inherits(u_refreshed, "try-error") && !is.null(u_refreshed)) user_row <- u_refreshed

    session$userData$current_user(user_row)
    updateQueryString("", mode = "replace")
  }

  auth_module("auth", set_view = set_view, on_success = on_success)
  user_reactive <- reactive({
    session$userData$current_user()
  })
  main_module("main", user_reactive = user_reactive, on_logout = function() {
    session$userData$current_user(NULL)
  })

  # Idle Handling
  observeEvent(input$idle_warning, {
    req(session$userData$current_user())
    showModal(modalDialog(
      title = "Sesión inactiva",
      "Tu sesión está a punto de expirar por inactividad. ¿Deseas continuar?",
      footer = tagList(
        actionButton("mb_keep_alive", "Continuar sesión", class = "btn btn-primary"),
        modalButton("Cerrar")
      ),
      easyClose = FALSE
    ))
  })

  observeEvent(input$mb_keep_alive, {
    removeModal()
    session$sendCustomMessage("reset_idle", TRUE)
  })

  observeEvent(input$idle_timeout, {
    req(session$userData$current_user())
    removeModal()
    # 1. Invalidar token en DB
    tok <- session$userData$session_token()
    if (!is.null(tok) && nzchar(tok)) try(db_delete_token(tok), silent = TRUE)
    session$userData$current_user(NULL)
    session$userData$session_token(NULL)
    # 2. Borrar cookie HttpOnly via endpoint server-side (igual que logout manual)
    session$sendCustomMessage("clearSessionCookie", list())
    showNotification("Sesión cerrada por inactividad.", type = "warning", duration = 10)
  })

  # Limpieza periódica de sesiones y tokens expirados (cada hora, solo en una sesión)
  session$onFlushed(function() {
    tryCatch({
      last_cleanup <- getOption("pugax_last_cleanup", 0)
      if ((as.numeric(Sys.time()) - last_cleanup) > 3600) {
        options(pugax_last_cleanup = as.numeric(Sys.time()))
        try(db_cleanup_expired_sessions(), silent = TRUE)
        try(db_cleanup_expired_reset_tokens(), silent = TRUE)
        try(db_cleanup_login_attempts(days = 1L), silent = TRUE)
      }
    }, error = function(e) NULL)
  }, once = TRUE)

  # Cierre seguro del pool al apagar la app
  onStop(function() {
    if (exists("pool_close", mode = "function")) {
      try(pool_close(), silent = TRUE)
    }
  })
}

shinyApp(ui, server)
