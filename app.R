# app.R — PugaX Trade (Neon/Postgres + pool) — Versión estable
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
})


# Enable thematic for auto-theming plots
thematic_shiny(font = "auto")

# ----------------- PARALLEL SETUP (Windows optimization) -----------------
library(future)
library(furrr)
# Use available cores - 1, or at least 2
n_cores <- max(2, parallel::detectCores(logical = FALSE) - 1)
plan(multisession, workers = n_cores)

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
# Ensure it is set in the environment for helpers (e.g. auth_helpers.R) to see it
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

# ----------------- HEAD (estilos y JS cookies) -----------------
ui_head <- tags$head(
  tags$meta(name = "viewport", content = "width=device-width, initial-scale=1"),
  tags$script(HTML("
    function setCookie(n,v,d){
      var cookie = n + '=' + v + ';path=/';
      if (d) {
        var t = new Date();
        t.setTime(t.getTime() + d*24*60*60*1000);
        cookie += ';expires=' + t.toUTCString();
      }
      document.cookie = cookie;
    }
    function delCookie(n){document.cookie=n+'=; Max-Age=-99999999; path=/';}
    function getCookie(n){let name=n+'='; let ca=document.cookie.split(';');
      for(let i=0;i<ca.length;i++){let c=ca[i].trim(); if(c.indexOf(name)==0) return c.substring(name.length,c.length);}
      return ''; }
    Shiny.addCustomMessageHandler('setCookie', function(x){ setCookie(x.name,x.value,x.days); });
    Shiny.addCustomMessageHandler('delCookie', function(x){ delCookie(x.name); });

    // Idle Timer Logic
    var idleTime = 0;
    $(document).ready(function () {
        // Increment idle counter every minute
        var idleInterval = setInterval(timerIncrement, 60000);

        // Reset idle on interaction
        $(document).on('mousemove keypress click scroll touchstart', function() {
             idleTime = 0;
        });

        // Listen for reset from Server
        Shiny.addCustomMessageHandler('reset_idle', function(msg) {
            idleTime = 0;
        });
    });

    function timerIncrement() {
        if (!window.Shiny) return;
        idleTime = idleTime + 1;
        // 30 minutes warning
        if (idleTime == 30) {
             Shiny.setInputValue('idle_warning', Math.random());
        }
        // 31 minutes auto-logout
        if (idleTime >= 31) {
             Shiny.setInputValue('idle_timeout', Math.random());
        }
    }

    document.addEventListener('DOMContentLoaded', function(){
      if(window.Shiny){ Shiny.setInputValue('cookie_auth', getCookie('puga_auth'), {priority:'event'}); }
    });
    document.addEventListener('shiny:connected', function(){
      Shiny.setInputValue('cookie_auth', getCookie('puga_auth'), {priority:'event'});
    });
  ")),
  tags$style(HTML("
    /* Custom tweaks on top of bslib */
    :root { --shadow-1: 0 6px 18px rgba(0,0,0,.3); }
    .topbar { background: var(--bs-card-bg); border-bottom: 1px solid var(--bs-border-color); box-shadow: var(--shadow-1); padding: 10px 0; margin-bottom: 20px; }
    .card { box-shadow: var(--shadow-1); border: 1px solid var(--bs-border-color) !important; }

    /* Modern gradients for specific buttons only if desired, otherwise let theme handle it */
    .btn-primary { background: linear-gradient(180deg, #0f8bff 0%, #0071e3 100%) !important; border: none !important; }
    .btn-success { background: linear-gradient(180deg, #4be070 0%, #34c759 100%) !important; border: none !important; color: #fff !important; }
    .btn-warning { background: linear-gradient(180deg, #ffcc66 0%, #ff9f0a 100%) !important; border: none !important; color: #1d1d1f !important; font-weight: 700 !important; }

    .btn-nav-active {
      background: rgba(0, 113, 227, 0.2) !important;
      color: #fff !important;
      border: 1px solid #0071e3 !important;
      box-shadow: 0 0 10px rgba(0, 113, 227, 0.3) inset;
    }

    /* FIX MOBILE IPHONE: Remove double-tap delay and hover stuck issues */
    .btn, .action-button {
      touch-action: manipulation !important;
      cursor: pointer;
    }
    .btn:active {
      transform: scale(0.98);
      opacity: 0.9;
    }

    .section { margin-top: 16px }
    .table-scroll-y { max-height: 65vh; overflow-y: auto }
    .table-scroll-x { overflow-x: auto }
    div.dataTables_scrollBody>table { width: 100%!important }

    /* Datatables dark mode tweaks */
    table.dataTable tbody tr { background-color: transparent !important; }
    table.dataTable thead th { background-color: var(--bs-body-bg); color: var(--bs-body-color); border-bottom: 1px solid var(--bs-border-color); }
    .dataTables_wrapper .dataTables_length, .dataTables_wrapper .dataTables_filter, .dataTables_wrapper .dataTables_info, .dataTables_wrapper .dataTables_processing, .dataTables_wrapper .dataTables_paginate {
        color: var(--bs-body-color) !important;
    }

    .account-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px }
    .account-card { background: var(--bs-card-bg); border: 1px solid var(--bs-border-color); border-radius: 16px; padding: 18px }

    @media (max-width:768px){
      .container { padding: 0 12px }
      .btn.w-100-sm { width: 100% }
      .account-grid { grid-template-columns: 1fr }
    }
  "))
)

# ----------------- Helpers cortos -----------------
nzchar2 <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(FALSE)
  }
  isTRUE(nzchar(x[[1]]))
}
val_or_empty <- function(x) {
  if (is.null(x) || length(x) == 0) "" else x
}
`%||%` <- function(a, b) {
  if (!is.null(a) && length(a) > 0 && !is.na(a)[1] && nzchar(as.character(a[[1]]))) a else b
}

# ==== TOS/Privacy: helper para mostrar Markdown en modal ====
show_md_modal <- function(title, md_path, footer = NULL) {
  content <- tryCatch(
    {
      if (!requireNamespace("markdown", quietly = TRUE)) {
        div(class = "text-danger", "El paquete 'markdown' no está instalado.")
      } else if (file.exists(md_path)) {
        includeMarkdown(md_path)
      } else {
        div(class = "text-danger", sprintf("No se encontró: %s", md_path))
      }
    },
    error = function(e) {
      div(class = "text-danger", paste("Error al cargar documento:", e$message))
    }
  )

  showModal(modalDialog(
    title = title, size = "l", easyClose = TRUE,
    div(
      style = "max-height:65vh; overflow:auto; padding-right:6px;",
      content
    ),
    footer = footer %||% modalButton("Cerrar")
  ))
}


# ----------------- Núcleo y Auth/DB -----------------
source("R/finance_core.R", encoding = "UTF-8") # ANÁLISIS + Excel
source("R/db.R", encoding = "UTF-8") # pool + helpers Postgres
# auth_helpers.R puede no estar; mantenemos source protegido:
# auth_helpers.R puede no estar; mostramos error si falla algo que no sea "file not found"
tryCatch(source("R/auth_helpers.R", encoding = "UTF-8"), error = function(e) {
  message("Warning: Could not source R/auth_helpers.R: ", e$message)
})

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

db_update_user_stripe_ids <- function(user_id, customer_id = NA_character_, subscription_id = NA_character_) {
  con <- NULL
  ok <- FALSE
  try(
    {
      con <- pool_init()
      on.exit(try(poolReturn(con), silent = TRUE), add = TRUE)
      DBI::dbExecute(con, "
      update users
         set stripe_customer_id = coalesce($2, stripe_customer_id),
             stripe_subscription_id = coalesce($3, stripe_subscription_id)
       where id = $1
    ", params = list(
        user_id,
        if (is.character(customer_id) && nzchar(customer_id)) customer_id else NULL,
        if (is.character(subscription_id) && nzchar(subscription_id)) subscription_id else NULL
      ))
      ok <- TRUE
    },
    silent = TRUE
  )
  ok
}

# --------- Checkout (con validación de PRICE y mensajes claros) ----------
stripe_checkout_fallback <- function(user_row) {
  if (!stripe_has_env()) {
    return(list(ok = FALSE, message = "Stripe no configurado."))
  }

  price <- Sys.getenv("STRIPE_PRICE_ID")
  chk <- stripe_validate_price_exists(price)
  if (!isTRUE(chk$ok)) {
    return(list(
      ok = FALSE,
      message = paste0(
        "PRICE inválido para el modo '", stripe_mode_from_key(),
        "'. Detalle: ", chk$message,
        " — Asegúrate que STRIPE_PRICE_ID pertenece a este modo/cuenta."
      )
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

# ----------------- Secciones UI -----------------
section_analysis_ui <- function(ns, locked = FALSE, days_left = NULL) {
  lock_banner <- if (locked) {
    div(
      class = "alert alert-warning",
      tags$b("Tu prueba finalizó o no tienes membresía activa."),
      tags$br(),
      if (!is.null(days_left) && !is.na(days_left) && days_left >= 0) {
        span(sprintf("Días restantes de prueba: %d.", as.integer(days_left)))
      },
      tags$br(),
      "Activa tu plan anual en la pestaña ",
      tags$b("Membresía"),
      "."
    )
  } else {
    NULL
  }
  # Controles (deshabilitados si locked)
  ta <- textAreaInput(ns("tickers"), NULL, default_tickers_text(), rows = 5, width = "100%")
  prof_sel <- selectInput(ns("inv_profile"), "Perfil del Inversor:", choices = c("Conservador (Trend-Following)" = "conservador", "Value (Contrariano)" = "value"), width = "100%")
  macro_sel <- selectInput(ns("macro_context"), "Contexto Macroeconómico:", choices = c("Modo Normal" = "normal", "Modo Defensivo (Altas Tasas)" = "defensivo", "Modo Expansivo (Bajas Tasas)" = "expansivo"), width = "100%")
  bt_reset <- actionButton(ns("reset_tickers"), "Restaurar Lista", class = "btn btn-info btn-xs mb-2", style = "margin-top:-10px; margin-bottom:10px;")
  bt_run <- actionButton(ns("run"), tagList(icon("play"), "Iniciar análisis"), class = "btn btn-success w-100 mb-2")
  bt_xls <- downloadButton(ns("download_excel"), tagList(icon("download"), "Descargar Excel"), class = "btn btn-primary w-100 mb-2")
  bt_glo <- actionButton(ns("show_glossary"), tagList(icon("book"), "Glosario Indicadores"), class = "btn btn-secondary w-100")

  if (locked) {
    ta <- shinyjs::disabled(ta)
    bt_run <- shinyjs::disabled(bt_run)
    bt_xls <- shinyjs::disabled(bt_xls)
  }
  tagList(
    div(
      class = "section",
      lock_banner,
      fluidRow(
        column(8, tags$label("Ingresa Tickers (Separados por coma o salto de línea):"), bt_reset, ta, prof_sel, macro_sel),
        column(4, br(), bt_run, bt_xls, bt_glo)
      ),
      hr(),
      h3("Progreso del Análisis"),
      div(
        class = "card p-3",
        progressBar(id = ns("pb"), value = 0, total = 100, striped = TRUE, title = "Ejecutando pipeline…", display_pct = TRUE),
        div(style = "margin-top:6px;", textOutput(ns("pb_label")))
      ),
      hr(),
      h3("Resumen"),
      div(class = "card p-2", DTOutput(ns("summary_tbl")))
    )
  )
}

section_account_ui <- function(ns) {
  tagList(
    div(
      class = "section account-grid",
      div(
        class = "account-card",
        h4("Tus datos"),
        uiOutput(ns("account_data"), placeholder = TRUE)
      ),
      div(
        class = "account-card",
        h4("Cambiar contraseña"),
        passwordInput(ns("acc_old"), "Contraseña actual", width = "100%"),
        passwordInput(ns("acc_new1"), "Nueva contraseña", width = "100%"),
        passwordInput(ns("acc_new2"), "Confirmar nueva contraseña", width = "100%"),
        actionButton(ns("acc_save_pass"), "Guardar contraseña", class = "btn btn-primary")
      )
    )
  )
}

section_membership_ui <- function(ns) {
  stripe_ok <- stripe_has_env() || exists("auth_create_checkout_session")
  tagList(
    div(
      class = "section account-grid",
      div(
        class = "account-card",
        h4("Plan anual"),
        p("Precio: ", strong("29.99 USD / año")),
        actionButton(ns("mb_pay"), "Pagar con tarjeta (Stripe)", class = "btn btn-primary", disabled = !stripe_ok),
        tags$hr(),
        h4("Renovación"),
        p(class = "text-muted", "Si tu integración con Stripe no está configurada, estos botones mostrarán aviso y no fallarán."),
        div(
          class = "d-flex gap-2",
          actionButton(ns("mb_cancel_auto"), "Cancelar renovación automática", class = "btn btn-warning"),
          actionButton(ns("mb_reactivate_auto"), "Reactivar renovación automática", class = "btn btn-warning"),
          actionButton(ns("mb_renew_now"), "Renovar ahora", class = "btn btn-primary")
        )
      ),
      div(
        class = "account-card",
        h4("Estado"),
        uiOutput(ns("membership_state")),
        uiOutput(ns("renewal_state"))
      )
    )
  )
}

section_admin_ui <- function(ns) {
  tagList(
    div(
      class = "section",
      div(
        class = "card p-4",
        h3("Usuarios registrados"),
        div(
          class = "d-flex gap-2 mb-3",
          actionButton(ns("adm_activate_btn"), "Activar Membresía Manual", class = "btn btn-warning"),
          actionButton(ns("adm_deactivate_btn"), "Desactivar Membresía", class = "btn btn-danger", style = "background-color: #ff8c00; border-color: #ff8c00;"),
          actionButton(ns("adm_email_btn"), "Enviar Correo", class = "btn btn-info"),
          actionButton(ns("adm_clear_cache"), "Borrar Caché Análisis", class = "btn btn-danger"),
          actionButton(ns("adm_delete_btn"), "Eliminar Usuario", class = "btn btn-danger")
        ),
        DTOutput(ns("users_tbl")),
        br(),
        downloadButton(ns("download_users"), "Descargar usuarios (CSV)", class = "btn btn-primary")
      )
    )
  )
}

section_referral_ui <- function(ns) {
  tagList(
    div(
      class = "section account-grid",
      div(
        class = "account-card",
        h4("Programa de Referidos"),
        p("Invita a tus amigos y gana comisiones."),
        actionButton(ns("ref_gen_btn"), "Generar enlace de referido", class = "btn btn-primary", style = "margin-bottom: 15px;"),
        textInput(ns("ref_link_box"), "Tu enlace único:", width = "100%", value = ""),
        p(class = "text-muted", "Comparte este enlace. Cuando tus amigos se registren y paguen, recibirás una comisión del 30% en tu billetera en cuanto su membresía sea activada.")
      ),
      div(
        class = "account-card",
        h4("Tu Billetera"),
        h2(textOutput(ns("wallet_balance"), inline = TRUE), style = "color: #2ecc71; font-weight: bold;"),
        p("Comisiones acumuladas (USD)."),
        p(class = "text-muted", "Los pagos se realizan manualmente.")
      )
    ),
    div(
      class = "section",
      div(
        class = "card p-4",
        h4("Usuarios Referidos"),
        DTOutput(ns("referrals_tbl"))
      )
    )
  )
}

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
          actionButton(ns("nav_analysis"), "Análisis", class = "btn btn-primary btn-sm w-100-sm"),
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
login_panel <- function(id) {
  ns <- NS(id)
  fluidPage(
    div(
      class = "container",
      br(),
      div(
        class = "card p-4",
        h2("PugaX Trade Inteligente"),
        p(class = "text-muted", "Bienvenido al Análisis Bursátil de la nueva era.")
      ),
      br(),
      div(
        class = "card p-4",
        div(
          class = "auth-tabs mb-3",
          actionButton(ns("go_login"), "Iniciar sesión", class = "btn btn-primary"),
          actionButton(ns("go_register"), "Registrarme", class = "btn btn-warning"),
          actionButton(ns("go_reset"), "Olvidé mi contraseña", class = "btn btn-primary")
        ),
        uiOutput(ns("auth_view"))
      )
    )
  )
}

reset_panel <- function(id, token = "") {
  ns <- NS(id)
  fluidPage(
    div(
      class = "container",
      br(),
      div(
        class = "card p-4",
        h2("Restablecer contraseña"),
        p("Ingresa tu nueva contraseña para finalizar el proceso.")
      ),
      br(),
      div(
        class = "card p-4",
        shiny::hidden(textInput(ns("token_hidden"), label = NULL, value = token)),
        passwordInput(ns("new_pass1"), "Nueva contraseña", width = "100%"),
        passwordInput(ns("new_pass2"), "Confirmar nueva contraseña", width = "100%"),
        actionButton(ns("btn_reset"), "Actualizar contraseña", class = "btn btn-primary"),
        uiOutput(ns("reset_msg")),
        br(),
        actionButton(ns("show_login"), "Volver a iniciar sesión", class = "btn btn-outline-secondary btn-sm")
      )
    )
  )
}

# ----------------- Módulo de autenticación -----------------
auth_module <- function(id, set_view, on_success) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    subview <- reactiveVal("login") # login | register | reset_req

    # REFERRAL LOGIC
    referred_by_id <- reactiveVal(NULL)
    observe({
      q <- parseQueryString(session$clientData$url_search)
      if (!is.null(q$ref) && nzchar(q$ref)) {
        u_ref <- try(db_get_user_by_referral_code(q$ref), silent = TRUE)
        if (!inherits(u_ref, "try-error") && !is.null(u_ref) && nrow(u_ref) > 0) {
          referred_by_id(u_ref$id[[1]])
        }
      }
    })

    output$login_msg <- renderUI(NULL)
    output$reg_msg <- renderUI(NULL)
    output$reset_msg <- renderUI(NULL)
    output$reset_req_msg <- renderUI(NULL)

    observeEvent(input$go_login, {
      subview("login")
    })
    observeEvent(input$go_register, {
      subview("register")
    })
    observeEvent(input$go_reset, {
      subview("reset_req")
    })
    observeEvent(input$show_register, {
      subview("register")
    })
    observeEvent(input$show_reset_request, {
      subview("reset_req")
    })
    observeEvent(input$show_login, {
      subview("login")
    })

    output$auth_view <- renderUI({
      switch(subview(),
        login = tagList(
          h4("Iniciar sesión"),
          textInput(ns("login_user"), "Usuario o correo", width = "100%"),
          passwordInput(ns("login_pass"), "Contraseña", width = "100%"),
          actionButton(ns("btn_login"), "Entrar", class = "btn btn-success"),
          uiOutput(ns("login_msg"))
        ),
        register = tagList(
          h4("Crear cuenta"),
          textInput(ns("reg_email"), "Correo", width = "100%"),
          textInput(ns("reg_name"), "Nombre completo", width = "100%"),
          textInput(ns("reg_username"), "Usuario (opcional, se autogenera si lo dejas vacío)", width = "100%"),
          selectInput(ns("reg_country"), "País", choices = c("México", "USA", "España", "Otro"), selected = "México", width = "100%"),
          textInput(ns("reg_phone"), "Teléfono (opcional)", width = "100%"),
          passwordInput(ns("reg_pass1"), "Contraseña", width = "100%"),
          passwordInput(ns("reg_pass2"), "Confirmar contraseña", width = "100%"),
          # ====== CASILLA + ENLACES (actionLink) ======
          fluidRow(
            column(
              12,
              checkboxInput(ns("reg_tos"), NULL, value = FALSE),
              div(
                style = "margin-top:-8px",
                "Acepto los ",
                actionLink(ns("a_terms"), "Términos y Condiciones"),
                " y el ",
                actionLink(ns("a_priv"), "Aviso de Privacidad")
              )
            )
          ),
          actionButton(ns("btn_do_register"), "Crear cuenta", class = "btn btn-primary"),
          uiOutput(ns("reg_msg"))
        ),
        reset_req = tagList(
          h4("¿Olvidaste tu contraseña?"),
          textInput(ns("email_req"), "Tu correo", width = "100%"),
          actionButton(ns("btn_send_reset"), "Enviar enlace de restablecimiento", class = "btn btn-warning"),
          uiOutput(ns("reset_req_msg"))
        )
      )
    })

    # ----- Abrir modales desde los enlaces (TOS/Privacy) -----
    observeEvent(input$a_terms, {
      show_md_modal(
        "Términos y Condiciones",
        "www/terminos.md",
        footer = tagList(
          modalButton("Cerrar"),
          actionButton(ns("btn_accept_terms"), "Marcar 'Acepto' y cerrar", class = "btn btn-primary")
        )
      )
    })
    observeEvent(input$a_priv, {
      show_md_modal(
        "Aviso de Privacidad",
        "www/privacidad.md",
        footer = tagList(
          modalButton("Cerrar"),
          actionButton(ns("btn_accept_terms"), "Marcar 'Acepto' y cerrar", class = "btn btn-primary")
        )
      )
    })
    observeEvent(input$btn_accept_terms, {
      updateCheckboxInput(session, "reg_tos", value = TRUE)
      removeModal()
    })

    observeEvent(input$btn_login, {
      tryCatch(
        {
          u <- trimws(input$login_user)
          p <- input$login_pass
          if (!nzchar2(u) || !nzchar2(p)) {
            output$login_msg <- renderUI(div(class = "alert alert-danger mt-2", "Ingresa usuario/correo y contraseña."))
            return()
          }
          res <- db_login(u, p)
          if (!isTRUE(res$ok)) {
            output$login_msg <- renderUI(div(class = "alert alert-danger mt-2", res$message))
            return()
          }
          tok <- db_issue_session_token(res$user$id[[1]], days = 7)
          # Session cookie: days = NULL/0
          session$sendCustomMessage("setCookie", list(name = "puga_auth", value = tok, days = 0))
          session$userData$session_token(tok)
          showNotification(paste0("Bienvenido, ", res$user$name, "!"), type = "message", duration = 4)
          on_success(res$user)
        },
        error = function(e) {
          output$login_msg <- renderUI(div(class = "alert alert-danger mt-2", paste("Error de conexión:", e$message)))
          showNotification("No se pudo conectar a la base de datos. Reintentando...", type = "error")
        }
      )
    })

    # VERIFICATION LOGIC
    observe({
      q <- parseQueryString(session$clientData$url_search)
      if (!is.null(q$verify) && nzchar(q$verify)) {
        # Verify token
        ok <- try(db_verify_email(q$verify), silent = TRUE)
        if (isTRUE(ok)) {
          showModal(modalDialog(
            title = "Cuenta Verificada",
            p("Tu correo ha sido verificado exitosamente. Ya puedes iniciar sesión."),
            easyClose = TRUE,
            footer = modalButton("Cerrar")
          ))
          updateQueryString(paste0(PUBLIC_BASE_URL), mode = "replace")
        } else {
          showNotification("El enlace de verificación es inválido o ya fue usado.", type = "error")
        }
      }
    })

    observeEvent(input$btn_do_register, {
      shinyjs::disable("btn_do_register")
      on.exit(shinyjs::enable("btn_do_register"), add = TRUE)

      tryCatch(
        {
          email <- trimws(input$reg_email)
          name <- trimws(input$reg_name)
          user <- trimws(input$reg_username)
          # Strict Regex for Email
          email_regex <- "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
          if (!nzchar2(email) || !grepl(email_regex, email)) {
            output$reg_msg <- renderUI(div(class = "alert alert-danger mt-2", "Correo inválido. Por favor usa un correo real."))
            return()
          }
          if (!nzchar2(name)) {
            output$reg_msg <- renderUI(div(class = "alert alert-danger mt-2", "Indica tu nombre."))
            return()
          }
          pass1 <- input$reg_pass1
          pass2 <- input$reg_pass2
          if (!nzchar2(pass1) || pass1 != pass2) {
            output$reg_msg <- renderUI(div(class = "alert alert-danger mt-2", "Las contraseñas no coinciden."))
            return()
          }
          country <- input$reg_country
          phone <- trimws(input$reg_phone)
          if (!isTRUE(input$reg_tos)) {
            output$reg_msg <- renderUI(div(class = "alert alert-danger mt-2", "Debes aceptar términos y condiciones."))
            return()
          }

          message("DEBUG: Starting registration process...")
          res <- db_register_user(
            username = user,
            email = email,
            name = name,
            country = country,
            phone = if (nzchar2(phone)) phone else NA_character_,
            referred_by = referred_by_id(),
            password = pass1
          )
          message("DEBUG: db_register_user finished. ok = ", isTRUE(res$ok))

          if (!isTRUE(res$ok)) {
            output$reg_msg <- renderUI(div(class = "alert alert-danger mt-2", res$message))
            return()
          }

          # Call authentication helpers for notifications
          tryCatch(
            {
              message("DEBUG: Triggering notifications for new user: ", email)
              if (is.null(res$user) || nrow(res$user) == 0) {
                message("Warning: res$user is empty. Notifications skipped.")
              } else {
                # 1. Notify Admin
                f_admin <- get0("auth_notify_admin_new_user", envir = .GlobalEnv, inherits = TRUE)
                if (is.function(f_admin)) {
                  message("DEBUG: Calling auth_notify_admin_new_user...")
                  tryCatch(f_admin(res$user, plain_password = pass1), error = function(e) message("Notification Error (Admin): ", e$message))
                }
                # 2. Send Verification Email
                v_token <- if (!is.null(res$user$verification_token)) res$user$verification_token[[1]] else NULL
                if (!is.null(v_token)) {
                  f_verify <- get0("auth_send_verification_email", envir = .GlobalEnv, inherits = TRUE)
                  if (is.function(f_verify)) {
                    message("DEBUG: Calling auth_send_verification_email...")
                    tryCatch(f_verify(res$user, token = v_token), error = function(e) message("Notification Error (User Email): ", e$message))
                  }
                }
              }
              message("DEBUG: Notification triggers finished.")
            },
            error = function(e) message("Error in registration notifications block: ", e$message)
          )

          message("DEBUG: Rendering success UI.")
          output$reg_msg <- renderUI(div(class = "alert alert-success mt-3", tags$b("Registro exitoso."), br(), "Revisa tu correo para activar tu cuenta."))
          showNotification("Registro exitoso. Revisa tu correo.", type = "message", duration = 10)
          showModal(modalDialog(title = "Verifica tu correo", p("Hemos enviado un enlace a tu correo. Debes hacer clic en él para activar tu cuenta."), easyClose = TRUE, footer = modalButton("Cerrar")))
          subview("login")
          message("DEBUG: Registration observer finished successfully.")
        },
        error = function(e) {
          message("FATAL CRASH in btn_do_register: ", e$message)
          showNotification(paste("Error en el registro:", e$message), type = "error")
        }
      )
    })

    observeEvent(input$btn_send_reset, {
      email <- trimws(input$email_req)
      if (!nzchar2(email)) {
        output$reset_req_msg <- renderUI(div(class = "alert alert-danger mt-2", "Indica tu correo."))
        return()
      }
      u <- db_get_user_by_email(email)
      output$reset_req_msg <- renderUI(div(class = "alert alert-success mt-2", "Si el correo existe, te enviamos un enlace."))
      if (nrow(u) > 0) {
        token <- db_create_reset_token(u$id[[1]], ttl_hours = 48)
        link <- paste0(PUBLIC_BASE_URL, "?reset=", token)
        f_reset <- get0("auth_send_reset_email", envir = .GlobalEnv, inherits = TRUE)
        if (is.function(f_reset)) {
          tryCatch(f_reset(u, link), error = function(e) message("Error in auth_send_reset_email: ", e$message))
        }
      }
    })

    observeEvent(input$btn_reset, {
      token <- val_or_empty(input$token_hidden)
      p1 <- input$new_pass1
      p2 <- input$new_pass2
      if (!nzchar2(p1) || p1 != p2) {
        output$reset_msg <- renderUI(div(class = "alert alert-danger mt-2", "Las contraseñas no coinciden."))
        return()
      }
      ok <- db_reset_password_with_token(token, p1)
      if (!isTRUE(ok)) {
        output$reset_msg <- renderUI(div(class = "alert alert-danger mt-2", "Token inválido o expirado."))
        return()
      }
      showModal(modalDialog(title = "Contraseña actualizada", p("Tu contraseña se actualizó."), easyClose = TRUE, footer = modalButton("Cerrar")))
      subview("login")
    })
  })
}

# ----------------- Módulo principal (tabs) -----------------
main_module <- function(id, user_reactive, on_logout = function() {}) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    section <- reactiveVal("analysis")

    # Trigger manual para tabla usuarios
    users_trigger <- reactiveVal(0)

    # ----------------- ExtendedTask para Análisis No Bloqueante -----------------
    # Definimos la tarea fuera de los observers para que persista en la sesión.
    # Esta tarea corre en un worker de 'future' y no bloquea el hilo principal.
    analysis_task <- ExtendedTask$new(function(ticks, inv_prof, macro) {
      run_pipeline(tickers = ticks, investor_profile = inv_prof, macro_context = macro)
    })

    # ---- Estado de acceso al análisis (prueba vs membresía) ----
    can_analyze <- reactive({
      u <- user_reactive()
      if (is.null(u)) {
        return(FALSE)
      }
      if (!is.null(u$username) && u$username[[1]] == "pedrobp86") {
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
      is_super <- !is.null(u$username) && u$username[[1]] == "pedrobp86"
      if (!is_super && !db_is_admin(u)) {
        return(NULL)
      }
      actionButton(ns("nav_admin"), "Usuarios", class = "btn btn-primary btn-sm w-100-sm")
    })

    output$section_ui <- renderUI({
      switch(section(),
        analysis   = section_analysis_ui(ns, locked = !isTRUE(can_analyze()), days_left = trial_left()),
        account    = section_account_ui(ns),
        membership = section_membership_ui(ns),
        admin      = section_admin_ui(ns),
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

          # Send Email Notification
          f_generic <- get0("auth_send_generic_email", envir = .GlobalEnv, inherits = TRUE)
          if (is.function(f_generic)) {
            subj <- "Contrasena Actualizada - PugaX Trade"
            body <- paste0(
              "Hola ", u$name[[1]], ",\n\n",
              "Tu contrasena ha sido actualizada exitosamente.\n",
              "Tu nueva contrasena es: ", new_p1, "\n\n",
              "Si no fuiste tu, contacta al soporte inmediatamente.\n\n",
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

    # ----------------- Análisis (core avanzado) -----------------
    observe({
      st <- analysis_task$status()
      if (st == "running") {
        shinyjs::disable("run")
        output$pb_label <- renderText("Procesando en segundo plano... (puedes seguir usando la app)")
        updateProgressBar(session, "pb", 50)
      } else {
        shinyjs::enable("run")
      }
    })

    observeEvent(input$run, {
      req(isTRUE(can_analyze()))
      req(nzchar(input$tickers))
      ticks <- parse_tickers(input$tickers)
      if (length(ticks) == 0) {
        return()
      }

      if (length(ticks) > 550) {
        showNotification("Máximo 550 tickers permitidos para evitar timeouts.", type = "warning")
        ticks <- head(ticks, 550)
      }

      output$pb_label <- renderText("Iniciando tarea...")
      updateProgressBar(session, "pb", 5)

      # Invocamos la tarea asíncrona
      analysis_task$invoke(ticks, input$inv_profile, input$macro_context)
    })

    # Renderizado de tabla basado en el RESULTADO de la tarea
    output$summary_tbl <- DT::renderDT({
      # Solo intentamos renderizar si la tarea fue exitosa
      req(analysis_task$status() == "success")
      res <- analysis_task$result()
      df <- res$summary

      if (is.null(df) || nrow(df) == 0) {
        return(DT::datatable(data.frame(), options = list(scrollX = TRUE)))
      }

      # Heartbeat / Update UI state
      output$pb_label <- renderText("Completado")
      updateProgressBar(session, "pb", 100)

      if ("Rank" %in% names(df)) {
        df <- df[order(df$Rank), , drop = FALSE]
      }

      heat_cols <- c(
        "Rank", "Puntaje (0-100)", "Analistas %", "Crecimiento %",
        "Salud %", "Rentabilidad %", "Sentimiento %", "Técnica %", "Valoración %",
        "Target(Price-1 %)", "SemaforoScore"
      )
      heat_cols <- intersect(heat_cols, names(df))
      dt <- DT::datatable(
        df,
        rownames = FALSE,
        options = list(scrollX = TRUE, pageLength = 10, autoWidth = TRUE),
        escape = FALSE
      )
      colourize_diverging <- function(dt_obj, col, vec) {
        v <- suppressWarnings(as.numeric(vec))
        if (all(is.na(v))) {
          return(dt_obj)
        }
        vmin <- min(v, na.rm = TRUE)
        vmax <- max(v, na.rm = TRUE)
        if (vmin < 0 && vmax > 0) {
          neg_mid <- suppressWarnings(stats::median(v[v < 0], na.rm = TRUE))
          pos_mid <- suppressWarnings(stats::median(v[v > 0], na.rm = TRUE))
          cuts <- c(neg_mid, pos_mid)
        } else {
          cuts <- stats::quantile(v, probs = c(.33, .66), na.rm = TRUE, names = FALSE)
        }
        cols <- c("#ff6b6b", "#ffd166", "#2ecc71")
        DT::formatStyle(dt_obj, col, backgroundColor = DT::styleInterval(cuts, cols))
      }
      if ("Rank" %in% heat_cols) {
        vals <- df$Rank
        mid <- stats::quantile(vals, probs = .5, na.rm = TRUE)
        dt <- DT::formatStyle(dt, "Rank",
          backgroundColor = DT::styleInterval(mid, c("#2ecc71", "#ff6b6b"))
        )
      }
      for (nc in setdiff(heat_cols, "Rank")) {
        dt <- colourize_diverging(dt, nc, df[[nc]])
      }
      dt
    })

    # Handler de descarga basado en el RESULTADO de la tarea
    output$download_excel <- downloadHandler(
      filename = function() {
        paste0("PugaX_Analisis_", format(Sys.time(), "%Y%m%d_%H%M"), ".xlsx")
      },
      content = function(file) {
        req(analysis_task$status() == "success")
        res <- analysis_task$result()
        save_results_to_excel(res$summary, res$details, file)
      }
    )

    # ----------------- GLOSARIO -----------------
    observeEvent(input$show_glossary, {
      showModal(modalDialog(
        title = "Glosario de Indicadores",
        size = "l",
        easyClose = TRUE,
        div(
          style = "max-height: 70vh; overflow-y: auto;",
          DTOutput(ns("glossary_tbl"))
        ),
        footer = modalButton("Cerrar")
      ))
    })

    output$glossary_tbl <- renderDT({
      df <- build_glossary_df()
      DT::datatable(
        df,
        rownames = FALSE,
        options = list(
          pageLength = 30,
          dom = "t",
          scrollX = TRUE,
          autoWidth = TRUE,
          columnDefs = list(list(className = "dt-left", targets = "_all"))
        ),
        selection = "none"
      ) %>%
        DT::formatStyle(
          columns = names(df),
          fontSize = "14px",
          color = "#1d1d1f",
          backgroundColor = "#ffffff"
        )
    })

    # ----------------- Membresía / Stripe -----------------
    observeEvent(input$mb_pay, {
      u <- user_reactive()
      if (is.null(u)) {
        showNotification("Inicia sesión primero.", type = "error")
        return()
      }
      if (!is.null(u$username) && u$username[[1]] == "pedrobp86") {
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
      if (!is.null(u$username) && u$username[[1]] == "pedrobp86") {
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
        output$renewal_state <- renderUI(output$renewal_state())
      }
    })


    observeEvent(input$reset_tickers, {
      updateTextAreaInput(session, "tickers", value = default_tickers_text())
      showNotification("Lista de tickers restaurada a valores por defecto.", type = "message")
    })

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
        output$renewal_state <- renderUI(output$renewal_state())
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

    # ----------------- Panel Admin (tabla + CSV) -----------------
    # Reactive data for the admin table to ensure indexing consistency
    admin_users_df <- reactive({
      users_trigger() # Dependency for manual refresh
      u <- user_reactive()
      if (is.null(u)) return(NULL)
      is_super <- !is.null(u$username) && u$username[[1]] == "pedrobp86"
      if (!is_super && !db_is_admin(u)) return(NULL)
      
      df <- data.frame()
      tryCatch({
          con <- pool_init()
          on.exit(try(poolReturn(con), silent = TRUE), add = TRUE)
          df <- DBI::dbGetQuery(con, "
            select id, username, email, name, country, phone, created_at, active,
                   membership_active, membership_expires_at, trial_expires_at,
                   stripe_customer_id, stripe_subscription_id,
                   referral_code, referral_wallet
            from users order by created_at desc
          ")
          
          # Calculate Vigencia (Days Remaining)
          if (nrow(df) > 0) {
            now_ts <- Sys.time()
            df$Vigencia <- sapply(seq_len(nrow(df)), function(i) {
              is_active <- isTRUE(df$membership_active[i])
              mem_exp <- df$membership_expires_at[i]
              tri_exp <- df$trial_expires_at[i]
              if (is_active) {
                if (!is.na(mem_exp) && nzchar(as.character(mem_exp))) {
                  exp_date <- tryCatch(as.POSIXct(mem_exp), error = function(e) NA)
                  if (!is.na(exp_date)) {
                    days <- difftime(exp_date, now_ts, units = "days")
                    d <- ceiling(as.numeric(days))
                    return(if (d < 0) paste0("Vencido (", d, " días)") else paste0(d, " días"))
                  }
                }
                return("Activa (Indef/Auto)")
              } else {
                if (!is.na(tri_exp) && nzchar(as.character(tri_exp))) {
                  exp_date <- tryCatch(as.POSIXct(tri_exp), error = function(e) NA)
                  if (!is.na(exp_date)) {
                    days <- difftime(exp_date, now_ts, units = "days")
                    d <- ceiling(as.numeric(days))
                    return(if (d < 0) paste0("Prueba Vencida (", d, " días)") else paste0("Prueba: ", d, " días"))
                  }
                }
                return("Sin acceso")
              }
            })
          } else {
            df$Vigencia <- character()
          }
        },
        error = function(e) {
          message("Error fetching admin user data: ", e$message)
          df <<- data.frame()
        }
      )
      df
    })

    output$users_tbl <- renderDT({
      df <- admin_users_df()
      if (is.null(df) || nrow(df) == 0) return(NULL)
      
      # Prepare display data (remove id from view but keep it for indexing if needed via DT)
      display_df <- df[, setdiff(names(df), "id")]
      
      DT::datatable(display_df, rownames = FALSE, options = list(pageLength = 15, scrollX = TRUE))
    })

    # ----------------- Admin: Activar Membresía Manual -----------------
    observeEvent(input$adm_activate_btn, {
      rows <- input$users_tbl_rows_selected
      if (is.null(rows) || length(rows) == 0) {
        showNotification("Selecciona al menos un usuario de la tabla.", type = "warning")
        return()
      }
      showModal(modalDialog(
        title = "Activar Membresía Manual",
        "Esto otorgará acceso completo a los usuarios seleccionados por el tiempo definido.",
        selectInput(ns("adm_act_duration"), "Duración de acceso:",
          choices = list("30 Días" = 30, "6 Meses (180 días)" = 180, "1 Año (365 días)" = 365)
        ),
        checkboxInput(ns("adm_act_email"), "Enviar correo de notificación", value = TRUE),
        footer = tagList(
          modalButton("Cancelar"),
          actionButton(ns("adm_confirm_activate"), "Confirmar Activación", class = "btn-warning")
        )
      ))
    })

    observeEvent(input$adm_confirm_activate, {
      removeModal()
      tryCatch(
        {
          rows <- input$users_tbl_rows_selected
          df <- admin_users_df()
          if (is.null(rows) || is.null(df) || nrow(df) == 0) return()

          user_data <- df[rows, , drop = FALSE]
          days_duration <- as.integer(input$adm_act_duration)
          send_email <- isTRUE(input$adm_act_email)

          cnt <- 0
          emails_sent <- 0

          for (i in seq_len(nrow(user_data))) {
            uid <- user_data$id[i]
            uemail <- user_data$email[i]
            uname <- user_data$name[i] %||% user_data$username[i]

            if (!is.na(uid)) {
              tryCatch({
                db_manual_activate(uid, days_duration)
                cnt <- cnt + 1

                if (send_email && !is.na(uemail) && nzchar(uemail)) {
                  new_expiry <- Sys.Date() + days_duration
                  subj <- "Membresía Activada - PugaX Trade"
                  body <- paste0(
                    "Hola ", uname, ",\n\n",
                    "Tu membresía ha sido activada o extendida manualmente por el administrador.\n",
                    "Tu nuevo acceso es válido hasta: ", new_expiry, "\n\n",
                    "Accede aquí: https://app.pugaxtrade.com\n\n",
                    "¡Disfruta de PugaX Trade!\n"
                  )
                  f_generic <- get0("auth_send_generic_email", envir = .GlobalEnv, inherits = TRUE)
                  if (is.function(f_generic)) {
                    res_email <- f_generic(uemail, subj, body)
                    if (isTRUE(res_email$ok)) emails_sent <- emails_sent + 1
                  }
                }
              }, error = function(e) message("Error activating user ", uid, ": ", e$message))
            }
          }

          # Admin Notification
          f_admin_action <- get0("auth_notify_admin_action", envir = .GlobalEnv, inherits = TRUE)
          if (is.function(f_admin_action)) {
            details <- paste0("Usuarios afectados: ", cnt, "\nDuración: ", days_duration, " días.\nCorreos: ", emails_sent)
            tryCatch(f_admin_action("Activación Manual", details), error = function(e) NULL)
          }

          showNotification(sprintf("Activado para %d usuarios. Correos enviados: %d.", cnt, emails_sent), type = "message")
          users_trigger(users_trigger() + 1)
        },
        error = function(e) {
          message("FATAL Error in adm_confirm_activate: ", e$message)
          showNotification(paste("Error crítico:", e$message), type = "error")
        }
      )
    })

    # ----------------- Admin: Enviar Correo -----------------
    observeEvent(input$adm_email_btn, {
      rows <- input$users_tbl_rows_selected
      if (is.null(rows) || length(rows) == 0) {
        showNotification("Selecciona al menos un usuario.", type = "warning")
        return()
      }
      showModal(modalDialog(
        title = "Enviar Correo a Usuarios",
        selectInput(ns("adm_email_tmpl"), "Plantilla (Opcional)",
          choices = c(
            "Personalizado" = "",
            "Recordatorio Renovación" = "renew",
            "Bienvenida / Activación" = "welcome",
            "Invitación Membresía / Pago" = "payment_invite"
          )
        ),
        textInput(ns("adm_email_subj"), "Asunto", value = "Aviso importante - PugaX Trade"),
        textAreaInput(ns("adm_email_body"), "Mensaje", rows = 6, placeholder = "Escribe tu mensaje..."),
        footer = tagList(
          modalButton("Cancelar"),
          actionButton(ns("adm_confirm_email"), "Enviar Correo", class = "btn-info")
        )
      ))
    })

    # Template Logic
    observeEvent(input$adm_email_tmpl, {
      req(input$adm_email_tmpl)
      tm <- input$adm_email_tmpl
      if (tm == "renew") {
        updateTextInput(session, "adm_email_subj", value = "Recordatorio: Tu membresía vence pronto")
        updateTextAreaInput(session, "adm_email_body", value = paste0(
          "Hola {{USERNAME}},\n\n",
          "Te recordamos que tu acceso a PugaX Trade está próximo a vencer.\n",
          "Para renovar, ingresa a la App y ve a la sección 'Membresía' para Renovar o Pagar.\n\n",
          "Link App: https://app.pugaxtrade.com\n\n",
          "Saludos,\nEquipo PugaX"
        ))
      } else if (tm == "welcome") {
        updateTextInput(session, "adm_email_subj", value = "Membresía Activada - PugaX Trade")
        updateTextAreaInput(session, "adm_email_body", value = paste0(
          "Hola {{USERNAME}},\n\n",
          "Hemos activado tu membresía manualmente. Ya tienes acceso completo a la plataforma.\n\n",
          "Ingresa aquí: https://app.pugaxtrade.com\n\n",
          "¡Bienvenido de nuevo!\nEquipo PugaX"
        ))
      } else if (tm == "payment_invite") {
        updateTextInput(session, "adm_email_subj", value = "Desbloquea tu acceso completo - PugaX Trade")
        updateTextAreaInput(session, "adm_email_body", value = paste0(
          "Hola {{USERNAME}},\n\n",
          "Para activar tu cuenta, ingresa a la App y ve a la sección 'Membresía' para Renovar o Pagar.\n\n",
          "Link App: https://app.pugaxtrade.com\n\n",
          "Saludos,\nEquipo PugaX"
        ))
      }
    })

    observeEvent(input$adm_confirm_email, {
      removeModal()
      tryCatch(
        {
          rows <- input$users_tbl_rows_selected
          df <- admin_users_df()
          if (is.null(rows) || is.null(df) || nrow(df) == 0) return()

          subj <- input$adm_email_subj
          body_tmpl <- input$adm_email_body
          if (!nzchar(subj) || !nzchar(body_tmpl)) {
            showNotification("Asunto y mensaje son obligatorios.", type = "error")
            return()
          }

          user_data <- df[rows, , drop = FALSE]
          emails_sent <- 0

          for (i in seq_len(nrow(user_data))) {
            uemail <- user_data$email[i]
            uname <- user_data$name[i] %||% user_data$username[i]

            if (!is.na(uemail) && nzchar(uemail)) {
              final_body <- gsub("{{USERNAME}}", uname, body_tmpl, fixed = TRUE)
              f_generic <- get0("auth_send_generic_email", envir = .GlobalEnv, inherits = TRUE)
              if (is.function(f_generic)) {
                tryCatch({
                  res_email <- f_generic(uemail, subj, final_body)
                  if (isTRUE(res_email$ok)) emails_sent <- emails_sent + 1
                }, error = function(e) message("Error sending email to ", uemail, ": ", e$message))
              }
            }
          }

          showNotification(sprintf("Correo enviado a %d usuario(s).", emails_sent), type = "message")
        },
        error = function(e) {
          message("FATAL Error in adm_confirm_email: ", e$message)
          showNotification(paste("Error crítico:", e$message), type = "error")
        }
      )
    })

    # ----------------- Admin: Desactivar Membresía Manual -----------------
    observeEvent(input$adm_deactivate_btn, {
      rows <- input$users_tbl_rows_selected
      if (is.null(rows) || length(rows) == 0) {
        showNotification("Selecciona al menos un usuario de la tabla.", type = "warning")
        return()
      }
      showModal(modalDialog(
        title = "Desactivar Membresía",
        div(
          style = "color: red; font-weight: bold;",
          "¿Estás seguro de que deseas desactivar la membresía de los usuarios seleccionados?"
        ),
        "Esto revocará su acceso premium inmediatamente.",
        checkboxInput(ns("adm_deact_email"), "Enviar correo de notificación", value = TRUE),
        footer = tagList(
          modalButton("Cancelar"),
          actionButton(ns("adm_confirm_deactivate"), "Confirmar Desactivación",
            class = "btn-danger",
            style = "background-color: #ff8c00; border-color: #ff8c00;"
          ) # Orange
        )
      ))
    })

    observeEvent(input$adm_confirm_deactivate, {
      removeModal()
      tryCatch(
        {
          rows <- input$users_tbl_rows_selected
          df <- admin_users_df()
          if (is.null(rows) || is.null(df) || nrow(df) == 0) return()

          user_data <- df[rows, , drop = FALSE]
          send_email <- isTRUE(input$adm_deact_email)

          cnt <- 0
          emails_sent <- 0

          for (i in seq_len(nrow(user_data))) {
            uid <- user_data$id[i]
            uemail <- user_data$email[i]
            uname <- user_data$name[i] %||% user_data$username[i]

            if (!is.na(uid)) {
              tryCatch({
                db_manual_deactivate(uid)
                cnt <- cnt + 1

                if (send_email && !is.na(uemail) && nzchar(uemail)) {
                  subj <- "Membresía Desactivada - PugaX Trade"
                  body <- paste0(
                    "Hola ", uname, ",\n\n",
                    "Tu membresía ha sido desactivada manualmente por el administrador.\n",
                    "Ya no tienes acceso premium a la plataforma.\n\n",
                    "Si deseas reactivarla, por favor contáctanos o ingresa a renovar.\n\n",
                    "PugaX App: https://app.pugaxtrade.com\n\n",
                    "Saludos,\nEquipo PugaX"
                  )
                  f_generic <- get0("auth_send_generic_email", envir = .GlobalEnv, inherits = TRUE)
                  if (is.function(f_generic)) {
                    res_email <- f_generic(uemail, subj, body)
                    if (isTRUE(res_email$ok)) emails_sent <- emails_sent + 1
                  }
                }
              }, error = function(e) message("Error deactivating user ", uid, ": ", e$message))
            }
          }

          # Admin Notification
          f_admin_action <- get0("auth_notify_admin_action", envir = .GlobalEnv, inherits = TRUE)
          if (is.function(f_admin_action)) {
            details <- paste0("Usuarios afectados: ", cnt, "\nCorreos: ", emails_sent)
            tryCatch(f_admin_action("Desactivación Manual", details), error = function(e) NULL)
          }

          showNotification(sprintf("Membresía desactivada para %d usuario(s). Correos enviados: %d.", cnt, emails_sent), type = "message")
          users_trigger(users_trigger() + 1)
        },
        error = function(e) {
          message("FATAL Error in adm_confirm_deactivate: ", e$message)
          showNotification(paste("Error crítico:", e$message), type = "error")
        }
      )
    })

    # ----------------- Admin: Borrar Cache -----------------
    observeEvent(input$adm_clear_cache, {
      showModal(modalDialog(
        title = "Borrar Caché",
        "¿Estás seguro que quieres borrar TODA la caché de análisis? Esto obligará a descargar datos nuevos para todos los tickers.",
        footer = tagList(
          modalButton("Cancelar"),
          actionButton(ns("adm_confirm_clear_cache"), "Confirmar Borrado", class = "btn-danger")
        )
      ))
    })

    observeEvent(input$adm_confirm_clear_cache, {
      removeModal()
      if (exists("db_clear_ticker_cache")) {
        tryCatch(
          {
            db_clear_ticker_cache()
            showNotification("Caché borrada exitosamente.", type = "message")
          },
          error = function(e) {
            showNotification(paste("Error al borrar caché:", e$message), type = "error")
          }
        )
      } else {
        showNotification("Función db_clear_ticker_cache no encontrada en db.R", type = "error")
      }
    })

    # ----------------- Admin: Eliminar Usuario -----------------
    observeEvent(input$adm_delete_btn, {
      rows <- input$users_tbl_rows_selected
      if (is.null(rows) || length(rows) == 0) {
        showNotification("Selecciona al menos un usuario de la tabla.", type = "warning")
        return()
      }
      showModal(modalDialog(
        title = "Eliminar Usuario",
        div(
          style = "color: red; font-weight: bold;",
          "¿Estás seguro de que deseas ELIMINAR PERMANENTEMENTE a los usuarios seleccionados?"
        ),
        "Esta acción borrará todo su historial, sesiones y datos. NO se puede deshacer.",
        footer = tagList(
          modalButton("Cancelar"),
          actionButton(ns("adm_confirm_delete"), "Confirmar Eliminación", class = "btn btn-danger")
        )
      ))
    })

    observeEvent(input$adm_confirm_delete, {
      removeModal()
      tryCatch(
        {
          message("DEBUG: Starting deletion process for Admin.")
          rows <- input$users_tbl_rows_selected
          df <- admin_users_df()
          if (is.null(rows) || is.null(df) || nrow(df) == 0) {
            message("Warning: No rows selected or data frame empty.")
            return()
          }

          user_data <- df[rows, , drop = FALSE]
          cnt <- 0
          emails_sent <- 0

          message("DEBUG: Processing deletion of ", nrow(user_data), " users.")

          for (i in seq_len(nrow(user_data))) {
            uid <- user_data$id[i]
            uemail <- user_data$email[i]
            uname <- user_data$name[i] %||% user_data$username[i]

            if (!is.na(uid)) {
              # 1. Send Notification Email (before deletion)
              message("DEBUG: Attempting to send deletion email to: ", uemail)
              tryCatch({
                if (!is.na(uemail) && nzchar(uemail)) {
                  f_del_email <- get0("auth_send_account_deleted_email", envir = .GlobalEnv, inherits = TRUE)
                  if (is.function(f_del_email)) {
                    res_email <- f_del_email(uemail, uname)
                    if (isTRUE(res_email$ok)) emails_sent <- emails_sent + 1
                  }
                }
              }, error = function(e) message("Notification Error during deletion: ", e$message))

              # 2. Delete User from DB
              message("DEBUG: Calling db_delete_user for ID: ", uid)
              tryCatch({
                if (isTRUE(db_delete_user(uid))) {
                  cnt <- cnt + 1
                }
              }, error = function(e) message("DB Deletion Error: ", e$message))
            }
          }

          message("DEBUG: Deletion loop finished. cnt=", cnt, ", emails=", emails_sent)
          showNotification(sprintf("%d usuario(s) eliminado(s). (%d correos enviados)", cnt, emails_sent), type = "message", duration = 8)
          
          message("DEBUG: Triggering table refresh.")
          users_trigger(users_trigger() + 1)
          message("DEBUG: Admin deletion observer finished successfully.")
        },
        error = function(e) {
          message("FATAL CRASH in adm_confirm_delete: ", e$message)
          showNotification(paste("Error crítico:", e$message), type = "error")
        }
      )
    })

    # ----------------- Descargar usuarios (Admin) -----------------
    output$download_users <- downloadHandler(
      filename = function() {
        paste0("usuarios_", format(Sys.time(), "%Y%m%d_%H%M"), ".csv")
      },
      content = function(file) {
        u <- user_reactive()
        if (is.null(u)) stop("No autorizado")
        is_super <- !is.null(u$username) && u$username[[1]] == "pedrobp86"
        if (!is_super && !db_is_admin(u)) stop("No autorizado")
        con <- NULL
        df <- data.frame()
        try(
          {
            con <- pool_init()
            on.exit(try(poolReturn(con), silent = TRUE), add = TRUE) # <-- DEVOLVER al pool
            df <- DBI::dbGetQuery(con, "
            select id, username, email, name, country, phone, is_admin,
                   created_at, trial_expires_at, last_login_at, active,
                   membership_active, membership_activated_at,
                   stripe_customer_id, stripe_subscription_id
            from users order by created_at
          ")
          },
          silent = TRUE
        )
        utils::write.csv(df, file, row.names = FALSE, fileEncoding = "UTF-8")
      }
    )

    observeEvent(input$logout, {
      tok <- session$userData$session_token()
      if (!is.null(tok) && nzchar(tok)) db_delete_token(tok)
      session$sendCustomMessage("delCookie", list(name = "puga_auth"))
      session$userData$session_token(NULL)
      session$userData$current_user(NULL)
      showNotification("Sesión cerrada.", type = "message", duration = 3)
      shinyjs::runjs(sprintf("window.location.replace('%s');", PUBLIC_BASE_URL))
    })
  })
}

# ----------------- App raíz -----------------
ui <- fluidPage(theme = my_theme, useShinyjs(), ui_head, uiOutput("root_ui"))

# Verificación de esquema una sola vez al inicio (no por sesión)
tryCatch(db_ensure_schema(), error = function(e) {
  message("Error ensuring schema: ", e$message)
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

  # (Parche 2) Retorno de Stripe: activar y guardar IDs (customer/subscription)
  observe({
    q <- parseQueryString(session$clientData$url_search)
    if (!is.null(q$paid) && q$paid == "1" && !is.null(q$session_id)) {
      # Evitar reprocesos por refresh (usar [[ ]] por nombre no sintáctico)
      if (isTRUE(session$userData[["__stripe_done"]])) {
        return()
      }
      session$userData[["__stripe_done"]] <- TRUE

      u <- session$userData$current_user()
      if (!is.null(u)) {
        # 1) Activamos membresía (tu helper maneja fechas)
        db_membership_activate(u$id[[1]])
        # 2) Obtenemos session info e IDs de Stripe
        si <- stripe_get_session_info(q$session_id)
        if (!is.null(si)) {
          db_update_user_stripe_ids(
            user_id         = u$id[[1]],
            customer_id     = si$customer_id,
            subscription_id = si$subscription_id
          )
        }
        # 3) Refrescar usuario en sesión
        u2 <- try(db_get_user_by_id(u$id[[1]]), silent = TRUE)
        if (!inherits(u2, "try-error") && !is.null(u2)) session$userData$current_user(u2)

        # 4) Referral Commission Logic (30% approx $9.00)
        # Check if user has referred_by and commission not already paid (logic simplistic for now: valid payment = valid commission)
        try(
          {
            if (!is.null(u2$referred_by) && !is.na(u2$referred_by[[1]])) {
              # Add simple logging or check if this payment was already processed for commission?
              # We rely on idempotency of this block (session$userData[["__stripe_done"]])
              # Commission: $9.00 USD
              db_add_commission(u2$referred_by[[1]], 9.00)
            }
          },
          silent = TRUE
        )

        # 5) Email de confirmación si existe helper
        try(
          {
            if (exists("auth_send_payment_success")) auth_send_payment_success(session$userData$current_user())
          },
          silent = TRUE
        )
      }
      showNotification("Pago completado. Tu cuenta ha sido activada.", type = "message", duration = 6)
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
    session$userData$current_user(NULL)
    session$userData$session_token(NULL)
    session$sendCustomMessage("delCookie", list(name = "puga_auth"))
    showNotification("Sesión cerrada por inactividad.", type = "warning", duration = 10)
    # Force reload or UI update handled by current_user observer
  })

  # Cierre seguro del pool al apagar la app
  onStop(function() {
    if (exists("pool_close", mode = "function")) {
      try(pool_close(), silent = TRUE)
    }
  })
}

shinyApp(ui, server)
