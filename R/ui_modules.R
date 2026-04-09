# R/ui_modules.R
# Módulos de UI para componentes secundarios (Cuenta, Membresía, Admin, Referidos)

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
  stripe_ok <- tryCatch(
    isTRUE(stripe_has_env()) || exists("auth_create_checkout_session"),
    error = function(e) FALSE
  )
  tagList(
    div(
      class = "section account-grid",
      div(
        class = "account-card",
        h4("Plan anual"),
        p("Precio: ", strong("29.99 USD / año")),
        actionButton(ns("mb_pay"), "Pagar con tarjeta (Stripe)", class = "btn btn-primary"),
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
