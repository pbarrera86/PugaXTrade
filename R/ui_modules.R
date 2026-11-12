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

section_membership_ui <- function(ns, user = NULL) {
  sid <- if(is.null(user)) "" else user$stripe_subscription_id[[1]] %||% ""
  customer <- if(is.null(user)) "" else user$stripe_customer_id[[1]] %||% ""
  status <- if(is.null(user) || is.null(user$stripe_status)) "" else user$stripe_status[[1]] %||% ""
  canceled <- !is.null(user) && isTRUE(user$stripe_cancel_at_period_end[[1]])
  admin <- !is.null(user) && (is_super_admin(user) || db_is_admin(user))
  existing <- nzchar(sid) && !status %in% c("canceled","incomplete_expired")
  tagList(div(class="section account-grid",
    div(class="account-card",
      h4("Plan anual"),
      p("Precio: ",strong("29.99 USD / año")),
      p(class="text-muted","Los impuestos aplicables y el importe final se muestran antes de confirmar el pago en Stripe."),
      if(!admin && !existing) actionButton(ns("mb_pay"),"Pagar con tarjeta (Stripe)",class="btn btn-primary"),
      if(nzchar(customer)) tagList(tags$hr(),h4("Renovación"),
        p(class="text-muted","Administra tu suscripción y método de pago. Al cancelar la renovación conservarás acceso hasta el fin del periodo pagado."),
        div(class="d-flex flex-wrap gap-2",
          if(existing && !canceled) actionButton(ns("mb_cancel_auto"),"Cancelar renovación automática",class="btn btn-warning"),
          if(existing && canceled) actionButton(ns("mb_reactivate_auto"),"Reactivar renovación automática",class="btn btn-warning"),
          actionButton(ns("mb_renew_now"),"Abrir portal de facturación",class="btn btn-primary")))
    ),
    div(class="account-card",h4("Estado"),uiOutput(ns("membership_state")),uiOutput(ns("renewal_state")))
  ))
}

section_admin_ui <- function(ns) {
  tagList(
    div(
      class = "section",
      div(
        class = "card p-4",
        uiOutput(ns("operations_status")),
        h3("Usuarios registrados"),
        div(
          class = "d-flex flex-wrap gap-2 mb-3",
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
