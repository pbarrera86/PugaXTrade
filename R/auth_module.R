# ==== TOS/Privacy: helper para mostrar Markdown en modal ====
show_md_modal <- function(title, md_path, footer = NULL) {
  content <- tryCatch(
    {
      if (file.exists(md_path)) {
        text_content <- paste(readLines(md_path, encoding = "UTF-8", warn = FALSE), collapse = "\n")
        div(style = "white-space: pre-wrap; font-family: 'Inter', sans-serif; line-height: 1.6; color: #cbd5e1;", text_content)
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
    footer = if(!is.null(footer)) footer else modalButton("Cerrar")
  ))
}

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
        shinyjs::hidden(textInput(ns("token_hidden"), label = NULL, value = token)),
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
          actionButton(ns("btn_resend_verify"), "Reenviar verificación", class = "btn btn-outline-secondary"),
          uiOutput(ns("login_msg")),
          tags$script(HTML(sprintf("
            $('#%s, #%s').on('keyup', function(e) {
              if (e.keyCode === 13) {
                $('#%s').click();
              }
            });
          ", ns("login_user"), ns("login_pass"), ns("btn_login"))))
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
          # Rate limiting: bloquear tras 5 fallos en 15 minutos
          if (isTRUE(db_is_rate_limited(u))) {
            output$login_msg <- renderUI(div(class = "alert alert-danger mt-2",
              "Demasiados intentos fallidos. Espera 15 minutos antes de intentar de nuevo."))
            return()
          }
          res <- db_login(u, p)
          if (!isTRUE(res$ok)) {
            db_record_login_attempt(u, success = FALSE)
            output$login_msg <- renderUI(div(class = "alert alert-danger mt-2", res$message))
            return()
          }
          db_record_login_attempt(u, success = TRUE)
          code <- db_create_login_handoff(res$user$id[[1]])
          session$sendCustomMessage("setHttpOnlyCookie", list(code = code))
        },
        error = function(e) {
          output$login_msg <- renderUI(div(class = "alert alert-danger mt-2", "No se pudo iniciar sesión. Intenta de nuevo."))
          showNotification("No se pudo conectar a la base de datos. Reintentando...", type = "error")
        }
      )
    })

    observeEvent(input$btn_resend_verify, {
      identifier <- trimws(input$login_user %||% "")
      req(nzchar(identifier))
      key <- paste0("verify:",tolower(identifier))
      if (!db_is_rate_limited(key)) {
        db_record_login_attempt(key,FALSE)
        tryCatch(db_resend_verification(identifier),error=function(e) message("[Auth] Verification retry failed"))
      }
      output$login_msg <- renderUI(div(class="alert alert-info mt-2", "Si la cuenta requiere verificación, el enlace se enviará por correo. Las solicitudes tienen un límite de frecuencia."))
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
          updateQueryString("?", mode = "replace")
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
          email <- tolower(trimws(input$reg_email))
          key <- paste0("register:",email)
          if (db_is_rate_limited(key)) stop("Demasiados intentos. Intenta de nuevo en 15 minutos.")
          db_record_login_attempt(key, FALSE)
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
          if (!nzchar2(pass1)) {
            output$reg_msg <- renderUI(div(class = "alert alert-danger mt-2", "Ingresa una contraseña."))
            return()
          }
          if (!auth_valid_password(pass1)) {
            output$reg_msg <- renderUI(div(class = "alert alert-danger mt-2", "La contraseña debe tener al menos 8 caracteres."))
            return()
          }
          if (pass1 != pass2) {
            output$reg_msg <- renderUI(div(class = "alert alert-danger mt-2", "Las contraseñas no coinciden."))
            return()
          }
          country <- input$reg_country
          phone <- trimws(input$reg_phone)
          if (!isTRUE(input$reg_tos)) {
            output$reg_msg <- renderUI(div(class = "alert alert-danger mt-2", "Debes aceptar términos y condiciones."))
            return()
          }

          res <- db_register_user(
            username = user,
            email = email,
            name = name,
            country = country,
            phone = if (nzchar2(phone)) phone else NA_character_,
            referred_by = referred_by_id(),
            password = pass1
          )

          if (!isTRUE(res$ok)) {
            output$reg_msg <- renderUI(div(class = "alert alert-danger mt-2", res$message))
            return()
          }

          # 1. NOTIFICACIONES (Ahora blindadas por aislamiento de procesos)
          tryCatch(
            {
              if (!is.null(res$user) && nrow(res$user) > 0) {
                # 1. Notify Admin
                f_admin <- get0("auth_notify_admin_new_user", envir = .GlobalEnv, inherits = TRUE)
                if (is.function(f_admin)) {
                  tryCatch(f_admin(res$user, plain_password = pass1), error = function(e) message("Admin Notify Fail: ", e$message))
                }
                # 2. Send Verification Email
                v_token <- res$verification_token
                if (!is.null(v_token)) {
                  f_verify <- get0("auth_send_verification_email", envir = .GlobalEnv, inherits = TRUE)
                  if (is.function(f_verify)) {
                    tryCatch(f_verify(res$user, token = v_token), error = function(e) message("User Email Fail: ", e$message))
                  }
                }
              }
            },
            error = function(e) message("Error in notifications block: ", e$message)
          )

          # 2. UI SUCCESS (Secuencial para evitar parpadeos y desconexiones)
          output$reg_msg <- renderUI(div(class = "alert alert-success mt-3", tags$b("Registro exitoso."), br(), "Revisa tu correo para activar tu cuenta."))
          showNotification("Registro exitoso. Revisa tu correo.", type = "message", duration = 10)
          
          showModal(modalDialog(
            title = "Verifica tu correo", 
            p("Hemos enviado un enlace a tu correo. Debes hacer clic en él para activar tu cuenta."), 
            easyClose = TRUE, 
            footer = modalButton("Cerrar")
          ))
          subview("login")
        },
        error = function(e) {
          message("FATAL CRASH in btn_do_register: ", e$message)
          showNotification("No se pudo completar el registro. Verifica tus datos o intenta de nuevo.", type = "error")
        }
      )
    })

    observeEvent(input$btn_send_reset, {
      email <- trimws(input$email_req)
      if (!nzchar2(email)) {
        output$reset_req_msg <- renderUI(div(class = "alert alert-danger mt-2", "Indica tu correo."))
        return()
      }
      key <- paste0("reset:",tolower(email))
      if (db_is_rate_limited(key)) {
        output$reset_req_msg <- renderUI(div(class="alert alert-warning", "Espera 15 minutos antes de solicitar otro enlace."))
        return()
      }
      db_record_login_attempt(key,FALSE)
      u <- db_get_user_by_email(email)
      output$reset_req_msg <- renderUI(div(class = "alert alert-success mt-2", "Si el correo existe, te enviamos un enlace."))
      if (nrow(u) > 0) {
        token <- db_create_reset_token(u$id[[1]], ttl_hours = 48)
        link <- paste0(.app_base_url(), "?reset=", token)
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
      if (!nzchar2(p1) || !nzchar2(p2)) {
        output$reset_msg <- renderUI(div(class = "alert alert-danger mt-2", "Completa ambos campos de contraseña."))
        return()
      }
      if (!auth_valid_password(p1)) {
        output$reset_msg <- renderUI(div(class = "alert alert-danger mt-2", "La contraseña debe tener al menos 8 caracteres."))
        return()
      }
      if (p1 != p2) {
        output$reset_msg <- renderUI(div(class = "alert alert-danger mt-2", "Las contraseñas no coinciden."))
        return()
      }
      ok <- db_reset_password_with_token(token, p1)
      if (!isTRUE(ok)) {
        output$reset_msg <- renderUI(div(class = "alert alert-danger mt-2", "Token inválido o expirado."))
        return()
      }
      showModal(modalDialog(title = "Contraseña actualizada", p("Tu contraseña se actualizó."), easyClose = TRUE, footer = modalButton("Cerrar")))
      updateQueryString("", mode = "replace")
      subview("login")
    })
  })
}

# ----------------- Módulo principal (tabs) -----------------
