admin_server <- function(id, user_reactive, users_trigger) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    admin_users_df <- reactive({
      users_trigger() 
      u <- user_reactive()
      if (is.null(u)) return(NULL)
      
      is_super <- is_super_admin(u)
      if (!is_super && !db_is_admin(u)) return(NULL)

      df <- data.frame()
      tryCatch(
        {
          p <- get_pool()
          df <- DBI::dbGetQuery(p, "
            select id, username, email, name, country, phone, created_at, active,
                   membership_active, membership_expires_at, trial_expires_at,
                   stripe_customer_id, stripe_subscription_id,
                   referral_code, referral_wallet
            from users order by created_at desc
          ")

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
      display_df <- df[, setdiff(names(df), "id")]
      DT::datatable(display_df, rownames = FALSE, options = list(pageLength = 15, scrollX = TRUE))
    })

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
              tryCatch(
                {
                  db_manual_activate(uid, days_duration)
                  cnt <- cnt + 1

                  if (send_email && !is.na(uemail) && nzchar(uemail)) {
                    new_expiry <- Sys.Date() + days_duration
                    subj <- "Membresía Activada - PugaX Trade"
                    body <- paste0(
                      "Hola ", uname, ",\n\n",
                      "Tu membresía ha sido activada o extendida manualmente por el administrador.\n",
                      "Tu nuevo acceso es válido hasta: ", new_expiry, "\n\n",
                      "Accede aquí: ", .app_base_url(), "\n\n",
                      "¡Disfruta de PugaX Trade!\n"
                    )
                    f_generic <- get0("auth_send_generic_email", envir = .GlobalEnv, inherits = TRUE)
                    if (is.function(f_generic)) {
                      res_email <- f_generic(uemail, subj, body)
                      if (isTRUE(res_email$ok)) emails_sent <- emails_sent + 1
                    }
                  }
                },
                error = function(e) message("Error activating user ", uid, ": ", e$message)
              )
            }
          }

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

    observeEvent(input$adm_email_tmpl, {
      req(input$adm_email_tmpl)
      tm <- input$adm_email_tmpl
      if (tm == "renew") {
        updateTextInput(session, "adm_email_subj", value = "Recordatorio: Tu membresía vence pronto")
        updateTextAreaInput(session, "adm_email_body", value = paste0(
          "Hola {{USERNAME}},\n\n",
          "Te recordamos que tu acceso a PugaX Trade está próximo a vencer.\n",
          "Para renovar, ingresa a la App y ve a la sección 'Membresía' para Renovar o Pagar.\n\n",
          "Link App: ", .app_base_url(), "\n\n",
          "Saludos,\nEquipo PugaX"
        ))
      } else if (tm == "welcome") {
        updateTextInput(session, "adm_email_subj", value = "Membresía Activada - PugaX Trade")
        updateTextAreaInput(session, "adm_email_body", value = paste0(
          "Hola {{USERNAME}},\n\n",
          "Hemos activado tu membresía manualmente. Ya tienes acceso completo a la plataforma.\n\n",
          "Ingresa aquí: ", .app_base_url(), "\n\n",
          "¡Bienvenido de nuevo!\nEquipo PugaX"
        ))
      } else if (tm == "payment_invite") {
        updateTextInput(session, "adm_email_subj", value = "Desbloquea tu acceso completo - PugaX Trade")
        updateTextAreaInput(session, "adm_email_body", value = paste0(
          "Hola {{USERNAME}},\n\n",
          "Para activar tu cuenta, ingresa a la App y ve a la sección 'Membresía' para Renovar o Pagar.\n\n",
          "Link App: ", .app_base_url(), "\n\n",
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
                tryCatch(
                  {
                    res_email <- f_generic(uemail, subj, final_body)
                    if (isTRUE(res_email$ok)) emails_sent <- emails_sent + 1
                  },
                  error = function(e) message("Error sending email to ", uemail, ": ", e$message)
                )
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
          ) 
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
              tryCatch(
                {
                  db_manual_deactivate(uid)
                  cnt <- cnt + 1

                  if (send_email && !is.na(uemail) && nzchar(uemail)) {
                    subj <- "Membresía Desactivada - PugaX Trade"
                    body <- paste0(
                      "Hola ", uname, ",\n\n",
                      "Tu membresía ha sido desactivada manualmente por el administrador.\n",
                      "Ya no tienes acceso premium a la plataforma.\n\n",
                      "Si deseas reactivarla, por favor contáctanos o ingresa a renovar.\n\n",
                      "PugaX App: ", .app_base_url(), "\n\n",
                      "Saludos,\nEquipo PugaX"
                    )
                    f_generic <- get0("auth_send_generic_email", envir = .GlobalEnv, inherits = TRUE)
                    if (is.function(f_generic)) {
                      res_email <- f_generic(uemail, subj, body)
                      if (isTRUE(res_email$ok)) emails_sent <- emails_sent + 1
                    }
                  }
                },
                error = function(e) message("Error deactivating user ", uid, ": ", e$message)
              )
            }
          }

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
          rows <- input$users_tbl_rows_selected
          df <- admin_users_df()
          if (is.null(rows) || is.null(df) || nrow(df) == 0) return()

          user_data <- df[rows, , drop = FALSE]
          cnt <- 0
          emails_sent <- 0

          for (i in seq_len(nrow(user_data))) {
            uid <- user_data$id[i]
            uemail <- user_data$email[i]
            uname <- user_data$name[i] %||% user_data$username[i]

            if (!is.na(uid)) {
              # 1. DELETE FIRST (Para liberar la DB lo antes posible)
              ok_del <- FALSE
              tryCatch({
                if (isTRUE(db_delete_user(uid))) {
                   ok_del <- TRUE
                   cnt <- cnt + 1
                }
              }, error = function(e) message("DB Deletion Error: ", e$message))

              # 2. ENVIAR NOTIFICACIONES solo si el borrado fue exitoso y sin bloquear
              if (ok_del) {
                tryCatch({
                   if (!is.na(uemail) && nzchar(uemail)) {
                     f_del_email <- get0("auth_send_account_deleted_email", envir = .GlobalEnv, inherits = TRUE)
                     if (is.function(f_del_email)) {
                       res_email <- f_del_email(uemail, uname)
                       if (isTRUE(res_email$ok)) emails_sent <- emails_sent + 1
                     }
                   }
                }, error = function(e) message("Notification Error: ", e$message))

                tryCatch({
                  f_admin_del <- get0("auth_notify_admin_account_deleted", envir = .GlobalEnv, inherits = TRUE)
                  if (is.function(f_admin_del)) {
                    f_admin_del(uid, uname, if(!is.na(uemail)) uemail else "N/A")
                  }
                }, error = function(e) message("Admin Notification Error: ", e$message))
              }
            }
          }

          # 3. FINALIZACIÓN (Secuencial para evitar desconexiones)
          if (cnt > 0) {
            users_trigger(users_trigger() + 1)
            showNotification(sprintf("Se eliminaron %d usuarios (%d correos enviados).", cnt, emails_sent), type = "message")
          } else {
            showNotification("No se pudo eliminar al usuario. Revisa los logs.", type = "error")
          }
          
        },
        error = function(e) {
          showNotification(paste("Error crítico en el proceso:", e$message), type = "error")
        }
      )
    })

    output$download_users <- downloadHandler(
      filename = function() {
        paste0("usuarios_", format(Sys.time(), "%Y%m%d_%H%M"), ".csv")
      },
      content = function(file) {
        u <- user_reactive()
        if (is.null(u) || (!is_super_admin(u) && !db_is_admin(u))) stop("No autorizado")
        
        con <- NULL
        df <- data.frame()
        try(
          {
            con <- pool_init()
            on.exit(try(poolReturn(con), silent = TRUE), add = TRUE)
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
  })
}
