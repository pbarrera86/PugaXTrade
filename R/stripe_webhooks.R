# R/stripe_webhooks.R
# Manejador de Webhooks nativo de Stripe para intercepción de peticiones HTTP en Shiny

handle_stripe_webhook <- function(req) {
  # 1. Leer el cuerpo de la petición (raw bytes)
  # Shiny / httpuv proporciona req$rook.input$read()
  req$rook.input$rewind()
  body_bytes <- req$rook.input$read(-1)
  
  if (length(body_bytes) == 0) {
    return(shiny::httpResponse(400, "text/plain", "Bad Request: Empty Body"))
  }
  
  payload_str <- rawToChar(body_bytes)
  
  # 2. Obtener firma y secreto
  sig_header <- req$HTTP_STRIPE_SIGNATURE
  secret <- Sys.getenv("STRIPE_WEBHOOK_SECRET")
  
  if (!nzchar(secret)) {
    # Si no hay secreto configurado, rechazamos por seguridad (o aceptamos bajo su propio riesgo)
    # Por seguridad, siempre exigimos el secreto para webhooks
    message("[Webhook] Error: STRIPE_WEBHOOK_SECRET no está configurado.")
    return(shiny::httpResponse(500, "text/plain", "Server configuration error"))
  }
  
  # 3. Validar la firma
  if (!verify_stripe_signature(payload_str, sig_header, secret)) {
    message("[Webhook] Error: Firma de Stripe inválida.")
    return(shiny::httpResponse(401, "text/plain", "Invalid Signature"))
  }
  
  # 4. Decodificar JSON
  event <- try(jsonlite::fromJSON(payload_str, simplifyVector = FALSE), silent = TRUE)
  if (inherits(event, "try-error")) {
    return(shiny::httpResponse(400, "text/plain", "Invalid JSON payload"))
  }
  
  # 5. Enrutador de eventos
  event_type <- event$type
  obj <- event$data$object
  
  message("[Webhook] Recibido evento: ", event_type)
  
  tryCatch({
    if (event_type == "checkout.session.completed") {
      # El usuario ha completado el pago
      user_id <- obj$client_reference_id
      customer_id <- obj$customer
      subscription_id <- obj$subscription
      
      if (!is.null(user_id) && nzchar(user_id)) {
        # 1. Activar membresía en la base de datos
        db_membership_activate(user_id)
        
        # 2. Actualizar IDs de Stripe en la DB
        db_update_user_stripe_ids(user_id, customer_id, subscription_id)
        
        # 3. Email de confirmación
        # NOTA: La comisión de referidos ya es manejada por db_membership_activate().
        # No repetir aquí para evitar doble acreditación si el webhook llega más de una vez.
        u_db <- try(db_get_user_by_id(user_id), silent = TRUE)
        if (!inherits(u_db, "try-error") && !is.null(u_db) && nrow(u_db) > 0) {
          if (exists("auth_send_payment_success", mode = "function")) {
            try(auth_send_payment_success(u_db), silent = TRUE)
          }
        }
        message("[Webhook] Membresía activada para usuario: ", user_id)
      } else {
        message("[Webhook] Warning: checkout.session.completed no trajo client_reference_id")
      }
      
    } else if (event_type == "customer.subscription.deleted") {
      # Suscripción cancelada/expirada — desactivar membresía
      customer_id <- obj$customer
      if (!is.null(customer_id) && nzchar(customer_id)) {
        u_db <- try(db_find_user_by_stripe_customer(customer_id), silent = TRUE)
        if (!inherits(u_db, "try-error") && !is.null(u_db) && nrow(u_db) > 0) {
          db_membership_deactivate(u_db$id[[1]])
          message("[Webhook] Membresía desactivada (suscripción borrada): ", u_db$id[[1]])
        }
      }

    } else if (event_type == "invoice.payment_failed") {
      # Pago fallido — desactivar membresía para evitar acceso sin pago
      customer_id <- obj$customer
      if (!is.null(customer_id) && nzchar(customer_id)) {
        u_db <- try(db_find_user_by_stripe_customer(customer_id), silent = TRUE)
        if (!inherits(u_db, "try-error") && !is.null(u_db) && nrow(u_db) > 0) {
          db_membership_deactivate(u_db$id[[1]])
          message("[Webhook] Membresía desactivada (pago fallido): ", u_db$id[[1]])
          # Notificar al usuario
          if (exists("auth_send_generic_email", mode = "function")) {
            body <- paste0(
              "Hola ", u_db$name[[1]], ",\n\n",
              "No pudimos procesar tu pago de renovación.\n",
              "Por favor actualiza tu método de pago en:\n",
              Sys.getenv("PUBLIC_BASE_URL", "https://app.pugaxtrade.com"), "\n\n",
              "Tu acceso ha sido suspendido temporalmente.\n\n",
              "Equipo PugaX"
            )
            try(auth_send_generic_email(u_db$email[[1]], "Pago fallido – PugaX Trade", body), silent = TRUE)
          }
        }
      }

    } else {
      # Eventos no manejados: responder 200 para que Stripe no reintente
      message("[Webhook] Evento ignorado (no manejado): ", event_type)
    }

    return(shiny::httpResponse(200, "text/plain", "OK"))
  }, error = function(e) {
    message("[Webhook] Error procesando evento: ", e$message)
    return(shiny::httpResponse(500, "text/plain", "Internal Server Error"))
  })
}

verify_stripe_signature <- function(payload_str, sig_header, secret) {
  if (is.null(sig_header) || !nzchar(sig_header)) return(FALSE)
  
  if (!requireNamespace("digest", quietly = TRUE)) {
    message("[Webhook] Error: paquete 'digest' no instalado necesario para verificar la firma.")
    return(FALSE)
  }
  
  parts <- strsplit(sig_header, ",")[[1]]
  t_part <- grep("^t=", parts, value = TRUE)
  v1_part <- grep("^v1=", parts, value = TRUE)
  
  if (length(t_part) == 0 || length(v1_part) == 0) return(FALSE)
  
  timestamp <- sub("^t=", "", t_part[1])
  v1_sig <- sub("^v1=", "", v1_part[1])

  # Protección contra replay attacks: rechazar webhooks con más de 5 minutos de antigüedad
  ts_num <- suppressWarnings(as.numeric(timestamp))
  if (is.na(ts_num) || abs(as.numeric(Sys.time()) - ts_num) > 300) {
    message("[Webhook] Firma rechazada: timestamp fuera de ventana de 5 minutos.")
    return(FALSE)
  }

  signed_payload <- paste0(timestamp, ".", payload_str)
  expected_sig <- digest::hmac(key = secret, object = signed_payload, algo = "sha256", serialize = FALSE)
  
  # Comparación sin early-exit para mitigar timing attacks básicos.
  # Aseguramos misma longitud antes de comparar carácter a carácter.
  if (nchar(expected_sig) != nchar(v1_sig)) return(FALSE)
  a_bytes <- utf8ToInt(expected_sig)
  b_bytes <- utf8ToInt(v1_sig)
  diff <- 0L
  for (i in seq_along(a_bytes)) diff <- bitwOr(diff, bitwXor(a_bytes[i], b_bytes[i]))
  return(diff == 0L)
}
