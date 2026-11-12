# R/extremos_module.R
# =========================================================
# Módulo: Screener de Extremos Locales (QMin / QMax)
# Se muestra como pestaña dentro de "Análisis Avanzado".
# Dos búsquedas:
#   - MÍNIMOS locales: precio por debajo del Precio QMin elegido
#     (caída desde el último máximo mayor que la mayoría de sus caídas).
#   - MÁXIMOS locales: precio por encima del Precio QMax elegido
#     (subida desde el último mínimo mayor que la mayoría de sus subidas).
# Lógica de cálculo en R/extremos_core.R
# =========================================================

# Textos y etiquetas según el tipo de búsqueda
ext_modo_info <- function(modo) {
  if (identical(modo, "max")) {
    list(
      titulo = "Empresas en MÁXIMOS locales",
      Q = "QMax", ref = "Último Mínimo", fref = "Fecha Últ. Mín.",
      mov = "Subida Actual", media = "Media Subidas", n = "# Subidas",
      nivel = "Nivel Superado", cond = "por encima de su",
      zona = "posible zona de techo o toma de utilidades",
      tramos = "subidas", color = "#2ecc71"
    )
  } else {
    list(
      titulo = "Empresas en MÍNIMOS locales",
      Q = "QMin", ref = "Último Máximo", fref = "Fecha Últ. Máx.",
      mov = "Caída Actual", media = "Media Caídas", n = "# Caídas",
      nivel = "Nivel Perforado", cond = "por debajo de su",
      zona = "posible zona de rebote o cambio de tendencia al alza",
      tramos = "caídas", color = "#ff6b6b"
    )
  }
}

ext_btn_label <- function(modo) {
  if (identical(modo, "max")) {
    tagList(icon("arrow-trend-up"), tags$b(" Buscar empresas en MÁXIMOS locales"),
      tags$br(), tags$small("Subieron más que su QMax · posible techo"))
  } else {
    tagList(icon("arrow-trend-down"), tags$b(" Buscar empresas en MÍNIMOS locales"),
      tags$br(), tags$small("Cayeron más que su QMin · posible rebote"))
  }
}

extremos_ui <- function(id) {
  ns <- NS(id)
  tagList(
    br(),
    div(
      class = "card p-3 mb-4",
      h4("Screener de Extremos Locales"),
      p(
        class = "text-muted", style = "margin-bottom:12px;",
        "Compara el movimiento actual de cada empresa contra sus propios movimientos históricos entre extremos locales. ",
        HTML("<b style='color:#ff6b6b;'>Mínimos:</b> la caída desde su último máximo ya supera a la mayoría de sus caídas (posible rebote). "),
        HTML("<b style='color:#2ecc71;'>Máximos:</b> la subida desde su último mínimo ya supera a la mayoría de sus subidas (posible techo).")
      ),
      fluidRow(
        column(8, textInput(ns("add_input"), "Agregar tickers (separados por coma o espacio)",
          placeholder = "Ej.: NVO, PFE, AMD", width = "100%")),
        column(2, div(style = "margin-top:32px;", actionButton(ns("add_btn"), tagList(icon("plus"), "Agregar"),
          class = "btn btn-info w-100"))),
        column(2, div(style = "margin-top:32px;", actionButton(ns("clear_btn"), tagList(icon("trash"), "Limpiar lista"),
          class = "btn btn-danger w-100")))
      ),
      textAreaInput(
        ns("tickers"), "Tickers a analizar (puedes escribir o pegar los que quieras)",
        value = ext_default_tickers_text(),
        rows = 4, width = "100%"
      ),
      uiOutput(ns("count_ui")),
      fluidRow(
        column(3, selectInput(ns("q_sel"), "Nivel de cuantil (QMin / QMax)",
          choices = c("80 (más resultados)" = 80, "85" = 85, "90 (recomendado)" = 90, "95" = 95, "99 (solo casos extremos)" = 99),
          selected = 90
        )),
        column(3, selectInput(ns("umbral"), "Sensibilidad de extremos (zigzag)",
          choices = c("15% (más extremos)" = 0.15, "20% (recomendado)" = 0.20, "25%" = 0.25, "30% (solo grandes ciclos)" = 0.30),
          selected = 0.20
        )),
        column(3, numericInput(ns("min_caidas"), "Mínimo de tramos históricos", value = 5, min = 2, max = 50, step = 1)),
        column(3, selectInput(ns("cache_h"), "Datos de precios",
          choices = c("Usar caché (12 h)" = 12, "Descargar frescos" = 0),
          selected = 12
        ))
      ),
      fluidRow(
        column(6, actionButton(ns("run_min"), ext_btn_label("min"),
          class = "btn btn-danger w-100 mb-2", style = "padding:10px 6px; line-height:1.25;")),
        column(6, actionButton(ns("run_max"), ext_btn_label("max"),
          class = "btn btn-success w-100 mb-2", style = "padding:10px 6px; line-height:1.25;"))
      ),
      fluidRow(
        column(6, actionButton(ns("reset_btn"), tagList(icon("undo"), "Restaurar tickers"), class = "btn btn-warning w-100 mb-2")),
        column(6, downloadButton(ns("dl_excel"), "Exportar Excel", class = "btn btn-primary w-100 mb-2"))
      ),
      uiOutput(ns("progress_ui"))
    ),
    div(
      class = "card p-3 mb-4",
      uiOutput(ns("res_title")),
      uiOutput(ns("resumen_ui")),
      checkboxInput(ns("ver_todos"), "Mostrar todas las empresas analizadas (no solo las que cumplen)", value = FALSE),
      DTOutput(ns("cand_tbl"))
    ),
    div(
      class = "card p-3 mb-4",
      fluidRow(
        column(8, h4("Detalle por empresa")),
        column(4, selectInput(ns("det_ticker"), NULL, choices = character(0), width = "100%"))
      ),
      plotlyOutput(ns("ret_plot"), height = "460px"),
      br(),
      plotlyOutput(ns("price_plot"), height = "420px"),
      br(),
      h5("Métricas actuales"),
      DTOutput(ns("metrics_tbl"))
    )
  )
}

extremos_server <- function(id, can_analyze = reactive(TRUE), user_reactive = reactive(NULL)) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    rv <- reactiveValues(
      result = NULL, running = FALSE, prog_file = NULL,
      status = "idle", # idle | running | ok | error
      msg = NULL, n_total = 0L, started = NULL, modo = "min"
    )

    # Controles que se bloquean mientras corre el proceso
    ctrl_ids <- c("run_min", "run_max", "reset_btn", "dl_excel", "add_btn", "clear_btn", "add_input",
                  "tickers", "q_sel", "umbral", "min_caidas", "cache_h")
    set_busy <- function(busy, modo = "min") {
      for (i in ctrl_ids) {
        if (busy) shinyjs::disable(i) else shinyjs::enable(i)
      }
      if (busy) {
        btn <- if (modo == "max") "run_max" else "run_min"
        shinyjs::html(btn, as.character(tagList(icon("spinner", class = "fa-spin"),
          tags$b(sprintf(" Buscando empresas en %s locales...", if (modo == "max") "MÁXIMOS" else "MÍNIMOS")),
          tags$br(), tags$small("Procesando, espera por favor"))))
      } else {
        shinyjs::html("run_min", as.character(ext_btn_label("min")))
        shinyjs::html("run_max", as.character(ext_btn_label("max")))
      }
    }

    parse_ticks <- function(txt) {
      x <- unlist(strsplit(toupper(txt %||% ""), "[,;\\s]+", perl = TRUE))
      x <- gsub("[^A-Z0-9.^=-]", "", x) # conserva el guion (LIVEPOLC-1.MX, MFRISCOA-1.MX)
      unique(x[nzchar(x)])
    }

    output$count_ui <- renderUI({
      n <- length(parse_ticks(input$tickers))
      div(class = "text-muted", style = "margin:-6px 0 10px 0; font-size:.9em;", sprintf("%d tickers en la lista", n))
    })

    observeEvent(input$add_btn, {
      nuevos <- parse_ticks(input$add_input)
      if (!length(nuevos)) {
        showNotification("Escribe al menos un ticker para agregar.", type = "warning")
        return()
      }
      actuales <- parse_ticks(input$tickers)
      agregados <- setdiff(nuevos, actuales)
      updateTextAreaInput(session, "tickers", value = paste(c(actuales, agregados), collapse = ", "))
      updateTextInput(session, "add_input", value = "")
      showNotification(
        if (length(agregados)) sprintf("Agregados: %s", paste(agregados, collapse = ", ")) else "Esos tickers ya estaban en la lista.",
        type = "message"
      )
    })

    observeEvent(input$clear_btn, {
      updateTextAreaInput(session, "tickers", value = "")
    })

    # ---------------- Tarea en segundo plano ----------------
    # Trabajo pesado en un proceso R independiente.
    # Preferimos mirai (proceso efímero propio): no depende del plan global de
    # `future`, que el análisis institucional puede dejar en "sequential" y
    # entonces bloquearía la sesión (sin refrescar la barra de avance).
    ext_worker_job <- function(app_dir, ticks, umbral, q_sel, min_caidas, cache_h, prog_file, modo) {
      setwd(app_dir)
      suppressWarnings(suppressMessages(library(quantmod)))
      source(file.path(app_dir, "R", "extremos_core.R"), local = TRUE)
      ext_run_screener(
        ticks,
        umbral = umbral, q_sel = q_sel, min_caidas = min_caidas,
        max_age_hours = cache_h, modo = modo,
        progress_cb = function(k, n, tk) {
          try(writeLines(sprintf("%d|%d|%s", k, n, tk), prog_file), silent = TRUE)
        }
      )
    }
    ext_current_job <- NULL
    ext_ticket <- NULL
    ext_release <- function() {
      try(puga_job_release(ext_ticket), silent=TRUE)
      ext_ticket <<- NULL
    }
    session$onSessionEnded(function() {
      if (!is.null(ext_current_job)) try(mirai::stop_mirai(ext_current_job), silent=TRUE)
      if (!is.null(shiny::isolate(rv$prog_file))) unlink(shiny::isolate(rv$prog_file))
      ext_release()
    })
    use_mirai <- requireNamespace("mirai", quietly = TRUE)
    message("[EXT] Worker en segundo plano: ", if (use_mirai) "mirai (proceso propio)" else "future")

    ext_task <- tryCatch(
      shiny::ExtendedTask$new(function(ticks, umbral, q_sel, min_caidas, cache_h, prog_file, modo) {
        app_dir <- normalizePath(getwd())
        if (use_mirai) {
          m <- mirai::mirai(
            job(app_dir, ticks, umbral, q_sel, min_caidas, cache_h, prog_file, modo),
            job = ext_worker_job, app_dir = app_dir, ticks = ticks, umbral = umbral,
            q_sel = q_sel, min_caidas = min_caidas, cache_h = cache_h, prog_file = prog_file, modo = modo, .timeout = puga_job_timeout_ms
          )
          ext_current_job <<- m
          promises::as.promise(m)
        } else {
          promises::future_promise(
            ext_worker_job(app_dir, ticks, umbral, q_sel, min_caidas, cache_h, prog_file, modo),
            seed = TRUE
          )
        }
      }),
      error = function(e) {
        message("[EXT] ExtendedTask no disponible: ", e$message)
        NULL
      }
    )

    observeEvent(input$reset_btn, {
      updateTextAreaInput(session, "tickers", value = ext_default_tickers_text())
    })

    params <- reactive({
      list(
        umbral = as.numeric(input$umbral),
        q_sel = as.integer(input$q_sel),
        min_caidas = max(2L, as.integer(input$min_caidas %||% 5)),
        cache_h = as.numeric(input$cache_h)
      )
    })

    finish_ok <- function(res) {
      rv$result <- res
      rv$running <- FALSE
      ext_release()
      if (is.null(res) || is.null(res$todos) || nrow(res$todos) == 0) {
        rv$status <- "error"
        rv$msg <- if (!is.null(res) && length(res$fallidos)) {
          sprintf("No se pudieron obtener datos de ningún ticker (%s). Verifica que los símbolos existan en Yahoo Finance.",
            paste(head(res$fallidos, 20), collapse = ", "))
        } else {
          "No se obtuvieron resultados. Verifica los tickers e intenta de nuevo."
        }
        showNotification(rv$msg, type = "error", duration = 10)
      } else {
        mi <- ext_modo_info(res$params$modo)
        rv$status <- "ok"
        seg <- if (!is.null(rv$started)) round(as.numeric(difftime(Sys.time(), rv$started, units = "secs"))) else NA
        rv$msg <- sprintf(
          "Proceso concluido con éxito (búsqueda de %s). %d empresas analizadas, %d cumplen la condición%s%s.",
          sub("Empresas en ", "", mi$titulo),
          nrow(res$todos), nrow(res$candidatos),
          if (length(res$fallidos)) sprintf(", %d sin datos suficientes (%s)", length(res$fallidos), paste(head(res$fallidos, 15), collapse = ", ")) else "",
          if (!is.na(seg)) sprintf(" · Tiempo: %d s", seg) else ""
        )
        showNotification("Proceso concluido con éxito.", type = "message", duration = 6)
      }
      set_busy(FALSE)
    }

    finish_error <- function(err) {
      rv$running <- FALSE
      ext_release()
      rv$status <- "error"
      message("[Extremes] Worker failed: ", err)
      rv$msg <- "No se pudo completar la búsqueda. Si alcanzó 20 minutos, reduce la lista de símbolos e intenta de nuevo."
      showNotification(rv$msg, type = "error", duration = 10)
      set_busy(FALSE)
    }

    run_search <- function(modo) {
      if (isTRUE(rv$running)) return()
      if (!isTRUE(can_analyze())) {
        showNotification("Tu prueba finalizó o no tienes membresía activa.", type = "warning")
        return()
      }
      ticks <- parse_ticks(input$tickers)
      if (length(ticks) > 1500L) {
        showNotification("El máximo por análisis es de 1500 símbolos.", type = "warning")
        return()
      }
      if (length(ticks) == 0) {
        showNotification("Ingresa al menos un ticker válido.", type = "warning")
        return()
      }
      if (!use_mirai || is.null(ext_task)) {
        showNotification("El servicio de análisis no está disponible. Intenta más tarde.", type="warning")
        return()
      }
      ext_ticket <<- tryCatch(puga_job_acquire(user_reactive()$id[[1]]), error=function(e) NULL)
      if (is.null(ext_ticket)) {
        showNotification("Hay análisis en curso. Espera a que termine el tuyo o vuelve a intentar en unos minutos.", type="warning")
        return()
      }
      p <- params()
      rv$modo <- modo
      rv$prog_file <- tempfile("ext_prog_", fileext = ".txt")
      writeLines(sprintf("0|%d|", length(ticks)), rv$prog_file)
      rv$n_total <- length(ticks)
      rv$started <- Sys.time()
      rv$running <- TRUE
      rv$status <- "running"
      rv$msg <- NULL
      set_busy(TRUE, modo)

      if (!is.null(ext_task)) {
        ok <- tryCatch({
          ext_task$invoke(ticks, p$umbral, p$q_sel, p$min_caidas, p$cache_h, rv$prog_file, modo)
          TRUE
        }, error = function(e) {
          message("[EXT] invoke() falló: ", e$message)
          FALSE
        })
        if (ok) return()
      }

      finish_error("No fue posible iniciar el proceso. Intenta más tarde.")
    }

    observeEvent(input$run_min, run_search("min"))
    observeEvent(input$run_max, run_search("max"))

    if (!is.null(ext_task)) {
      observeEvent(ext_task$status(), {
        st <- ext_task$status()
        if (!isTRUE(rv$running)) return()
        if (st == "success") {
          res <- tryCatch(ext_task$result(), error = function(e) NULL)
          finish_ok(res)
        } else if (st == "error") {
          err <- tryCatch({ ext_task$result(); "error desconocido" }, error = function(e) conditionMessage(e))
          finish_error(err)
        }
      }, ignoreInit = TRUE)
    }

    # ---------------- Barra de avance y estado final ----------------
    output$progress_ui <- renderUI({
      st <- rv$status
      bar <- function(pct, cls, label) {
        div(class = "progress", style = "height:26px; margin-top:6px;",
          div(class = paste("progress-bar", cls), role = "progressbar",
            style = sprintf("width:%d%%; font-weight:700; font-size:.95em;", pct),
            `aria-valuenow` = pct, `aria-valuemin` = 0, `aria-valuemax` = 100, label))
      }
      if (st == "running") {
        invalidateLater(700)
        ln <- tryCatch(readLines(rv$prog_file, warn = FALSE)[1], error = function(e) NA_character_)
        parts <- strsplit(ifelse(is.na(ln), "0|1|", ln), "|", fixed = TRUE)[[1]]
        k <- suppressWarnings(as.integer(parts[1])); n <- suppressWarnings(as.integer(parts[2]))
        tk <- if (length(parts) >= 3) parts[3] else ""
        if (is.na(k)) k <- 0L
        if (is.na(n) || n < 1) n <- max(1L, rv$n_total)
        # k = ticker en proceso; se cuenta como terminado el anterior
        done <- max(0L, k - 1L)
        pct <- as.integer(floor(100 * done / n))
        seg <- round(as.numeric(difftime(Sys.time(), rv$started, units = "secs")))
        que <- if (rv$modo == "max") "máximos" else "mínimos"
        return(tagList(
          div(style = "font-weight:600; margin-top:10px;",
            icon("spinner", class = "fa-spin"),
            if (nzchar(tk)) sprintf(" Buscando %s locales · Procesando %s (%d de %d) · %d s", que, tk, k, n, seg)
            else sprintf(" Preparando %d tickers...", n)),
          bar(max(pct, 2L), "progress-bar-striped progress-bar-animated bg-info", sprintf("%d%%", pct))
        ))
      }
      if (st == "ok") {
        return(tagList(
          bar(100L, "bg-success", "100%"),
          div(class = "alert alert-success mt-2", style = "margin-bottom:0;", icon("check-circle"), " ", rv$msg)
        ))
      }
      if (st == "error") {
        return(tagList(
          bar(100L, "bg-danger", "Error"),
          div(class = "alert alert-danger mt-2", style = "margin-bottom:0;", icon("exclamation-triangle"), " ", rv$msg)
        ))
      }
      NULL
    })

    # ---------------- Tabla de candidatos ----------------
    modo_res <- reactive({
      if (is.null(rv$result)) "min" else (rv$result$params$modo %||% "min")
    })

    tabla_actual <- reactive({
      req(rv$result)
      if (isTRUE(input$ver_todos)) rv$result$todos else rv$result$candidatos
    })

    output$res_title <- renderUI({
      if (is.null(rv$result)) return(h4("Empresas que cumplen la condición"))
      mi <- ext_modo_info(modo_res())
      h4(style = sprintf("color:%s;", mi$color), icon(if (modo_res() == "max") "arrow-trend-up" else "arrow-trend-down"), " ", mi$titulo)
    })

    output$resumen_ui <- renderUI({
      req(rv$result)
      r <- rv$result
      mi <- ext_modo_info(modo_res())
      div(
        class = "alert alert-info", style = "margin-bottom:10px;",
        HTML(sprintf(
          "<b>%d</b> de <b>%d</b> empresas están <b>%s %s %d</b> → %s (zigzag %d%%, mínimo %d %s históricas).%s",
          nrow(r$candidatos), nrow(r$todos), mi$cond, mi$Q, r$params$q_sel, mi$zona,
          round(100 * r$params$umbral), r$params$min_caidas, mi$tramos,
          if (length(r$fallidos)) sprintf(" Sin datos suficientes: %s.", paste(head(r$fallidos, 15), collapse = ", ")) else ""
        ))
      )
    })

    output$cand_tbl <- renderDT({
      df <- tabla_actual()
      q <- rv$result$params$q_sel
      mi <- ext_modo_info(modo_res())
      if (is.null(df) || nrow(df) == 0) {
        return(datatable(data.frame(Mensaje = "Ninguna empresa cumple la condición con estos parámetros."),
          rownames = FALSE, options = list(dom = "t")))
      }
      show <- data.frame(
        Ticker = df$Ticker,
        `Precio Actual` = round(df$PrecioActual, 2),
        Ref = round(df$ExtremoRef, 2),
        FRef = format(df$FechaExtremoRef, "%Y-%m-%d"),
        Mov = df$MovActual,
        QSel = df$QSel,
        PrecioQ = round(df$PrecioQSel, 2),
        `Distancia al Nivel` = df$DistanciaPct,
        Nivel = df$NivelSuperado,
        Media = df$QMedia,
        NT = df$NumTramos,
        `Días Últ. Extremo` = df$DiasUltExtremo,
        Tramo = df$Tramo,
        check.names = FALSE
      )
      lbl_q <- sprintf("%s%d", mi$Q, q)
      names(show) <- c("Ticker", "Precio Actual", mi$ref, mi$fref, mi$mov, lbl_q, paste("Precio", lbl_q),
        "Distancia al Nivel", mi$nivel, mi$media, mi$n, "Días Últ. Extremo", "Tramo")

      datatable(
        show,
        rownames = FALSE, selection = "single",
        options = list(scrollX = TRUE, pageLength = 15,
          order = list(list(7, if (modo_res() == "max") "desc" else "asc")))
      ) %>%
        formatPercentage(c(mi$mov, lbl_q, "Distancia al Nivel", mi$media), 1) %>%
        formatStyle("Distancia al Nivel", color = mi$color, fontWeight = "bold") %>%
        formatStyle("Ticker", fontWeight = "bold")
    })

    # ---------------- Selector de detalle ----------------
    observeEvent(rv$result, {
      r <- rv$result
      tks <- c(r$candidatos$Ticker, setdiff(r$todos$Ticker, r$candidatos$Ticker))
      updateSelectInput(session, "det_ticker", choices = tks, selected = if (length(tks)) tks[1] else character(0))
    })
    observeEvent(input$cand_tbl_rows_selected, {
      df <- tabla_actual()
      i <- input$cand_tbl_rows_selected
      if (length(i) && !is.null(df) && nrow(df) >= i) updateSelectInput(session, "det_ticker", selected = df$Ticker[i])
    })

    detalle <- reactive({
      req(rv$result, input$det_ticker)
      rv$result$details[[input$det_ticker]]
    })

    # ---------------- Gráfica: Rendimiento serie extremos locales ----------------
    output$ret_plot <- renderPlotly({
      d <- detalle(); req(d)
      tk <- input$det_ticker
      modo <- modo_res()
      piv <- d$pivots
      tb <- d$table
      g <- function(con, col) tb[[col]][tb$Concepto == con]

      serie <- data.frame(date = piv$date, ret = ifelse(is.na(piv$ret), 0, piv$ret) * 100, type = piv$type, conf = piv$confirmed)
      serie_c <- serie[serie$conf, ]
      # Punto actual: caída desde el último máximo (mínimos) o subida desde el último mínimo (máximos)
      mov <- if (modo == "max") d$rebote_actual else d$caida_actual
      act <- data.frame(date = d$date_now, ret = mov * 100)
      x0 <- min(serie$date); x1 <- d$date_now

      niveles <- list(
        list("QMin 80", g("Quantil80", "Minimos"), "#6c7cff", "dash"),
        list("QMin 90", g("Quantil90", "Minimos"), "#c084fc", "dash"),
        list("QMin 99", g("Quantil99", "Minimos"), "#ffcc00", "dash"),
        list("Media Min", g("Media", "Minimos"), "#ff6b6b", "dot"),
        list("QMax 80", g("Quantil80", "Maximos"), "#6c7cff", "dash"),
        list("QMax 90", g("Quantil90", "Maximos"), "#c084fc", "dash"),
        list("QMax 99", g("Quantil99", "Maximos"), "#ffcc00", "dash"),
        list("Media Max", g("Media", "Maximos"), "#2ecc71", "dot")
      )

      p <- plot_ly() %>%
        add_lines(data = serie_c, x = ~date, y = ~ret, name = "Rendimiento (%)",
          line = list(color = "#bbbbbb", width = 1.6),
          text = ~sprintf("%s<br>%s: %.1f%%", format(date, "%Y-%m-%d"), ifelse(type == "min", "Mínimo", "Máximo"), ret),
          hoverinfo = "text") %>%
        add_markers(data = serie_c, x = ~date, y = ~ret, name = "Extremos",
          marker = list(size = 6, color = ifelse(serie_c$type == "min", "#ff6b6b", "#2ecc71")),
          hoverinfo = "skip", showlegend = FALSE) %>%
        add_markers(data = act, x = ~date, y = ~ret, name = "Actual",
          marker = list(size = 12, color = "#00d4ff", symbol = "diamond"),
          text = ~sprintf("Actual: %+.1f%% desde el %s", ret, if (modo == "max") "último mínimo" else "último máximo"),
          hoverinfo = "text")
      for (nv in niveles) {
        p <- p %>% add_segments(x = x0, xend = x1, y = nv[[2]] * 100, yend = nv[[2]] * 100,
          name = sprintf("%s (%.1f%%)", nv[[1]], nv[[2]] * 100),
          line = list(color = nv[[3]], dash = nv[[4]], width = 1.2), hoverinfo = "name")
      }
      titulo <- if (modo == "max") {
        sprintf("%s · QMax80: %.1f%% · QMax90: %.1f%% · MediaMax: %.1f%% · Subida actual: %+.1f%% · DíasUExtremo: %d",
          tk, g("Quantil80", "Maximos") * 100, g("Quantil90", "Maximos") * 100, g("Media", "Maximos") * 100,
          mov * 100, d$dias_ult_extremo)
      } else {
        sprintf("%s · QMin80: %.1f%% · QMin90: %.1f%% · MediaMin: %.1f%% · Caída actual: %.1f%% · DíasUExtremo: %d",
          tk, g("Quantil80", "Minimos") * 100, g("Quantil90", "Minimos") * 100, g("Media", "Minimos") * 100,
          mov * 100, d$dias_ult_extremo)
      }
      p %>% layout(
        title = list(text = titulo, font = list(size = 13)),
        xaxis = list(title = "Fecha"),
        yaxis = list(title = "Rendimiento entre extremos (%)", zeroline = TRUE),
        legend = list(orientation = "v", x = 1.02, y = 1)
      )
    })

    # ---------------- Gráfica: precio con extremos y niveles ----------------
    output$price_plot <- renderPlotly({
      d <- detalle(); req(d)
      tk <- input$det_ticker
      modo <- modo_res()
      px <- d$prices
      piv <- d$pivots[d$pivots$confirmed, ]
      tb <- d$table
      q <- rv$result$params$q_sel
      ref <- if (modo == "max") d$last_min else d$last_max
      desde <- max(min(px$date), min(d$last_max$date, d$last_min$date) - 365 * 2)
      pxs <- px[px$date >= desde, ]
      pivs <- piv[piv$date >= desde, ]
      lv <- tb[tb$Concepto %in% c("Quantil80", "Quantil90", "Quantil99", "Media"), ]
      niveles <- if (modo == "max") lv$PreciosMaximos else lv$PreciosMinimos
      pref <- if (modo == "max") "QMax " else "QMin "
      cols <- c(Quantil80 = "#6c7cff", Quantil90 = "#c084fc", Quantil99 = "#ffcc00",
                Media = if (modo == "max") "#2ecc71" else "#ff6b6b")

      p <- plot_ly() %>%
        add_lines(data = pxs, x = ~date, y = ~close, name = "Precio", line = list(color = "#e0e0e0", width = 1.5)) %>%
        add_markers(data = pivs, x = ~date, y = ~price, name = "Extremos locales",
          marker = list(size = 9, color = ifelse(pivs$type == "min", "#ff6b6b", "#2ecc71")),
          text = ~sprintf("%s %s: $%.2f", ifelse(type == "min", "Mínimo", "Máximo"), format(date, "%Y-%m-%d"), price),
          hoverinfo = "text")
      for (i in seq_len(nrow(lv))) {
        con <- lv$Concepto[i]
        lab <- if (con == "Media") (if (modo == "max") "Media Max" else "Media Min") else sub("Quantil", pref, con)
        es_sel <- con == paste0("Quantil", q)
        p <- p %>% add_segments(x = ref$date, xend = d$date_now, y = niveles[i], yend = niveles[i],
          name = sprintf("Precio %s ($%.2f)", lab, niveles[i]),
          line = list(color = cols[[con]], dash = if (es_sel) "solid" else "dash", width = if (es_sel) 2.5 else 1.2),
          hoverinfo = "name")
      }
      p %>% layout(
        title = list(text = sprintf("%s · Precio actual $%.2f · Último máximo $%.2f (%s) · Último mínimo $%.2f (%s) · Niveles %s desde el %s",
          tk, d$price_now, d$last_max$price, format(d$last_max$date, "%Y-%m-%d"),
          d$last_min$price, format(d$last_min$date, "%Y-%m-%d"),
          trimws(pref), if (modo == "max") "último mínimo" else "último máximo"), font = list(size = 12)),
        xaxis = list(title = "Fecha"),
        yaxis = list(title = if (grepl("\\.MX$", tk)) "Precio (MXN)" else "Precio (USD)"),
        legend = list(orientation = "h", x = 0, y = -0.18)
      )
    })

    # ---------------- Tabla de métricas actuales ----------------
    output$metrics_tbl <- renderDT({
      d <- detalle(); req(d)
      tb <- d$table
      q <- rv$result$params$q_sel
      modo <- modo_res()
      ord <- c(paste0("Quantil", c(40, 45)), "Media", paste0("Quantil", c(50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 99)))
      tb <- tb[match(ord, tb$Concepto), ]
      pos_min <- tb$PosicionActual                       # Precio − Precio Mínimo
      pos_max <- tb$PrecioActual - tb$PreciosMaximos     # Precio − Precio Máximo
      if (modo == "max") {
        # Resistencia estadística más cercana por encima del precio
        cercano <- if (any(pos_max <= 0)) which(pos_max == max(pos_max[pos_max <= 0]))[1] else NA_integer_
      } else {
        # Soporte estadístico más cercano por debajo del precio
        cercano <- if (any(pos_min >= 0)) which(pos_min == min(pos_min[pos_min >= 0]))[1] else NA_integer_
      }

      show <- data.frame(
        Concepto = tb$Concepto,
        Minimos = tb$Minimos, Maximos = tb$Maximos,
        `Precio Ultimo Minimo` = round(tb$PrecioUltimoMinimo, 2),
        `Precio Ultimo Maximo` = round(tb$PrecioUltimoMaximo, 2),
        `Precio Actual` = round(tb$PrecioActual, 2),
        `Precios Minimos` = round(tb$PreciosMinimos, 2),
        `Precios Maximos` = round(tb$PreciosMaximos, 2),
        `Posicion vs Minimo` = round(pos_min, 2),
        `Posicion vs Maximo` = round(pos_max, 2),
        .cercano = seq_len(nrow(tb)) == cercano,
        .sel = tb$Concepto == paste0("Quantil", q),
        check.names = FALSE
      )
      dt <- datatable(
        show, rownames = FALSE, selection = "none",
        options = list(dom = "t", pageLength = 20, ordering = FALSE, scrollX = TRUE,
          columnDefs = list(list(visible = FALSE, targets = c(10, 11))))
      ) %>%
        formatPercentage(c("Minimos", "Maximos"), 1) %>%
        formatStyle("Concepto", ".sel", fontWeight = styleEqual(TRUE, "bold"),
          color = styleEqual(TRUE, "#00d4ff"))
      if (modo == "max") {
        dt %>%
          formatStyle("Posicion vs Maximo", backgroundColor = styleInterval(0, c("transparent", "#7f1d1d"))) %>%
          formatStyle("Posicion vs Maximo", ".cercano", backgroundColor = styleEqual(TRUE, "#3f9e3f")) %>%
          formatStyle("Precios Maximos", ".cercano", backgroundColor = styleEqual(TRUE, "#c0392b"))
      } else {
        dt %>%
          formatStyle("Posicion vs Minimo", backgroundColor = styleInterval(0, c("#7f1d1d", "transparent"))) %>%
          formatStyle("Posicion vs Minimo", ".cercano", backgroundColor = styleEqual(TRUE, "#3f9e3f")) %>%
          formatStyle("Precios Minimos", ".cercano", backgroundColor = styleEqual(TRUE, "#c0392b"))
      }
    })

    # ---------------- Exportar Excel ----------------
    output$dl_excel <- downloadHandler(
      filename = function() {
        sprintf("extremos_locales_%s_%s.xlsx", if (modo_res() == "max") "maximos" else "minimos", format(Sys.Date(), "%Y%m%d"))
      },
      content = function(file) {
        req(can_analyze(), rv$result)
        r <- rv$result
        wb <- openxlsx::createWorkbook()
        add <- function(nm, df) {
          openxlsx::addWorksheet(wb, nm)
          openxlsx::writeData(wb, nm, if (is.null(df) || nrow(df) == 0) data.frame(Mensaje = "Sin datos") else df)
        }
        if (is.null(r)) {
          add("Candidatos", NULL)
        } else {
          mi <- ext_modo_info(modo_res())
          ren <- function(df) {
            if (is.null(df) || !nrow(df)) return(df)
            lbl_q <- sprintf("%s%d", mi$Q, r$params$q_sel)
            names(df) <- c("Ticker", "Precio Actual", mi$ref, mi$fref, mi$mov, lbl_q, paste("Precio", lbl_q),
              "Distancia al Nivel", mi$nivel, mi$media, mi$n, "Días Últ. Extremo", "Tramo")
            df
          }
          add(sprintf("%s %s%d", if (modo_res() == "max") "Sobre" else "Bajo", mi$Q, r$params$q_sel), ren(r$candidatos))
          add("Todas", ren(r$todos))
          if (!is.null(input$det_ticker) && !is.null(r$details[[input$det_ticker]])) {
            add(paste0("Metricas_", substr(input$det_ticker, 1, 20)), r$details[[input$det_ticker]]$table)
          }
        }
        openxlsx::saveWorkbook(wb, file, overwrite = TRUE)
      }
    )
  })
}

# =========================================================
# Guía de uso e interpretación — Extremos Locales (QMin / QMax)
# =========================================================
extremos_guide_ui <- function() {
  h4s <- "color: #ffcc66; font-weight: bold; margin-top: 6px;"
  # Misma lógica de color que los botones: rojo = mínimos, verde = máximos
  c_min <- "color:#ff6b6b;font-weight:bold;"
  c_max <- "color:#2ecc71;font-weight:bold;"
  div(
    class = "card p-4 mb-4",
    style = "max-width: 920px; margin: 0 auto; font-size: 1.05em;",
    h3(style = "color: #0f62fe; font-weight: bold; margin-bottom: 20px;",
      "Guía de Uso e Interpretación — Extremos Locales (QMin / QMax)"),
    p("Este módulo compara el movimiento actual de cada empresa contra sus propios movimientos históricos y responde dos preguntas:"),
    tags$ul(
      tags$li(HTML(sprintf("<span style='%s'>▼ Mínimos locales:</span> ¿la <b>caída</b> actual ya es más grande que la mayoría de sus caídas históricas? → posible <b>zona de rebote</b>.", c_min))),
      tags$li(HTML(sprintf("<span style='%s'>▲ Máximos locales:</span> ¿la <b>subida</b> actual ya es más grande que la mayoría de sus subidas históricas? → posible <b>zona de techo</b> o de toma de utilidades.", c_max)))
    ),
    div(class = "alert alert-secondary",
      HTML("<b>En una frase:</b> el botón <b style='color:#ff6b6b;'>rojo</b> busca acciones que <b>cayeron de más</b> (mínimos locales); el botón <b style='color:#2ecc71;'>verde</b> busca acciones que <b>subieron de más</b> (máximos locales), siempre comparadas contra su propia historia.")),
    hr(),

    h4(style = h4s, "1. ¿Qué son los extremos locales?"),
    p("Son los picos (máximos) y valles (mínimos) importantes del precio. Se detectan con un ",
      strong("zigzag"), ": un máximo se confirma cuando el precio cae al menos X% desde él, y un mínimo se confirma cuando el precio sube al menos X% desde él."),
    tags$ul(
      tags$li(HTML("<b>Cada tramo máximo → mínimo es una caída</b> (rendimiento negativo).")),
      tags$li(HTML("<b>Cada tramo mínimo → máximo es una subida</b> (rendimiento positivo).")),
      tags$li(HTML("Con toda la historia de la acción (desde 1990 si existe) se forma la <b>serie de rendimientos entre extremos</b>, la línea gris de la primera gráfica."))
    ),
    br(),

    h4(style = h4s, "2. ¿Qué son el QMin y el QMax?"),
    tags$ul(
      tags$li(HTML("<b>QMin P</b> (cuantil de caídas): la caída que solo el (100−P)% de las caídas históricas ha superado. Ejemplo: <b>QMin90 = −45%</b> → el 90% de las caídas de esa empresa fueron menores a 45%.")),
      tags$li(HTML("<b>QMax P</b> (cuantil de subidas): la subida que solo el (100−P)% de las subidas históricas ha superado. Ejemplo: <b>QMax90 = +133%</b> → el 90% de sus subidas fueron menores a +133%.")),
      tags$li(HTML("<b>Media Min / Media Max</b>: la caída y la subida promedio entre extremos."))
    ),
    br(),

    h4(style = h4s, "3. Cómo se convierten en precios"),
    tags$ul(
      tags$li(HTML("<b>Precio Mínimo P</b> = Precio Último Máximo × (1 + QMin P). Precio al que la caída actual igualaría al cuantil P.")),
      tags$li(HTML("<b>Precio Máximo P</b> = Precio Último Mínimo × (1 + QMax P). Precio al que la subida actual igualaría al cuantil P.")),
      tags$li(HTML("<b>Posición vs Mínimo</b> = Precio Actual − Precio Mínimo P. <span style='color:#c0392b;font-weight:bold;'>Negativa</span>: ya perforó ese nivel hacia abajo.")),
      tags$li(HTML("<b>Posición vs Máximo</b> = Precio Actual − Precio Máximo P. <span style='color:#c0392b;font-weight:bold;'>Positiva</span>: ya superó ese nivel hacia arriba."))
    ),
    div(class = "alert alert-secondary",
      HTML("<b>Ejemplo de mínimos (INTC):</b> Último máximo $127.39 y QMin90 = −45.0% → Precio QMin90 = 127.39 × (1 − 0.45) = <b>$70.13</b>.
      Con precio de $58.27 está <b>por debajo</b>: su caída (−54.3%) es mayor que el 90% de sus caídas → aparece en la búsqueda de <b>MÍNIMOS</b>.<br><br>
      <b>Ejemplo de máximos:</b> si el último mínimo fue $20 y el QMax90 = +150% → Precio QMax90 = 20 × 2.5 = <b>$50</b>.
      Si la acción cotiza en $55 está <b>por encima</b>: su subida (+175%) es mayor que el 90% de sus subidas → aparece en la búsqueda de <b>MÁXIMOS</b>.")),
    br(),

    h4(style = h4s, "4. Cómo usar el módulo (paso a paso)"),
    tags$ol(
      tags$li(HTML("Escribe o pega tus tickers, o agrégalos con el campo <b>Agregar tickers</b>. Usa <b>Limpiar lista</b> para empezar de cero.")),
      tags$li(HTML("Elige el <b>Nivel de cuantil</b> (90 recomendado). Aplica como QMin en la búsqueda de mínimos y como QMax en la de máximos.")),
      tags$li(HTML(sprintf("Da clic en <span style='%s'>Buscar empresas en MÍNIMOS locales</span> o en <span style='%s'>Buscar empresas en MÁXIMOS locales</span>.", c_min, c_max))),
      tags$li(HTML("Espera a que la barra llegue a 100% y aparezca <b>“Proceso concluido con éxito”</b>. Si algo falla verás el error en rojo.")),
      tags$li(HTML("Revisa la tabla de resultados y da clic en una fila para ver su detalle (gráficas y métricas)."))
    ),
    br(),

    h4(style = h4s, "5. Parámetros"),
    tags$ul(
      tags$li(HTML("<b>Nivel de cuantil</b>: 80 da más resultados; 95 o 99 solo muestra casos excepcionales.")),
      tags$li(HTML("<b>Sensibilidad (zigzag)</b>: 15% detecta más oscilaciones (ciclos cortos); 25–30% solo ciclos grandes. <b>Recomendado: 20%.</b> Si la cambias, cambian los cuantiles.")),
      tags$li(HTML("<b>Mínimo de tramos históricos</b>: cuántas caídas (búsqueda de mínimos) o subidas (búsqueda de máximos) debe tener la empresa para que la estadística sea confiable. Con menos de 5 es débil.")),
      tags$li(HTML("<b>Datos de precios</b>: 'Usar caché' reutiliza descargas de las últimas 12 h (más rápido). 'Descargar frescos' fuerza datos nuevos."))
    ),
    br(),

    h4(style = h4s, "6. Cómo leer los resultados"),
    tags$ul(
      tags$li(HTML("<b>Tabla de empresas</b>: ordenada por <b>Distancia al Nivel</b>, de la más extrema a la menos extrema. En mínimos, más negativa = más hundida bajo el QMin. En máximos, más positiva = más estirada sobre el QMax.")),
      tags$li(HTML("<b>Nivel Perforado / Nivel Superado</b>: el cuantil más extremo que ya rompió (por ejemplo QMin95 o QMax99).")),
      tags$li(HTML("<b>Tramo</b>: 'Bajista' = el último extremo confirmado fue un máximo y el precio va a la baja; 'Alcista' = el último confirmado fue un mínimo y el precio va al alza.")),
      tags$li(HTML("<b>Días Últ. Extremo</b>: días desde el último máximo o mínimo confirmado.")),
      tags$li(HTML("<b>Gráfica de rendimientos</b>: puntos <span style='color:#ff6b6b;'>rojos</span> = mínimos (caídas), <span style='color:#2ecc71;'>verdes</span> = máximos (subidas); el <b>rombo azul</b> es el movimiento actual. En mínimos, si queda bajo las líneas QMin, está en zona extrema; en máximos, si queda sobre las líneas QMax, está en zona extrema.")),
      tags$li(HTML("<b>Gráfica de precio</b>: muestra los extremos recientes y las líneas de precio QMin (desde el último máximo) o QMax (desde el último mínimo). La línea sólida es el nivel elegido.")),
      tags$li(HTML("<b>Métricas actuales</b>: la celda <span style='background:#3f9e3f;color:#fff;padding:0 4px;'>verde</span> marca el siguiente nivel estadístico. En mínimos es el soporte más cercano por debajo del precio; en máximos, la resistencia más cercana por encima. Las celdas <span style='background:#7f1d1d;color:#fff;padding:0 4px;'>rojo oscuro</span> son niveles que el precio ya rompió."))
    ),
    br(),

    h4(style = h4s, "7. Cómo interpretar cada búsqueda"),
    fluidRow(
      column(6, div(class = "p-3", style = "border:1px solid #ff6b6b; border-radius:12px; height:100%;",
        h5(style = c_min, "▼ Empresas en MÍNIMOS locales"),
        tags$ul(
          tags$li("Indica sobreventa estadística: cayó más que casi siempre."),
          tags$li("Úsalo para encontrar candidatas a compra o rebote."),
          tags$li("Confirma la calidad en el Dashboard Analítico (evita deterioro fundamental)."),
          tags$li("Espera una señal de giro: Tramo 'Alcista', cierre arriba del máximo previo o volumen comprador."),
          tags$li("Stop: debajo del siguiente nivel QMin (QMin95/QMin99). Objetivo: Media Max o QMax desde el mínimo.")
        )
      )),
      column(6, div(class = "p-3", style = "border:1px solid #2ecc71; border-radius:12px; height:100%;",
        h5(style = c_max, "▲ Empresas en MÁXIMOS locales"),
        tags$ul(
          tags$li("Indica sobrecompra estadística: subió más que casi siempre."),
          tags$li("Úsalo para tomar utilidades, subir el stop o evitar compras tardías."),
          tags$li("No es señal de venta en corto por sí sola: una tendencia fuerte puede seguir."),
          tags$li("Espera una señal de agotamiento: Tramo 'Bajista', cierre bajo el mínimo previo o volumen vendedor."),
          tags$li("Si ya tienes la acción: protege la ganancia con un stop debajo del último soporte.")
        )
      ))
    ),
    br(),

    div(
      class = "alert alert-primary mt-3",
      style = "font-size: 1.05em;",
      HTML("<b>💡 Regla de oro:</b> estar bajo el QMin90 o sobre el QMax90 indica que el movimiento es <b>estadísticamente extremo</b>. No garantiza que ya tocó fondo o techo.
      Úsalo como <b>filtro de oportunidades y de riesgo</b>, y combínalo con fundamentales y una confirmación técnica antes de operar.
      Los cuantiles se basan en el pasado: un cambio estructural en la empresa (fraude, quiebra, un producto disruptivo) puede generar movimientos mayores que cualquiera de su historia.")
    )
  )
}
