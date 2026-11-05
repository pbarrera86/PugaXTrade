# R/extremos_module.R
# =========================================================
# Módulo: Screener de Extremos Locales (QMin / QMax)
# Se muestra como pestaña dentro de "Análisis Avanzado".
# Lógica de cálculo en R/extremos_core.R
# =========================================================

extremos_ui <- function(id) {
  ns <- NS(id)
  tagList(
    br(),
    div(
      class = "card p-3 mb-4",
      h4("Screener de Extremos Locales"),
      p(
        class = "text-muted", style = "margin-bottom:12px;",
        "Detecta empresas cuyo precio actual está por debajo del nivel QMin elegido: ",
        "la caída desde su último máximo ya es mayor que la de la mayoría de sus caídas históricas ",
        "entre extremos locales. Es una zona de posible rebote o cambio de tendencia al alza."
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
        value = if (exists("inst_default_tickers_text", mode = "function")) inst_default_tickers_text() else "NVO, AAPL, MSFT",
        rows = 4, width = "100%"
      ),
      uiOutput(ns("count_ui")),
      fluidRow(
        column(3, selectInput(ns("q_sel"), "Nivel QMin a perforar",
          choices = c("QMin 80" = 80, "QMin 85" = 85, "QMin 90" = 90, "QMin 95" = 95, "QMin 99" = 99),
          selected = 90
        )),
        column(3, selectInput(ns("umbral"), "Sensibilidad de extremos (zigzag)",
          choices = c("15% (más extremos)" = 0.15, "20% (recomendado)" = 0.20, "25%" = 0.25, "30% (solo grandes ciclos)" = 0.30),
          selected = 0.20
        )),
        column(3, numericInput(ns("min_caidas"), "Mínimo de caídas históricas", value = 5, min = 2, max = 50, step = 1)),
        column(3, selectInput(ns("cache_h"), "Datos de precios",
          choices = c("Usar caché (12 h)" = 12, "Descargar frescos" = 0),
          selected = 12
        ))
      ),
      fluidRow(
        column(4, actionButton(ns("run_btn"), tagList(icon("play"), "Buscar empresas"), class = "btn btn-success w-100 mb-2")),
        column(4, actionButton(ns("reset_btn"), tagList(icon("undo"), "Restaurar tickers"), class = "btn btn-warning w-100 mb-2")),
        column(4, downloadButton(ns("dl_excel"), "Exportar Excel", class = "btn btn-primary w-100 mb-2"))
      ),
      uiOutput(ns("progress_ui"))
    ),
    div(
      class = "card p-3 mb-4",
      h4("Empresas que cumplen la condición"),
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

extremos_server <- function(id, can_analyze = reactive(TRUE)) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    rv <- reactiveValues(
      result = NULL, running = FALSE, prog_file = NULL,
      status = "idle", # idle | running | ok | error
      msg = NULL, n_total = 0L, started = NULL
    )

    # Controles que se bloquean mientras corre el proceso
    ctrl_ids <- c("run_btn", "reset_btn", "dl_excel", "add_btn", "clear_btn", "add_input",
                  "tickers", "q_sel", "umbral", "min_caidas", "cache_h")
    set_busy <- function(busy) {
      for (i in ctrl_ids) {
        if (busy) shinyjs::disable(i) else shinyjs::enable(i)
      }
      if (busy) {
        shinyjs::html("run_btn", as.character(tagList(icon("spinner", class = "fa-spin"), " Procesando...")))
      } else {
        shinyjs::html("run_btn", as.character(tagList(icon("play"), " Buscar empresas")))
      }
    }

    parse_ticks <- function(txt) {
      x <- unlist(strsplit(toupper(txt %||% ""), "[,;\\s]+", perl = TRUE))
      x <- gsub("[^A-Z0-9.\\-^=]", "", x)
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
    ext_worker_job <- function(app_dir, ticks, umbral, q_sel, min_caidas, cache_h, prog_file) {
      setwd(app_dir)
      suppressWarnings(suppressMessages(library(quantmod)))
      source(file.path(app_dir, "R", "extremos_core.R"), local = TRUE)
      ext_run_screener(
        ticks,
        umbral = umbral, q_sel = q_sel, min_caidas = min_caidas,
        max_age_hours = cache_h,
        progress_cb = function(k, n, tk) {
          try(writeLines(sprintf("%d|%d|%s", k, n, tk), prog_file), silent = TRUE)
        }
      )
    }
    use_mirai <- requireNamespace("mirai", quietly = TRUE)
    message("[EXT] Worker en segundo plano: ", if (use_mirai) "mirai (proceso propio)" else "future")

    ext_task <- tryCatch(
      shiny::ExtendedTask$new(function(ticks, umbral, q_sel, min_caidas, cache_h, prog_file) {
        app_dir <- normalizePath(getwd())
        if (use_mirai) {
          m <- mirai::mirai(
            job(app_dir, ticks, umbral, q_sel, min_caidas, cache_h, prog_file),
            job = ext_worker_job, app_dir = app_dir, ticks = ticks, umbral = umbral,
            q_sel = q_sel, min_caidas = min_caidas, cache_h = cache_h, prog_file = prog_file
          )
          promises::as.promise(m)
        } else {
          promises::future_promise(
            ext_worker_job(app_dir, ticks, umbral, q_sel, min_caidas, cache_h, prog_file),
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
      if (exists("inst_default_tickers_text", mode = "function")) {
        updateTextAreaInput(session, "tickers", value = inst_default_tickers_text())
      }
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
        rv$status <- "ok"
        seg <- if (!is.null(rv$started)) round(as.numeric(difftime(Sys.time(), rv$started, units = "secs"))) else NA
        rv$msg <- sprintf(
          "Proceso concluido con éxito. %d empresas analizadas, %d cumplen la condición%s%s.",
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
      rv$status <- "error"
      rv$msg <- paste("Ocurrió un error durante el proceso:", err)
      showNotification(rv$msg, type = "error", duration = 10)
      set_busy(FALSE)
    }

    observeEvent(input$run_btn, {
      if (isTRUE(rv$running)) return()
      if (!isTRUE(can_analyze())) {
        showNotification("Tu prueba finalizó o no tienes membresía activa.", type = "warning")
        return()
      }
      ticks <- parse_ticks(input$tickers)
      if (length(ticks) == 0) {
        showNotification("Ingresa al menos un ticker válido.", type = "warning")
        return()
      }
      p <- params()
      rv$prog_file <- tempfile("ext_prog_", fileext = ".txt")
      writeLines(sprintf("0|%d|", length(ticks)), rv$prog_file)
      rv$n_total <- length(ticks)
      rv$started <- Sys.time()
      rv$running <- TRUE
      rv$status <- "running"
      rv$msg <- NULL
      set_busy(TRUE)

      if (!is.null(ext_task)) {
        ok <- tryCatch({
          ext_task$invoke(ticks, p$umbral, p$q_sel, p$min_caidas, p$cache_h, rv$prog_file)
          TRUE
        }, error = function(e) {
          message("[EXT] invoke() falló: ", e$message)
          FALSE
        })
        if (ok) return()
      }

      # Fallback sincrónico (si no hay workers en segundo plano)
      tryCatch({
        res <- withProgress(message = "Analizando extremos locales...", value = 0, {
          ext_run_screener(
            ticks, umbral = p$umbral, q_sel = p$q_sel, min_caidas = p$min_caidas,
            max_age_hours = p$cache_h,
            progress_cb = function(k, n, tk) incProgress(1 / n, detail = tk)
          )
        })
        finish_ok(res)
      }, error = function(e) finish_error(conditionMessage(e)))
    })

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
        return(tagList(
          div(style = "font-weight:600; margin-top:10px;",
            icon("spinner", class = "fa-spin"),
            if (nzchar(tk)) sprintf(" Procesando %s (%d de %d) · %d s", tk, k, n, seg) else sprintf(" Preparando %d tickers...", n)),
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
    tabla_actual <- reactive({
      req(rv$result)
      if (isTRUE(input$ver_todos)) rv$result$todos else rv$result$candidatos
    })

    output$resumen_ui <- renderUI({
      req(rv$result)
      r <- rv$result
      n_ok <- nrow(r$todos)
      n_c <- nrow(r$candidatos)
      tagList(
        div(
          class = "alert alert-info", style = "margin-bottom:10px;",
          HTML(sprintf(
            "<b>%d</b> de <b>%d</b> empresas están por debajo de su <b>QMin %d</b> (zigzag %d%%, mínimo %d caídas históricas).%s",
            n_c, n_ok, r$params$q_sel, round(100 * r$params$umbral), r$params$min_caidas,
            if (length(r$fallidos)) sprintf(" Sin datos suficientes: %s.", paste(head(r$fallidos, 15), collapse = ", ")) else ""
          ))
        )
      )
    })

    output$cand_tbl <- renderDT({
      df <- tabla_actual()
      q <- rv$result$params$q_sel
      if (is.null(df) || nrow(df) == 0) {
        return(datatable(data.frame(Mensaje = "Ninguna empresa cumple la condición con estos parámetros."),
          rownames = FALSE, options = list(dom = "t")))
      }
      show <- data.frame(
        Ticker = df$Ticker,
        `Precio Actual` = round(df$PrecioActual, 2),
        `Último Máximo` = round(df$UltimoMaximo, 2),
        `Fecha Últ. Máx.` = format(df$FechaUltMax, "%Y-%m-%d"),
        `Caída Actual` = df$CaidaActual,
        QMinSel = df$QMinSel,
        PrecioQ = round(df$PrecioQMinSel, 2),
        `Distancia al Nivel` = df$DistanciaPct,
        `Nivel Perforado` = df$NivelPerforado,
        `Media Caídas` = df$QMedia,
        `# Caídas` = df$NumCaidas,
        `Días Últ. Extremo` = df$DiasUltExtremo,
        Tramo = df$Tramo,
        check.names = FALSE
      )
      names(show)[names(show) == "QMinSel"] <- sprintf("QMin%d", q)
      names(show)[names(show) == "PrecioQ"] <- sprintf("Precio QMin%d", q)

      datatable(
        show,
        rownames = FALSE, selection = "single",
        options = list(scrollX = TRUE, pageLength = 15, order = list(list(7, "asc")))
      ) %>%
        formatPercentage(c("Caída Actual", sprintf("QMin%d", q), "Distancia al Nivel", "Media Caídas"), 1) %>%
        formatStyle("Distancia al Nivel",
          color = styleInterval(0, c("#ff6b6b", "#2ecc71")), fontWeight = "bold"
        ) %>%
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
      piv <- d$pivots
      tb <- d$table
      g <- function(con, col) tb[[col]][tb$Concepto == con]

      serie <- data.frame(date = piv$date, ret = ifelse(is.na(piv$ret), 0, piv$ret) * 100, type = piv$type, conf = piv$confirmed)
      serie_c <- serie[serie$conf, ]
      # Punto actual: caída desde el último máximo confirmado
      act <- data.frame(date = d$date_now, ret = d$caida_actual * 100)
      x0 <- min(serie$date); x1 <- d$date_now

      niveles <- list(
        list("QMin 80", g("Quantil80", "Minimos"), "#6c7cff", "dash"),
        list("QMin 90", g("Quantil90", "Minimos"), "#2ecc71", "dash"),
        list("QMin 99", g("Quantil99", "Minimos"), "#ffcc00", "dash"),
        list("Media Min", g("Media", "Minimos"), "#ff6b6b", "dot"),
        list("QMax 80", g("Quantil80", "Maximos"), "#6c7cff", "dash"),
        list("QMax 90", g("Quantil90", "Maximos"), "#2ecc71", "dash"),
        list("QMax 99", g("Quantil99", "Maximos"), "#ffcc00", "dash"),
        list("Media Max", g("Media", "Maximos"), "#ff6b6b", "dot")
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
          text = ~sprintf("Actual: %.1f%% desde último máximo", ret), hoverinfo = "text")
      for (nv in niveles) {
        p <- p %>% add_segments(x = x0, xend = x1, y = nv[[2]] * 100, yend = nv[[2]] * 100,
          name = sprintf("%s (%.1f%%)", nv[[1]], nv[[2]] * 100),
          line = list(color = nv[[3]], dash = nv[[4]], width = 1.2), hoverinfo = "name")
      }
      p %>% layout(
        title = list(text = sprintf(
          "%s · QMin80: %.1f%% · QMin90: %.1f%% · MediaMin: %.1f%% · Actual: %.1f%% · MediaMax: %.1f%% · DíasUExtremo: %d",
          tk, g("Quantil80", "Minimos") * 100, g("Quantil90", "Minimos") * 100, g("Media", "Minimos") * 100,
          d$caida_actual * 100, g("Media", "Maximos") * 100, d$dias_ult_extremo
        ), font = list(size = 13)),
        xaxis = list(title = "Fecha"),
        yaxis = list(title = "Rendimiento entre extremos (%)", zeroline = TRUE),
        legend = list(orientation = "v", x = 1.02, y = 1)
      )
    })

    # ---------------- Gráfica: precio con extremos y niveles ----------------
    output$price_plot <- renderPlotly({
      d <- detalle(); req(d)
      tk <- input$det_ticker
      px <- d$prices
      piv <- d$pivots[d$pivots$confirmed, ]
      tb <- d$table
      q <- rv$result$params$q_sel
      desde <- max(min(px$date), d$last_max$date - 365 * 3)
      pxs <- px[px$date >= desde, ]
      pivs <- piv[piv$date >= desde, ]
      lv <- tb[tb$Concepto %in% c("Quantil80", "Quantil90", "Quantil99", "Media"), ]
      cols <- c(Quantil80 = "#6c7cff", Quantil90 = "#2ecc71", Quantil99 = "#ffcc00", Media = "#ff6b6b")

      p <- plot_ly() %>%
        add_lines(data = pxs, x = ~date, y = ~close, name = "Precio", line = list(color = "#e0e0e0", width = 1.5)) %>%
        add_markers(data = pivs, x = ~date, y = ~price, name = "Extremos locales",
          marker = list(size = 9, color = ifelse(pivs$type == "min", "#ff6b6b", "#2ecc71")),
          text = ~sprintf("%s %s: $%.2f", ifelse(type == "min", "Mínimo", "Máximo"), format(date, "%Y-%m-%d"), price),
          hoverinfo = "text")
      for (i in seq_len(nrow(lv))) {
        con <- lv$Concepto[i]
        lab <- if (con == "Media") "Media Min" else sub("Quantil", "QMin ", con)
        p <- p %>% add_segments(x = d$last_max$date, xend = d$date_now, y = lv$PreciosMinimos[i], yend = lv$PreciosMinimos[i],
          name = sprintf("Precio %s ($%.2f)", lab, lv$PreciosMinimos[i]),
          line = list(color = cols[[con]], dash = if (con == paste0("Quantil", q)) "solid" else "dash", width = if (con == paste0("Quantil", q)) 2.5 else 1.2),
          hoverinfo = "name")
      }
      p %>% layout(
        title = list(text = sprintf("%s · Precio actual $%.2f · Último máximo $%.2f (%s) · Último mínimo $%.2f",
          tk, d$price_now, d$last_max$price, format(d$last_max$date, "%Y-%m-%d"), d$last_min$price), font = list(size = 13)),
        xaxis = list(title = "Fecha"),
        yaxis = list(title = "Precio (USD)"),
        legend = list(orientation = "h", x = 0, y = -0.18)
      )
    })

    # ---------------- Tabla de métricas actuales ----------------
    output$metrics_tbl <- renderDT({
      d <- detalle(); req(d)
      tb <- d$table
      q <- rv$result$params$q_sel
      ord <- c(paste0("Quantil", c(40, 45)), "Media", paste0("Quantil", c(50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 99)))
      tb <- tb[match(ord, tb$Concepto), ]
      # Nivel más cercano por debajo del precio (posición positiva mínima)
      pos <- tb$PosicionActual
      cercano <- if (any(pos >= 0)) which(pos == min(pos[pos >= 0]))[1] else NA_integer_

      show <- data.frame(
        Concepto = tb$Concepto,
        Minimos = tb$Minimos, Maximos = tb$Maximos,
        `Precio Ultimo Minimo` = round(tb$PrecioUltimoMinimo, 2),
        `Precio Ultimo Maximo` = round(tb$PrecioUltimoMaximo, 2),
        `Precio Actual` = round(tb$PrecioActual, 2),
        `Precios Minimos` = round(tb$PreciosMinimos, 2),
        `Precios Maximos` = round(tb$PreciosMaximos, 2),
        `Posicion Actual` = round(tb$PosicionActual, 2),
        .cercano = seq_len(nrow(tb)) == cercano,
        .sel = tb$Concepto == paste0("Quantil", q),
        check.names = FALSE
      )
      datatable(
        show, rownames = FALSE, selection = "none",
        options = list(dom = "t", pageLength = 20, ordering = FALSE, scrollX = TRUE,
          columnDefs = list(list(visible = FALSE, targets = c(9, 10))))
      ) %>%
        formatPercentage(c("Minimos", "Maximos"), 1) %>%
        formatStyle("Posicion Actual",
          backgroundColor = styleInterval(0, c("#7f1d1d", "transparent"))) %>%
        formatStyle("Posicion Actual", ".cercano",
          backgroundColor = styleEqual(TRUE, "#3f9e3f")) %>%
        formatStyle("Precios Minimos", ".cercano",
          backgroundColor = styleEqual(TRUE, "#c0392b")) %>%
        formatStyle("Concepto", ".sel", fontWeight = styleEqual(TRUE, "bold"),
          color = styleEqual(TRUE, "#00d4ff"))
    })

    # ---------------- Exportar Excel ----------------
    output$dl_excel <- downloadHandler(
      filename = function() sprintf("extremos_locales_%s.xlsx", format(Sys.Date(), "%Y%m%d")),
      content = function(file) {
        r <- rv$result
        wb <- openxlsx::createWorkbook()
        add <- function(nm, df) {
          openxlsx::addWorksheet(wb, nm)
          openxlsx::writeData(wb, nm, if (is.null(df) || nrow(df) == 0) data.frame(Mensaje = "Sin datos") else df)
        }
        if (is.null(r)) {
          add("Candidatos", NULL)
        } else {
          add(sprintf("Bajo QMin%d", r$params$q_sel), r$candidatos)
          add("Todas", r$todos)
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
# Guía de uso e interpretación — Extremos Locales (QMin)
# =========================================================
extremos_guide_ui <- function() {
  h4s <- "color: #0f172a; font-weight: bold;"
  div(
    class = "card p-4 mb-4",
    style = "max-width: 900px; margin: 0 auto; font-size: 1.05em;",
    h3(style = "color: #0f62fe; font-weight: bold; margin-bottom: 20px;",
      "Guía de Uso e Interpretación — Extremos Locales (QMin)"),
    p("Este módulo responde una pregunta concreta: ",
      strong("¿la caída actual de una empresa ya es más grande que la mayoría de las caídas que ha tenido en su historia?"),
      " Si la respuesta es sí, la acción está en una zona estadísticamente extrema, donde históricamente suelen aparecer rebotes o cambios de tendencia al alza."),
    hr(),

    h4(style = h4s, "1. ¿Qué son los extremos locales?"),
    p("Son los picos (máximos) y valles (mínimos) importantes del precio. Se detectan con un ",
      strong("zigzag"), ": un máximo se confirma cuando el precio cae al menos X% desde él, y un mínimo se confirma cuando el precio sube al menos X% desde él."),
    tags$ul(
      tags$li(HTML("<b>Cada tramo máximo → mínimo es una caída</b> (rendimiento negativo).")),
      tags$li(HTML("<b>Cada tramo mínimo → máximo es una subida</b> (rendimiento positivo).")),
      tags$li(HTML("Con toda la historia de la acción (desde 1990 si existe) se forma la <b>serie de rendimientos entre extremos</b>, que es la línea gris de la primera gráfica."))
    ),
    br(),

    h4(style = h4s, "2. ¿Qué es el QMin y el QMax?"),
    tags$ul(
      tags$li(HTML("<b>QMin P</b> (cuantil de mínimos): la caída que solo el (100−P)% de las caídas históricas ha superado. Ejemplo: <b>QMin90 = −45%</b> significa que el 90% de las caídas de esa empresa fueron menores a 45% y solo el 10% fue más profunda.")),
      tags$li(HTML("<b>QMax P</b> (cuantil de máximos): lo mismo para las subidas. <b>QMax80 = +100%</b> significa que el 80% de los rebotes fueron menores a +100%.")),
      tags$li(HTML("<b>Media Min / Media Max</b>: la caída y la subida promedio entre extremos."))
    ),
    br(),

    h4(style = h4s, "3. Cómo se convierten en precios"),
    tags$ul(
      tags$li(HTML("<b>Precio Mínimo P</b> = Precio Último Máximo × (1 + QMin P). Es el precio al que la caída actual igualaría al cuantil P.")),
      tags$li(HTML("<b>Precio Máximo P</b> = Precio Último Mínimo × (1 + QMax P). Es un objetivo estadístico de subida desde el último valle.")),
      tags$li(HTML("<b>Posición Actual</b> = Precio Actual − Precio Mínimo P. <span style='color:#c0392b;font-weight:bold;'>Negativa</span>: el precio ya perforó ese nivel. <span style='color:#2e7d32;font-weight:bold;'>Positiva</span>: aún está por encima."))
    ),
    div(class = "alert alert-secondary",
      HTML("<b>Ejemplo (INTC):</b> Último máximo $127.39 y QMin90 = −45.0% → Precio QMin90 = 127.39 × (1 − 0.45) = <b>$70.13</b>.
      Con precio actual de $58.27, la Posición Actual es −11.86 → <b>ya perforó el QMin90</b> (y también el QMin95 de $61.36).
      Su caída actual (−54.3%) es mayor que el 95% de sus caídas históricas.")),
    br(),

    h4(style = h4s, "4. Parámetros del screener"),
    tags$ul(
      tags$li(HTML("<b>Nivel QMin a perforar</b>: la condición del filtro. QMin80 da más resultados (caídas grandes); QMin95 o QMin99 solo muestra caídas excepcionales. <b>Recomendado: QMin90.</b>")),
      tags$li(HTML("<b>Sensibilidad (zigzag)</b>: 15% detecta más oscilaciones (ciclos cortos); 25–30% solo ciclos grandes. <b>Recomendado: 20%.</b> Si cambias la sensibilidad, cambian los cuantiles.")),
      tags$li(HTML("<b>Mínimo de caídas históricas</b>: exige suficiente historia para que el cuantil sea confiable. Con menos de 5 caídas la estadística es débil.")),
      tags$li(HTML("<b>Datos de precios</b>: 'Usar caché' reutiliza descargas de las últimas 12 h (más rápido). 'Descargar frescos' fuerza datos nuevos."))
    ),
    br(),

    h4(style = h4s, "5. Cómo leer los resultados"),
    tags$ul(
      tags$li(HTML("<b>Tabla de empresas</b>: ordenada por <b>Distancia al Nivel</b> (más negativa = más hundida bajo el QMin). <b>Nivel Perforado</b> indica el cuantil más profundo que ya rompió (QMin95, QMin99…).")),
      tags$li(HTML("<b>Tramo</b>: 'Bajista' = la caída sigue en curso; 'Alcista' = ya rebotó al menos el % del zigzag desde el último mínimo.")),
      tags$li(HTML("<b>Días Últ. Extremo</b>: días desde el último máximo/mínimo confirmado. Caídas muy recientes y profundas suelen ser más violentas; caídas largas suelen indicar capitulación.")),
      tags$li(HTML("<b>Gráfica de rendimientos</b>: puntos rojos = caídas, verdes = subidas; el rombo azul es la caída actual. Si el rombo queda por debajo de las líneas QMin, está en zona extrema.")),
      tags$li(HTML("<b>Gráfica de precio</b>: muestra los extremos recientes y las líneas de precio QMin80/90/99 proyectadas desde el último máximo (la línea sólida es el nivel elegido).")),
      tags$li(HTML("<b>Métricas actuales</b>: la celda <span style='background:#3f9e3f;color:#fff;padding:0 4px;'>verde</span> marca el soporte estadístico más cercano por debajo del precio (el siguiente nivel que podría tocar)."))
    ),
    br(),

    h4(style = h4s, "6. Flujo recomendado"),
    tags$ol(
      tags$li("Ejecuta el screener con QMin90 y zigzag 20% sobre tu lista."),
      tags$li("Prioriza empresas con más de 10 caídas históricas y que perforen QMin90 o más."),
      tags$li("Confirma la calidad en el Dashboard Analítico: evita las de semáforo rojo por deterioro fundamental (una caída extrema puede ser justificada)."),
      tags$li("Espera una señal de giro: que el Tramo cambie a 'Alcista', un cierre arriba del máximo del día anterior o un repunte con volumen."),
      tags$li("Define tu stop debajo del siguiente nivel QMin (por ejemplo QMin95 o QMin99) y tu objetivo con la Media Max o QMax del último mínimo.")
    ),
    br(),

    div(
      class = "alert alert-primary mt-3",
      style = "font-size: 1.05em;",
      HTML("<b>💡 Regla de oro:</b> estar bajo el QMin90 indica que la caída es <b>estadísticamente extrema</b>, no que ya tocó fondo.
      Úsalo como <b>filtro de oportunidades</b> y combínalo con fundamentales y una confirmación técnica antes de comprar.
      Los cuantiles se basan en el pasado: un cambio estructural en la empresa (fraude, quiebra, pérdida de negocio) puede generar caídas mayores que cualquiera de su historia.")
    )
  )
}
