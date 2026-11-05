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
    ext_task <- tryCatch(
      shiny::ExtendedTask$new(function(ticks, umbral, q_sel, min_caidas, cache_h, prog_file) {
        promises::future_promise(
          {
            suppressWarnings(suppressMessages({
              library(quantmod)
            }))
            source("R/extremos_core.R", local = TRUE)
            ext_run_screener(
              ticks,
              umbral = umbral, q_sel = q_sel, min_caidas = min_caidas,
              max_age_hours = cache_h,
              progress_cb = function(k, n, tk) {
                try(writeLines(sprintf("%d|%d|%s", k, n, tk), prog_file), silent = TRUE)
              }
            )
          },
          seed = TRUE
        )
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
