# R/institutional_module.R
# Módulo Aislado Institucional

institutional_ui <- function(id) {
  ns <- NS(id)

  fluidPage(
    # Quitado theme = my_theme, useShinyjs() , etc. Porque la principal lo maneja
    inst_ui_global_head(),
    div(
      class = "hero-wrap",
      div(class = "hero-title", "Análisis Avanzado"),
      div(
        class = "hero-sub",
        "Calcula fundamentales, técnicos, clasifica y exporta el resultado."
      )
    ),
    uiOutput(ns("lock_banner_ui")),
    tabsetPanel(
      id = ns("main_tabs"),
      type = "pills",
      tabPanel(
        "Dashboard Analítico",
        br(),
        # 1. ENTRADA (Control del análisis)
        fluidRow(
          column(
            width = 12,
            div(
              class = "card p-3 mb-4",
              h4("Entrada"),
              textAreaInput(
                ns("tickers"),
                "Tickers",
                value = inst_default_tickers_text(),
                rows = 4,
                width = "100%"
              ),
              fluidRow(
                column(6, selectInput(ns("perfil"), "Perfil de inversor", choices = c("Conservador", "Balanceado", "Agresivo"), selected = "Balanceado")),
                column(6, selectInput(ns("modo_cache"), "Modo de caché", choices = c("Local", "Postgres"), selected = if (inst_can_use_postgres_cache()) "Postgres" else "Local"))
              ),
              fluidRow(
                column(6, actionButton(ns("run_btn"), "Iniciar análisis", icon = icon("play"), class = "btn-success w-100", style = "margin-bottom: 10px;"),
                       shinyjs::hidden(actionButton(ns("cancel_btn"), "Cancelar análisis", icon = icon("stop"), class = "btn-danger w-100", style = "margin-bottom: 10px;"))),
                column(6, actionButton(ns("reset_btn"), "Restaurar Tickers", icon = icon("rotate-left"), class = "btn-secondary w-100", style = "margin-bottom: 10px;"))
              ),
              fluidRow(
                column(6, actionButton(ns("glossary_btn"), "Glosario", class = "btn-warning w-100")),
                column(6, downloadButton(ns("download_excel"), "Exportar Excel", class = "btn-primary w-100"))
              ),
              br(),
              uiOutput(ns("progress_ui")),
              actionButton(ns("clear_cache_btn"), "Limpiar Caché Local", class = "btn-danger w-100", icon = icon("trash"))
            )
          )
        ),
        # 2. RESUMEN GENERAL (Resultados principales)
        fluidRow(
          column(
            width = 12,
            div(
              class = "card p-3 mb-4",
              h4("Resumen general"),
              DTOutput(ns("summary_tbl"))
            )
          )
        ),
        # 3. CRITERIO (Leyenda)
        fluidRow(
          column(
            width = 12,
            div(
              class = "card p-3 mb-4",
              h4("Criterio"),
              div(
                class = "d-flex flex-wrap gap-4",
                p(HTML("<span class='sem-dot' style='background:#22c55e;'></span><b>Verde:</b> fortaleza alta.")),
                p(HTML("<span class='sem-dot' style='background:#f59e0b;'></span><b>Amarillo:</b> señales mixtas.")),
                p(HTML("<span class='sem-dot' style='background:#ef4444;'></span><b>Rojo:</b> debilidad o mayor riesgo."))
              )
            )
          )
        ),
        # 4. GRÁFICA INTERACTIVA
        fluidRow(
          column(
            width = 12,
            div(
              class = "card p-3 mb-4",
              h4("Gráfica por ticker"),
              plotlyOutput(ns("price_plot"), height = "500px") # Aumentado a 500 para aprovechar el ancho
            )
          )
        ),
        # 5. DETALLE POR INDICADOR
        fluidRow(
          column(
            width = 12,
            div(
              class = "card p-3 mb-4",
              h4("Detalle por indicador"),
              selectInput(ns("ticker_detalle"), "Selecciona ticker para profundizar", choices = NULL, width = "100%"),
              uiOutput(ns("ticker_badges_ui")),
              DTOutput(ns("detail_tbl"))
            )
          )
        ),
        # 6. MATRIZ Y BACKTESTING (Secciones finales)
        fluidRow(
          column(
            width = 12,
            div(
              class = "card p-3 mb-4",
              h4("Matriz de Correlación"),
              plotlyOutput(ns("cor_plot"), height = "450px")
            )
          )
        ),
        fluidRow(
          column(
            width = 12,
            div(
              class = "card p-3 mb-4",
              h4("Backtesting (1 Año Histórico)"),
              DTOutput(ns("backtest_tbl"))
            )
          )
        )
      ), # Fin tabPanel Dashboard
      tabPanel(
        "Guía de Uso (Playbook)",
        br(),
        inst_playbook_tab_ui()
      )
    ) # Fin tabsetPanel
  )
}

institutional_server <- function(id, user_reactive) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    rv <- reactiveValues(
      results       = NULL,
      progress      = 0,
      progress_msg  = "Esperando análisis...",
      result        = NULL,
      wb_path       = NULL,
      cancelled     = FALSE,
      task_running  = FALSE, # TRUE mientras el ExtendedTask está activo
      anim_step     = 0 # contador para animar la barra
    )

    # ---- Lógica de Bloqueo / Banner Local ----
    can_analyze <- reactive({
      u <- user_reactive()
      if (is.null(u)) {
        return(FALSE)
      }
      if (is_super_admin(u) || db_is_admin(u) || db_membership_is_active(u)) {
        return(TRUE)
      }
      left <- suppressWarnings(as.numeric(db_trial_days_left(u)[1]))
      isTRUE(!is.na(left) && left >= 0)
    })

    output$lock_banner_ui <- renderUI({
      u <- user_reactive()
      if (is.null(u)) {
        return(NULL)
      }

      active <- db_membership_is_active(u) || is_super_admin(u) || db_is_admin(u)
      if (active) {
        return(NULL)
      }

      left <- suppressWarnings(as.numeric(db_trial_days_left(u)[1]))
      if (is.na(left) || left < 0) {
        div(
          class = "alert alert-danger mb-4",
          icon("exclamation-triangle"), " Tu período de prueba finalizó. El análisis institucional está bloqueado."
        )
      } else {
        div(
          class = "alert alert-warning mb-4",
          icon("clock"), sprintf(" Período de prueba: Tienes %d días de acceso restante.", as.integer(left))
        )
      }
    })

    # Reactivo independiente para controlar la animación de progreso
    animating <- reactiveVal(FALSE)

    # Animación de progreso mientras el worker corre
    # Arranca cuando animating() es TRUE, independiente del estado del ExtendedTask
    observe({
      req(animating())
      invalidateLater(800)

      # Mensajes rotativos para dar sensación de avance
      msgs <- c(
        "Descargando datos de Finviz y Yahoo Finance...",
        "Calculando indicadores fundamentales...",
        "Evaluando métricas técnicas (SMA, RSI)...",
        "Calculando Beta, Divergencias y Cruces...",
        "Aplicando pesos por perfil de inversor...",
        "Generando semáforos y rankings...",
        "Construyendo matrices de correlación...",
        "Ejecutando backtesting (1 año)...",
        "Finalizando análisis institucional..."
      )

      # Usar isolate para no activar una revaluación instantánea (infinite loop)
      step <- isolate(rv$anim_step)
      curr_prog <- isolate(rv$progress)

      rv$anim_step <- (step + 1) %% length(msgs)

      # Progreso simulado: sube de 5% a 90% de forma gradual
      if (curr_prog < 90) {
        increment <- if (curr_prog < 30) 4 else if (curr_prog < 60) 2 else 1
        rv$progress <- min(90, curr_prog + increment)
      }
      rv$progress_msg <- msgs[[rv$anim_step + 1]]
    })

    output$progress_ui <- renderUI({
      tagList(
        div(style = "font-weight:600; margin-bottom:8px;", paste0(rv$progress_msg, " (", rv$progress, "%)")),
        div(
          class = "progress",
          div(
            class = "progress-bar",
            role = "progressbar",
            style = paste0("width:", rv$progress, "%;"),
            paste0(rv$progress, "%")
          )
        )
      )
    })

    observeEvent(input$reset_btn, {
      updateTextAreaInput(session, "tickers", value = inst_default_tickers_text())
      showNotification("Lista restaurada.", type = "message")
    })

    observeEvent(input$glossary_btn, {
      inst_show_glossary_modal(inst_build_glossary_df())
    })

    observeEvent(input$clear_cache_btn, {
      # 1. Limpiar caché en disco (archivos RDS locales)
      cache_path <- file.path(getwd(), "cache")
      if (dir.exists(cache_path)) {
        unlink(cache_path, recursive = TRUE, force = TRUE)
        dir.create(cache_path, showWarnings = FALSE)
      }

      # 2. Limpiar caché Postgres usando la función centralizada de db.R
      # db_clear_ticker_cache() limpia: cached_analysis (TRUNCATE) + ticker_cache (DROP)
      if (exists("db_clear_ticker_cache", mode = "function")) {
        tryCatch(
          {
            db_clear_ticker_cache()
            message("[INST] Caché Postgres limpiada via db_clear_ticker_cache().")
          },
          error = function(e) message("[INST] Aviso al limpiar caché Postgres: ", e$message)
        )
      }

      # 3. Resetear flag de inicialización del pool local para forzar re-creación de tabla
      if (exists("pg_cache_initialized", envir = .GlobalEnv)) {
        tryCatch(assign("pg_cache_initialized", FALSE, envir = .GlobalEnv), error = function(e) NULL)
      }

      showNotification("Caché local y en DB limpiadas. El próximo análisis descargará datos frescos.", type = "message")
    })

    # Tarea en background para Análisis Institucional
    # Usamos try para evitar crashes si la versión de Shiny es muy antigua
    institutional_task <- tryCatch(
      {
        shiny::ExtendedTask$new(function(ticks, perf, use_pg) {
          promises::future_promise(
            {
              # Estabilidad: Forzamos plan secuencial interno y cargamos entorno núcleo
              library(future)
              plan(sequential)

              # Sourcing del núcleo para visibilidad de helpers en el worker
              source("R/db.R", local = TRUE)
              source("R/finance_core.R", local = TRUE)
              source("R/institutional_core.R", local = TRUE)

              # SEGURIDAD DE CONEXIONES: Prevenir leaks cerrando los pools al terminar el future
              on.exit(
                {
                  if (exists("pool_close", mode = "function")) try(pool_close(), silent = TRUE)
                  if (exists("pg_pool_global") && !is.null(get("pg_pool_global"))) {
                    try(pool::poolClose(get("pg_pool_global")), silent = TRUE)
                  }
                },
                add = TRUE
              )

              suppressWarnings({
                library(dplyr)
                library(httr)
                library(rvest)
                library(xml2)
                library(quantmod)
                library(TTR)
                library(plotly)
                library(openxlsx)
              })
              inst_run_pipeline(
                tickers = ticks,
                investor_profile = perf,
                use_postgres_cache = use_pg,
                progress_cb = NULL
              )
            },
            seed = TRUE
          )
        })
      },
      error = function(e) {
        message("Warning: Institutional ExtendedTask failed or missing: ", e$message)
        NULL
      }
    )


    observeEvent(input$run_btn, {
      req(can_analyze())
      tickers <- inst_parse_tickers(input$tickers)

      if (length(tickers) == 0) {
        showNotification("Ingresa al menos un ticker válido.", type = "warning")
        return()
      }

      rv$progress <- 5
      rv$anim_step <- 0
      rv$progress_msg <- sprintf("Iniciando análisis de %d tickers...", length(tickers))
      rv$cancelled <- FALSE
      rv$task_running <- TRUE
      animating(TRUE) # <-- Arranca la animación inmediatamente
      shinyjs::disable("run_btn")
      shinyjs::show("cancel_btn")

      use_pg <- identical(input$modo_cache, "Postgres") && inst_can_use_postgres_cache()

      if (!is.null(institutional_task)) {
        result_ok <- tryCatch(
          {
            institutional_task$invoke(tickers, input$perfil, use_pg)
            TRUE
          },
          error = function(e) {
            message("[Inst] invoke() fallido: ", e$message)
            FALSE
          }
        )

        if (!result_ok) {
          # Fallback sincrónico si el worker no pudo iniciarse
          showNotification(
            "Análisis institucional en modo secuencial (workers no disponibles).",
            type = "warning", duration = 5
          )
          tryCatch(
            {
              res <- inst_run_pipeline(tickers, input$perfil, use_pg, progress_cb = NULL)
              if (!rv$cancelled) rv$result <- res
              rv$progress <- 100
              rv$progress_msg <- "Completado"
              shinyjs::enable("run_btn")
              shinyjs::hide("cancel_btn")
            },
            error = function(e) {
              showNotification(paste("Error institucional:", e$message), type = "error", duration = 8)
              shinyjs::enable("run_btn")
              shinyjs::hide("cancel_btn")
            }
          )
        }
      } else {
        showNotification("Modo compatibilidad: Análisis Institucional bloqueante.", type = "warning")
        res <- inst_run_pipeline(tickers, input$perfil, use_pg, progress_cb = NULL)
        if (!rv$cancelled) rv$result <- res
        rv$progress <- 100
        rv$progress_msg <- "Completado (Sincrónico)"
        shinyjs::enable("run_btn")
        shinyjs::hide("cancel_btn")
      }
    })

    observeEvent(input$cancel_btn, {
      rv$cancelled <- TRUE
      rv$task_running <- FALSE
      rv$progress <- 0
      rv$progress_msg <- "Análisis cancelado por el usuario."
      animating(FALSE) # <-- Detener animación al cancelar
      shinyjs::enable("run_btn")
      shinyjs::hide("cancel_btn")
      showNotification("Análisis cancelado. Los resultados en segundo plano serán descartados.")
    })

    if (!is.null(institutional_task)) {
      # Observador de estado: controla botones y detiene la animación
      observe({
        req(institutional_task)
        status <- institutional_task$status()

        if (status == "running") {
          rv$task_running <- TRUE
        } else {
          # Detener animación
          rv$task_running <- FALSE
          shinyjs::enable("run_btn")
          shinyjs::hide("cancel_btn")
        }

        if (status == "error") {
          rv$progress <- 0
          rv$progress_msg <- "Error en el análisis."
          try(
            {
              err_msg <- conditionMessage(institutional_task$result())
              showNotification(paste("Error:", err_msg), type = "error")
            },
            silent = TRUE
          )
        }
      })

      # Observador para procesar resultados exitosos
      observeEvent(institutional_task$result(), {
        req(institutional_task$status() == "success")

        # Si el usuario canceló, ignoramos el resultado
        if (rv$cancelled) {
          return()
        }

        res <- institutional_task$result()
        req(!is.null(res))

        rv$result <- res
        rv$task_running <- FALSE
        animating(FALSE) # <-- Detener animación al completar
        updateSelectInput(session, "ticker_detalle",
          choices  = res$summary$Ticker,
          selected = res$summary$Ticker[[1]]
        )

        wb_tmp <- tempfile(fileext = ".xlsx")
        inst_export_analysis_to_excel(res, wb_tmp)
        rv$wb_path <- wb_tmp

        rv$progress <- 100
        rv$progress_msg <- sprintf(
          "✅ Análisis completado: %d tickers procesados",
          nrow(res$summary)
        )
        showNotification("Análisis completado con éxito.", type = "message")
      })
    }

    output$download_excel <- downloadHandler(
      filename = function() {
        paste0("analisis_bursatil_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".xlsx")
      },
      content = function(file) {
        req(rv$result)
        # Usar el archivo pre-generado si existe (evita re-ejecutar la exportación completa)
        if (!is.null(rv$wb_path) && file.exists(rv$wb_path)) {
          file.copy(rv$wb_path, file, overwrite = TRUE)
        } else {
          inst_export_analysis_to_excel(rv$result, file)
        }
      }
    )

    output$summary_tbl <- renderDT({
      req(rv$result)

      df <- tryCatch(
        {
          # Obtenemos índices de columnas críticas de backtest de forma robusta
          bt_data <- rv$result$backtest
          col_ret <- if (any(grepl("Retorno Total", names(bt_data)))) {
            names(bt_data)[grepl("Retorno Total", names(bt_data))][1]
          } else {
            ""
          }

          base_df <- rv$result$summary %>%
            mutate(Rank = row_number()) %>%
            left_join(
              bt_data %>%
                group_by(Ticker) %>%
                summarise(
                  `Ret_B&H` = if (col_ret != "") .data[[col_ret]][grepl("Buy & Hold", Estrategia)][1] else NA_real_,
                  `Ret_Estr` = if (col_ret != "") .data[[col_ret]][grepl("Semaforo|Sem.foro", Estrategia)][1] else NA_real_,
                  .groups = "drop"
                ),
              by = "Ticker"
            )

          # Columnas opcionales — sólo incluirlas si existen
          optional_cols <- c("Sector", "Perfil")
          existing_optional <- intersect(optional_cols, names(base_df))

          base_df %>%
            mutate(
              across(matches("Sem.foro"), ~ inst_semaforo_html(.x)),
              across(matches("Precio"), ~ inst_fmt_num(.x)),
              across(matches("Ret_Estr|Ret_B&H"), ~ inst_fmt_num(.x, 1)),
              across(matches("RSI"), ~ inst_fmt_num(.x, 2)),
              across(matches("SMA"), ~ inst_fmt_num(.x, 2)),
              across(matches("Potencial"), ~ inst_fmt_num(.x, 2))
            ) %>%
            select(
              any_of(c("Rank", "Ticker")),
              matches("Puntaje"),
              matches("Sem.foro"),
              matches("Senal Playbook"),
              matches("Potencial"),
              any_of(existing_optional),
              matches("Ret_Estr"),
              matches("Ret_B&H"),
              everything()
            )
        },
        error = function(e) {
          message("[INST SummaryTbl] Error en procesamiento: ", e$message)
          rv$result$summary %>% mutate(Rank = row_number())
        }
      )

      # Detección robusta de columnas para formateo
      col_senal <- names(df)[grepl("Senal Playbook", names(df))][1]
      col_puntaje <- names(df)[grepl("Puntaje", names(df))][1]
      col_interp_idx <- which(grepl("Interpretacion", names(df))) - 1L

      # columnDef para Interpretacion: icono con tooltip CSS al hover
      # IMPORTANTE: JS esta en paste() con single-quotes para evitar conflictos con el parser de R
      js_render <- paste0(
        "function(data, type, row) {",
        '  if (type !== "display" || !data) return "";',
        '  var safe = data.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");',
        '  return "<span class=\'inst-icon\' data-tip=\'" + safe + "\' style=\'cursor:help\'>&#128203;</span>";',
        "}"
      )

      js_init <- paste0(
        "function(settings, json) {",
        "  var body = $(this.api().table().body());",
        '  body.on("mouseenter", "span.inst-icon", function(e) {',
        '    var tip = $(this).attr("data-tip");',
        "    if (!tip) return;",
        '    $("<div id=\'inst-tt\'></div>").text(tip).css({',
        '      position:"fixed", background:"#1e293b", color:"#f1f5f9",',
        '      padding:"10px 14px", borderRadius:"8px", fontSize:"0.82em",',
        '      maxWidth:"360px", lineHeight:"1.5", zIndex:99999,',
        '      boxShadow:"0 4px 18px rgba(0,0,0,0.45)", pointerEvents:"none",',
        '      whiteSpace:"pre-wrap"',
        '    }).appendTo("body");',
        '    $(document).on("mousemove.insttt", function(ev) {',
        '      $("#inst-tt").css({ left: ev.clientX + 14, top: ev.clientY + 14 });',
        "    });",
        '  }).on("mouseleave", "span.inst-icon", function() {',
        '    $("#inst-tt").remove();',
        '    $(document).off("mousemove.insttt");',
        "  });",
        "}"
      )

      col_defs <- if (length(col_interp_idx) > 0) {
        list(list(targets = col_interp_idx, width = "36px", render = JS(js_render)))
      } else {
        list()
      }

      dt_obj <- DT::datatable(
        df,
        rownames = FALSE,
        escape = FALSE,
        options = list(
          scrollX      = TRUE,
          pageLength   = 10,
          autoWidth    = TRUE,
          columnDefs   = col_defs,
          initComplete = JS(js_init)
        )
      )

      # Formateo condicional del puntaje
      if (length(col_puntaje) == 1 && !is.na(col_puntaje) && col_puntaje != "" &&
        col_puntaje %in% names(df) && is.numeric(df[[col_puntaje]])) {
        dt_obj <- dt_obj %>%
          DT::formatRound(col_puntaje, 1) %>%
          DT::formatStyle(
            col_puntaje,
            backgroundColor = DT::styleInterval(c(55, 75), c("#fee2e2", "#fef3c7", "#dcfce7"))
          )
      }

      # Negrita a la señal Playbook si existe
      if (length(col_senal) == 1 && !is.na(col_senal) && col_senal != "") {
        dt_obj <- dt_obj %>% DT::formatStyle(col_senal, fontWeight = "bold")
      }

      dt_obj
    })

    output$ranking_tbl <- renderDT({
      req(rv$result)

      df <- rv$result$ranking
      message("[INST] Renderizando RankingTbl con ", nrow(df), " filas.")

      dt_obj <- datatable(
        df %>%
          mutate(
            across(matches("Sem.foro"), ~ inst_semaforo_html(.x)),
            across(matches("Puntaje"), ~ inst_fmt_num(.x, 1))
          ),
        rownames = FALSE,
        escape = FALSE,
        options = list(dom = "t", pageLength = 10, ordering = FALSE)
      )

      col_senal <- names(df)[grepl("e.al Playbook", names(df))]
      if (length(col_senal) > 0) {
        dt_obj <- dt_obj %>% DT::formatStyle(col_senal[1], fontWeight = "bold")
      }

      dt_obj
    })


    output$ticker_badges_ui <- renderUI({
      req(rv$result, input$ticker_detalle)
      df <- rv$result$details[[input$ticker_detalle]]
      req(!is.null(df))

      price_str <- df$Valor[df$Indicador == "Precio actual (USD)"]
      graham_str <- df$Valor[df$Indicador == "Valor Graham (USD)"]
      margen_str <- df$Valor[df$Indicador == "Margen Graham"]

      beta_str <- df$Valor[df$Indicador == "Beta (1A)"]
      beta_sem <- df[[names(df)[grepl("Sem.foro", names(df))][1]]][df$Indicador == "Beta (1A)"]

      cruce_str <- df$Unidad[df$Indicador == "Cruce Media Móvil"]
      cruce_sem <- df[[names(df)[grepl("Sem.foro", names(df))][1]]][df$Indicador == "Cruce Media Móvil"]

      div_str <- df$Unidad[df$Indicador == "Divergencia RSI"]
      div_sem <- df[[names(df)[grepl("Sem.foro", names(df))][1]]][df$Indicador == "Divergencia RSI"]

      precio <- suppressWarnings(as.numeric(price_str))
      graham <- suppressWarnings(as.numeric(graham_str))
      margen <- suppressWarnings(as.numeric(margen_str))

      fair_value_ui <- if (length(precio) > 0 && length(graham) > 0 && !is.na(precio) && !is.na(graham) && graham > 0) {
        pct_fair <- min(100, max(0, (precio / graham) * 50))
        color_bar <- if (precio <= graham) "#10b981" else "#ef4444"
        margen_texto <- if (length(margen) > 0 && !is.na(margen)) paste0(ifelse(margen > 0, "+", ""), margen, "%") else ""

        div(
          style = "margin-bottom: 15px; background: #f8fafc; padding: 10px; border-radius: 8px; border: 1px solid #e2e8f0;",
          h5(icon("scale-balanced"), " Análisis Fair Value (Fórmula Graham)", style = "margin-top:0; margin-bottom:10px; color:#0f172a; font-weight:600;"),
          div(
            style = "display:flex; justify-content:space-between; font-size: 0.9em; margin-bottom:4px;",
            span(style = "color:#10b981; font-weight:bold;", "Descuento"),
            span(tags$b(margen_texto, style = paste0("color:", color_bar, "; font-size: 1.1em;"))),
            span(style = "color:#ef4444; font-weight:bold;", "Sobrevalorado")
          ),
          div(
            style = "width: 100%; height: 8px; background: #cbd5e1; border-radius: 4px; position:relative;",
            div(style = "position:absolute; left:50%; top:-4px; height:16px; width:2px; background:#475569; z-index:10;"),
            div(style = paste0("position:absolute; left:0; top:0; height:100%; width:", pct_fair, "%; background:", color_bar, "; border-radius:4px; max-width:100%;"))
          ),
          div(
            style = "display:flex; justify-content:space-between; margin-top:8px; font-size:0.85em; font-weight:600; color:#475569;",
            span(paste0("$", round(precio, 2), " (Actual)")),
            span(paste0("Fair Value: $", round(graham, 2)))
          )
        )
      } else {
        div(
          style = "margin-bottom: 15px; padding: 10px; border-radius: 8px; border: 1px dashed #ef4444; background:#fef2f2;",
          tags$b(icon("triangle-exclamation"), "Valor Graham: No disponible", style = "color:#ef4444; font-size: 0.9em;")
        )
      }

      get_sem_color <- function(sem) {
        if (length(sem) == 0 || is.na(sem)) {
          return("#64748b")
        }
        if (sem == "Verde") {
          return("#10b981")
        }
        if (sem == "Rojo") {
          return("#ef4444")
        }
        return("#f59e0b")
      }

      badges <- div(
        style = "display:flex; gap: 8px; flex-wrap: wrap; margin-bottom: 15px;",
        if (length(beta_str) > 0 && !is.na(beta_str)) span(style = paste0("background:", get_sem_color(beta_sem), "; color:white; padding:4px 8px; border-radius:4px; font-size:0.85em; font-weight:bold;"), icon("chart-line"), " Beta: ", round(as.numeric(beta_str), 2)) else NULL,
        if (length(cruce_str) > 0 && !is.na(cruce_str) && cruce_str != "Ninguno" && cruce_str != "") {
          span(style = paste0("background:", get_sem_color(cruce_sem), "; color:white; padding:4px 8px; border-radius:4px; font-size:0.85em; font-weight:bold;"), icon("arrows-turn-to-dots"), " ", cruce_str)
        } else {
          NULL
        },
        if (length(div_str) > 0 && !is.na(div_str) && div_str != "Ninguna" && div_str != "") {
          span(style = paste0("background:", get_sem_color(div_sem), "; color:white; padding:4px 8px; border-radius:4px; font-size:0.85em; font-weight:bold;"), icon("bolt"), " ", div_str)
        } else {
          NULL
        }
      )

      tagList(fair_value_ui, badges)
    })

    output$detail_tbl <- renderDT({
      req(rv$result, input$ticker_detalle)

      df <- rv$result$details[[input$ticker_detalle]]
      req(!is.null(df))

      bt <- rv$result$backtest %>% filter(Ticker == input$ticker_detalle)
      if (nrow(bt) == 2) {
        ret_bnh <- bt$`Retorno Total (%)`[grepl("Buy & Hold", bt$Estrategia)]
        ret_sys <- bt$`Retorno Total (%)`[grepl("Semaforo|Semáforo", bt$Estrategia)]
        dd_bnh <- bt$`Max Drawdown (%)`[grepl("Buy & Hold", bt$Estrategia)]
        dd_sys <- bt$`Max Drawdown (%)`[grepl("Semaforo|Semáforo", bt$Estrategia)]

        # Definimos las columnas del tribble usando nombres limpios temporalmente
        bt_rows <- tibble::tribble(
          ~Cat, ~Ind, ~Val, ~Sem, ~Pes, ~Raz, ~Uni,
          "Backtest (1A)", "Retorno Estrategia", as.numeric(ret_sys), "N/A", 0, "Comparativo Sistémico", "%",
          "Backtest (1A)", "Retorno Buy & Hold", as.numeric(ret_bnh), "N/A", 0, "Benchmark", "%",
          "Backtest (1A)", "Drawdown Estrategia", as.numeric(dd_sys), "N/A", 0, "Riesgo Sistémico Máximo", "%",
          "Backtest (1A)", "Drawdown Buy & Hold", as.numeric(dd_bnh), "N/A", 0, "Riesgo B&H Máximo", "%"
        )
        # Renombramos para que coincida con el df original (que tiene acentos)
        names(bt_rows) <- c(
          names(df)[grepl("Categ", names(df))][1], "Indicador", "Valor",
          names(df)[grepl("Sem.foro", names(df))][1], "Peso",
          names(df)[grepl("Raz.n", names(df))][1], "Unidad"
        )
        df <- bind_rows(df, bt_rows)
      }

      df2 <- df %>%
        mutate(
          across(matches("Sem.foro"), ~ case_when(
            .x == "Verde" ~ "Verde",
            .x == "Amarillo" ~ "Amarillo",
            .x == "Rojo" ~ "Rojo",
            TRUE ~ "N/A"
          )),
          Valor = inst_format_value_with_unit(Valor, Unidad)
        ) %>%
        select(matches("Categoria"), Indicador, Valor, matches("Sem.foro"), Peso, matches("Razon"), matches("Interpretacion"))

      sem_col_idx <- which(grepl("Sem.foro", names(df2))) - 1L # 0-based para DT JS

      datatable(
        df2,
        rownames = FALSE,
        escape = FALSE,
        options = list(
          scrollX = TRUE,
          pageLength = 30,
          autoWidth = TRUE,
          columnDefs = list(
            list(
              targets = sem_col_idx,
              render = JS(
                "function(data, type, row) {
                  if (type !== 'display') return data;
                  var map = {
                    'Verde':    ['#16a34a','#fff'],
                    'Amarillo': ['#d97706','#fff'],
                    'Rojo':     ['#dc2626','#fff'],
                    'N/A':      ['#64748b','#fff']
                  };
                  var c = map[data] || map['N/A'];
                  return '<span style=\"background:'+c[0]+';color:'+c[1]+
                    ';padding:2px 10px;border-radius:12px;font-size:0.8em;font-weight:600;display:inline-block;min-width:64px;text-align:center;\">'+
                    data+'</span>';
                }"
              )
            )
          )
        )
      )
    })

    output$price_plot <- renderPlotly({
      req(rv$result, input$ticker_detalle)

      hist_df <- rv$result$history[[input$ticker_detalle]]
      req(!is.null(hist_df), nrow(hist_df) > 0)

      p <- inst_build_price_plot(hist_df, input$ticker_detalle)

      bt <- rv$result$backtest %>% filter(Ticker == input$ticker_detalle)
      if (nrow(bt) == 2) {
        ret_bnh <- bt$`Retorno Total (%)`[grepl("Buy & Hold", bt$Estrategia)]
        ret_sys <- bt$`Retorno Total (%)`[grepl("Semaforo|Semáforo", bt$Estrategia)]

        msg <- paste0("<b>Backtest (1A):</b> Estrategia ", round(ret_sys, 1), "% | B&H ", round(ret_bnh, 1), "%")
        p <- p %>% layout(
          annotations = list(
            list(
              x = 0.5, y = 1.05, xref = "paper", yref = "paper",
              text = msg, showarrow = FALSE, font = list(size = 14, color = "#10b981")
            )
          )
        )
      }
      p
    })

    output$cor_plot <- renderPlotly({
      req(rv$result$correlation)
      inst_build_correlation_plot(rv$result$correlation)
    })

    output$backtest_tbl <- renderDT({
      req(rv$result$backtest)
      datatable(
        rv$result$backtest %>%
          mutate(
            `Retorno Total (%)` = inst_fmt_num(`Retorno Total (%)`),
            `Max Drawdown (%)` = inst_fmt_num(`Max Drawdown (%)`)
          ),
        rownames = FALSE,
        options = list(pageLength = 10, scrollX = TRUE, autoWidth = TRUE, dom = "t")
      )
    })
  })
}
