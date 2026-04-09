suppressWarnings({
  library(shiny)
  library(DT)
  library(dplyr)
  library(plotly)
  library(htmltools)
})

`%||%` <- function(a, b) {
  if (is.null(a) || length(a) == 0 || (is.character(a) && !nzchar(a[1]))) b else a
}

inst_ui_global_head <- function() {
  tags$head(
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1"),
    tags$style(HTML("
      /* Native Bootstrap theme is used */
      .hero-wrap { margin-bottom: 18px; margin-top: 10px; }
      .hero-title { font-size: 34px; font-weight: 800; margin-bottom: 6px; }
      .hero-sub { opacity: 0.8; margin-bottom: 16px; font-size: 1.1em; }
      .sem-dot {
        display: inline-block;
        width: 14px;
        height: 14px;
        border-radius: 50%;
        margin-right: 8px;
      }
      .progress {
        height: 22px;
        background: #e5e7eb;
        border-radius: 999px;
        overflow: hidden;
      }
      .progress-bar {
        height: 100%;
        background: linear-gradient(90deg, #0f62fe, #60a5fa);
        color: #fff;
        text-align: center;
        font-weight: 700;
        line-height: 22px;
      }
      @media (max-width: 768px) {
        .hero-title { font-size: 28px; }
      }
    "))
  )
}

inst_fmt_num <- function(x, digits = 2) {
  ifelse(is.na(x), NA_character_, format(round(as.numeric(x), digits), nsmall = digits, big.mark = ","))
}

inst_semaforo_color <- function(x) {
  dplyr::case_when(
    x == "Verde" ~ "#22c55e",
    x == "Amarillo" ~ "#f59e0b",
    TRUE ~ "#ef4444"
  )
}

inst_semaforo_html <- function(x) {
  col <- inst_semaforo_color(x)
  paste0(
    "<span class='sem-dot' style='background:", col, ";'></span>",
    x
  )
}

inst_format_value_with_unit <- function(value, unit) {
  mapply(function(v, u) {
    if (is.na(v)) return(NA_character_)
    if (is.null(u) || is.na(u) || !nzchar(u) || identical(u, "")) return(as.character(round(v, 2)))

    # Mantener el formato con espacio para coherencia
    paste0(round(v, 2), " ", u)
  }, value, unit, USE.NAMES = FALSE)
}

inst_show_glossary_modal <- function(glossary_df) {
  showModal(
    modalDialog(
      title = "Glosario de indicadores",
      size = "l",
      easyClose = TRUE,
      footer = modalButton("Cerrar"),
      div(
        style = "max-height:70vh; overflow:auto;",
        DT::renderDataTable({
          datatable(
            glossary_df,
            rownames = FALSE,
            options = list(scrollX = TRUE, pageLength = 15)
          )
        })
      )
    )
  )
}

inst_build_price_plot <- function(hist_df, ticker) {
  plot_ly(hist_df, x = ~Fecha) %>%
    add_lines(y = ~Cierre, name = "Precio", line = list(width = 2)) %>%
    add_lines(y = ~SMA20, name = "SMA20", line = list(dash = "dot")) %>%
    add_lines(y = ~SMA50, name = "SMA50", line = list(dash = "dash")) %>%
    add_lines(y = ~SMA200, name = "SMA200", line = list(dash = "longdash")) %>%
    layout(
      title = paste("Precio y medias móviles -", ticker),
      xaxis = list(title = "Fecha"),
      yaxis = list(title = "Precio (USD)"),
      legend = list(orientation = "h", x = 0, y = 1.1)
    )
}

inst_build_correlation_plot <- function(cor_mat) {
  if (is.null(cor_mat) || length(cor_mat) == 0) {
    return(plotly::plotly_empty(type = "scatter", mode = "markers") %>% layout(title = "Sin datos suficientes"))
  }
  
  # Preparar matriz para plotly heatmap (invierte orden de filas para que diagional quede top-left a bottom-right)
  plot_ly(
    x = colnames(cor_mat), 
    y = rownames(cor_mat), 
    z = cor_mat, 
    type = "heatmap",
    colorscale = "RdBu",
    zmin = -1, zmax = 1
  ) %>%
    layout(
      title = "Matriz de Correlación (Retornos Diarios)",
      xaxis = list(title = ""),
      yaxis = list(title = "")
    )
}

inst_playbook_tab_ui <- function() {
  div(
    class = "card p-4 mb-4",
    style = "max-width: 900px; margin: 0 auto; font-size: 1.05em;",
    h3(
      style = "color: #0f62fe; font-weight: bold; margin-bottom: 20px;", 
      "Libro de Jugadas (Playbook) - 4 Pasos"
    ),
    p("Usa esta guía paso a paso para tomar decisiones de compra y venta fundamentadas, basándote en los nuevos análisis de la plataforma."),
    hr(),
    
    h4(style = "color: #0f172a; font-weight: bold;", "Paso 1: El Filtro Inicial (Semáforo)"),
    p("Revisa la tabla de ", strong("Ranking"), " o el ", strong("Resumen General"), "."),
    tags$ul(
      tags$li(HTML("<b>Descarta:</b> Todo lo rojo. Suelen tener problemas fundamentales persistentes o tendencias bajistas muy fuertes.")),
      tags$li(HTML("<b>Enfócate:</b> Solo en los <b>Verdes</b>. Son empresas sólidas, manejando bien su deuda y en tendencia alcista confirmada."))
    ),
    br(),
    
    h4(style = "color: #0f172a; font-weight: bold;", "Paso 2: La Prueba de Fuego (Backtest)"),
    p("Revisa la tabla inferior de ", strong("Backtesting (1 Año)"), " para tus tickers seleccionados."),
    tags$ul(
      tags$li(HTML("<b>Elige acciones donde la Estrategia venza o empate al Buy & Hold.</b>")),
      tags$li(HTML("<b>Atención especial al Riesgo:</b> Queremos que el <b>Max Drawdown</b> (la peor caída de la estrategia) sea mucho MENOR que el del 'Buy & Hold'."))
    ),
    br(),
    
    h4(style = "color: #0f172a; font-weight: bold;", "Paso 3: Evaluando Precio y Riesgo (Detalle)"),
    p("Ve a la sección de ", strong("Detalle por Indicador"), "."),
    tags$ul(
      tags$li(HTML("<b>Margen Graham (Valuación):</b> Si es positivo o muy cercano a 0, estás comprando a un excelente precio o precio justo. Si es fuertemente negativo, asumes más riesgo de sobrevaloración.")),
      tags$li(HTML("<b>Volatilidad y Drawdown:</b> Si eres un inversor conservador, evita entrar a acciones cuyas caídas históricas recientes superen el 35-40%, o baja el tamaño de tu posición para mitigar el golpe."))
    ),
    br(),
    
    h4(style = "color: #0f172a; font-weight: bold;", "Paso 4: Armando el Equipo (Correlación)"),
    p("Finalmente, observa la ", strong("Matriz de Correlación"), "."),
    tags$ul(
      tags$li(HTML("<b>Evita:</b> Combinar varias empresas en rojo intenso (cercanas a 1.0). Se mueven idéntico; no estarías diversificando el riesgo, solo duplicándolo.")),
      tags$li(HTML("<b>Busca:</b> Mezclar empresas con colores muy pálidos o azules (-1.0 a 0.5) en la matriz. Eso significa mayor protección estructural en caídas generales del mercado."))
    ),
    br(),
    
    div(
      class = "alert alert-primary mt-3",
      style = "font-size: 1.1em;",
      HTML("<b>💡 Regla de Oro (Señal de Compra Fuerte):</b><br>
      <ol style='margin-top:10px;'>
        <li>Semáforo Verde.</li>
        <li>Buen histórico de Backtest.</li>
        <li>Buen precio (Margen Graham positivo).</li>
        <li>Baja correlación con el resto de tus activos.</li>
      </ol>
      <b>¿Cuándo Vender?</b><br>Vuelve a evaluar tu portafolio quincenal o mensualmente. Si un activo que compraste cambia su semáforo de Verde a Rojo y se mantiene así, será tu señal táctica para cerrar la posición.")
    )
  )
}
