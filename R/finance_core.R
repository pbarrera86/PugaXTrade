# R/finance_core.R — Núcleo avanzado: Finviz + heurísticas + Excel
# - Sin score de dividendos en el resumen (columna “Dividendos %” eliminada)
# - Sin indicadores "Dividend Gr. 3/5Y" ni "Dividend Est."
# - 31 indicadores por ticker (manteniendo “Payout” como indicador, no como score)
# - Semáforo con círculos coloreados (tamaño 18) en hojas por ticker
# - Resumen: degradado por COLUMNA; Rank inverso; SIN color en Price y Target
# - Hojas por ticker: NADA de color en 'Valor'; círculos + degradado en 'Semáforo'
# - DEFAULT_TICKERS + default_tickers_text() + run_pipeline_default() con normalización
suppressWarnings({
  library(dplyr)
  library(purrr)
  library(tidyr)
  library(tibble)
  library(stringr)
  library(httr)
  library(rvest)
  library(xml2)
  library(quantmod)
  library(TTR)
  library(openxlsx)
  library(lubridate)
  library(future)
  library(furrr)
})

`%||%` <- function(a, b) if (is.null(a) || length(a) == 0 || (is.character(a) && !nzchar(a))) b else a

# ------------------- Utilidades básicas -------------------
parse_tickers <- function(txt) {
  if (is.null(txt) || !nzchar(txt)) {
    return(character(0))
  }
  # separa por comas, punto y coma o cualquier whitespace (espacio, tab, salto de línea, etc.)
  xs <- unlist(strsplit(txt, "[,;\\s]+"))
  xs <- toupper(trimws(xs))
  # limpia entradas con basura a los extremos, conservando letras, dígitos, punto y guión
  xs <- gsub("^[^A-Z0-9.-]+|[^A-Z0-9.-]+$", "", xs)
  xs <- xs[nzchar(xs)]
  unique(xs)
}

.http_get <- function(url, tries = 3, sleep_sec = 1) {
  ua <- paste0(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) ",
    "AppleWebKit/537.36 (KHTML, like Gecko) ",
    "Chrome/124.0 Safari/537.36"
  )
  for (i in seq_len(tries)) {
    res <- try(httr::GET(
      url,
      httr::add_headers(
        `User-Agent` = ua,
        `Accept` = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        `Accept-Language` = "en-US,en;q=0.9",
        `Cache-Control` = "no-cache",
        `Pragma` = "no-cache"
      ),
      httr::timeout(10)
    ), silent = TRUE)

    if (!inherits(res, "try-error")) {
      code <- httr::status_code(res)
      if (code %in% 200:299) {
        return(res)
      }
      if (code == 404) {
        return(NULL)
      } # Not found, don't retry
    }

    # Exponential backoff: 1s, 2s, 4s...
    Sys.sleep(sleep_sec * (2^(i - 1)))
  }
  NULL
}

# ------------------- Tickers por defecto -------------------
DEFAULT_TICKERS <- unique(c(
  "NVDA", "MSFT", "AAPL", "AMZN", "META", "AVGO", "GOOGL", "GOOG", "TSLA", "JPM", "ORCL", "WMT", "LLY", "V", "MA", "NFLX", "XOM", "JNJ", "HD", "COST", "ABBV", "PLTR", "BAC", "PG", "CVX", "UNH", "GE", "KO", "CSCO", "TMUS", "WFC", "AMD", "PM", "MS", "IBM", "GS", "ABT", "NVO", "FCN", "BCPC", "RIVN", "BABA", "TSM", "EBS", "ASML", "DJT", "MSTR", "MARA", "UBS", "SHEL", "CLSK", "AVAV", "MUSA", "BBVA", "HE", "SOFI", "NIO", "TAL", "NVAX", "AXP", "CRM", "LIN", "MCD", "RTX", "T", "CAT", "DIS", "MRK", "UBER", "PEP", "NOW", "C", "INTU", "VZ", "QCOM", "ANET", "MU", "TMO", "BKNG", "BLK", "GEV", "SPGI", "BA", "SCHW", "TXN", "TJX", "ISRG", "LRCX", "ADBE", "LOW", "ACN", "NEE", "AMGN", "ETN", "PGR", "BX", "APH", "AMAT", "SYK", "COF", "BSX", "HON", "DHR", "PANW", "GILD", "PFE", "KKR", "KLAC", "UNP", "DE", "INTC", "COP", "ADP", "CMCSA", "ADI", "MDT", "LMT", "CRWD", "WELL", "DASH", "NKE", "MO", "PLD", "CB", "SO", "ICE", "CEG", "VRTX", "CDNS", "HCA", "MCO", "PH", "CME", "AMT", "SBUX", "MMC", "BMY", "CVS", "DUK", "GD", "MCK", "NEM", "ORLY", "DELL", "SHW", "WM", "TT", "RCL", "COIN", "NOC", "APO", "CTAS", "PNC", "MDLZ", "MMM", "ITW", "EQIX", "BK", "AJG", "ABNB", "ECL", "AON", "SNPS", "CI", "MSI", "USB", "HWM", "TDG", "UPS", "FI", "EMR", "AZO", "JCI", "WMB", "VST", "MAR", "RSG", "ELV", "WDAY", "NSC", "HLT", "MNST", "PYPL", "TEL", "APD", "CL", "GLW", "ZTS", "FCX", "EOG", "ADSK", "CMI", "AFL", "FTNT", "KMI", "AXON", "SPG", "TRV", "REGN", "TFC", "DLR", "URI", "CSX", "COR", "AEP", "NDAQ", "CMG", "FDX", "PCAR", "VLO", "CARR", "MET", "SLB", "ALL", "IDXX", "PSX", "BDX", "FAST", "SRE", "O", "ROP", "GM", "MPC", "PWR", "NXPI", "LHX", "D", "DHI", "MSCI", "AMP", "OKE", "STX", "WBD", "CPRT", "PSA", "ROST", "CBRE", "GWW", "PAYX", "DDOG", "CTVA", "XYZ", "BKR", "OXY", "F", "GRMN", "TTWO", "FANG", "PEG", "HSY", "VMC", "ETR", "RMD", "AME", "EXC", "EW", "KR", "LYV", "SYY", "CCI", "KMB", "CCL", "AIG", "TGT", "EBAY", "MPWR", "YUM", "EA", "XEL", "PRU", "A", "GEHC", "OTIS", "ACGL", "PCG", "RJF", "UAL", "CTSH", "XYL", "KVUE", "LVS", "CHTR", "HIG", "KDP", "MLM", "FICO", "CSGP", "DAL", "ROK", "NUE", "LEN", "WEC", "TRGP", "MCHP", "VRSK", "WDC", "VICI", "ED", "CAH", "FIS", "PHM", "VRSN", "AEE", "K", "ROL", "AWK", "MTB", "IR", "VTR", "TSCO", "STT", "NRG", "EQT", "IQV", "EL", "DD", "WAB", "EFX", "HUM", "WTW", "HPE", "AVB", "WRB", "IBKR", "EXPE", "SYF", "DTE", "BR", "IRM", "DXCM", "KEYS", "ADM", "FITB", "BRO", "EXR", "ODFL", "KHC", "ES", "DOV", "ULTA", "STE", "DRI", "CBOE", "STZ", "RF", "CINF", "WSM", "PTC", "CNP", "EQR", "IP", "NTAP", "PPG", "NTRS", "HBAN", "FE", "MTD", "HPQ", "ATO", "GIS", "TDY", "VLTO", "PPL", "SMCI", "DVN", "CHD", "FSLR", "CFG", "PODD", "JBL", "LH", "TPR", "BIIB", "CMS", "TPL", "EIX", "SBAC", "CDW", "CPAY", "TTD", "NVR", "HUBB", "TROW", "SW", "DG", "TYL", "EXE", "LDOS", "GDDY", "DLTR", "L", "ON", "STLD", "DGX", "KEY", "GPN", "LUV", "ERIE", "INCY", "TKO", "FTV", "IFF", "EVRG", "MAA", "BG", "LNT", "ZBRA", "CHRW", "BBY", "CNC", "MAS", "CLX", "ALLE", "BLDR", "DPZ", "KIM", "OMC", "TXT", "MKC", "WY", "GEN", "J", "DECK", "DOW", "SNA", "ESS", "LYB", "EXPD", "ZBH", "PSKY", "GPC", "LULU", "TSN", "AMCR", "LII", "PKG", "TRMB", "HAL", "IT", "NI", "RL", "FFIV", "WST", "CTRA", "INVH", "PNR", "TER", "WAT", "APTV", "PFG", "RVTY", "PNW", "ALB", "DVA", "CAG", "MTCH", "KMX", "AES", "AOS", "AIZ", "COO", "REG", "DOC", "SOLV", "NDSN", "WYNN", "BEN", "FOX", "UDR", "FOXA", "BXP", "SWK", "IEX", "MGM", "ALGN", "TAP", "MOH", "MRNA", "HAS", "IPG", "IVZ", "CPB", "ARE", "HOLX", "EG", "HRL", "CF", "BALL", "JBHT", "AVY", "FDS", "PAYC", "UHS", "JKHY", "NCLH", "CPT", "GL", "VTRS", "NWSA", "SJM", "SWKS", "DAY", "MOS", "AKAM", "POOL", "HST", "BAX", "GNRC", "HII", "EMN", "CZR", "NWS", "CRL", "MKTX", "LW", "EPAM", "APA", "LKQ", "TECH", "FRT", "MHK", "HSIC", "ENPH", "CRCL", "NU", "NET", "JD", "SHOP", "MELI", "PTON"
))

# String para prellenar el textarea en Shiny
default_tickers_text <- function() paste(DEFAULT_TICKERS, collapse = ", ")

# ------------------- Finviz -------------------
.read_finviz_table <- function(sym) {
  url <- sprintf("https://finviz.com/quote.ashx?t=%s", utils::URLencode(sym))
  res <- .http_get(url)
  if (is.null(res)) {
    return(NULL)
  }
  html <- try(read_html(res), silent = TRUE)
  if (inherits(html, "try-error")) {
    return(NULL)
  }

  nodes <- html_elements(html, "table.snapshot-table2")
  if (length(nodes) == 0) {
    return(NULL)
  }
  kv <- html_elements(nodes[[1]], "td")
  txt <- html_text(kv, trim = TRUE)
  if (length(txt) %% 2 != 0) txt <- head(txt, -1)
  if (length(txt) < 4) {
    return(NULL)
  }

  keys <- txt[seq(1, length(txt), by = 2)]
  vals <- txt[seq(2, length(txt), by = 2)]
  nkeys <- make.names(gsub("[$/%().-]", ".", keys))
  out <- setNames(as.list(vals), nkeys)
  out$raw_keys <- keys
  out$raw_vals <- vals
  out
}

# ------------------- Utilidades robustas de parsing -------------------
as_num <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(NA_real_)
  }
  x <- as.character(x[1])
  if (is.na(x) || !nzchar(x) || x %in% c("-", "N/A", "NA")) {
    return(NA_real_)
  }
  # Eliminar comas, signos de dolar, espacios, etc.
  # Mantener solo dígitos, punto, y signo negativo
  clean <- gsub("[^0-9.-]", "", x)
  suppressWarnings(as.numeric(clean))
}

as_pct <- function(x) {
  # Similar a as_num pero si viene "56.6%" lo trata como 56.6
  # El prompt pide interpretar "56.6%" como 56.6 (no 0.566)
  as_num(x)
}

# Wrapper compatible para código legado (reemplaza .num)
.num <- as_num

# ------------------- Glosario Data Frame -------------------
build_glossary_df <- function() {
  tibble::tribble(
    ~`Indicador EN`, ~`Indicador ES`, ~`Definición (qué mide)`, ~`Interpretación genérica`, ~`Trampas / notas`, ~`Dirección`, ~`Rangos (Verde/Amarillo/Rojo)`,

    # VALUACIÓN
    "P/E", "P/U", "Precio actual vs Utilidad por acción.", "Cuánto pagas por cada $1 de ganancia. Bajo es mejor.", "Cuidado con cíclicas (P/E bajo engañoso).", "Bajo es mejor", "V: <=22 | A: 22-35 | R: >35",
    "PEG", "PEG", "P/U dividido por tasa de crecimiento.", "Ajusta la valuación al crecimiento. <1 es ganga.", "Depende de la estimación de crecimiento.", "Bajo es mejor", "V: <=1.2 | A: 1.2-2.5 | R: >2.5",
    "Forward P/E", "P/U futuro", "Precio vs Utilidad esperada a 12 meses.", "Expectativa futura. Si < P/E actual, se espera crecimiento.", "Basado en estimaciones que pueden fallar.", "Bajo es mejor", "V: <=22 | A: 22-35 | R: >35",
    "P/S", "P/Ventas", "Precio vs Ventas por acción.", "Cuánto pagas por $1 de ventas. Útil si no hay ganancias.", "Márgenes bajos toleran P/S bajos.", "Bajo es mejor", "V: <=6 | A: 6-10 | R: >10",
    "P/FCF", "P/FCF", "Precio vs Flujo de Caja Libre.", "El dinero real que genera el negocio. Mejor que P/E.", "FCF puede ser volátil por capex.", "Bajo es mejor", "V: <=25 | A: 25-45 | R: >45",
    "EV/EBITDA", "EV/EBITDA", "Valor empresa (incl. deuda) vs EBITDA.", "Valuación neutral a estructura de capital.", "Ignora gastos de capital (Capex).", "Bajo es mejor", "V: <=18 | A: 18-25 | R: >25",
    "P/B", "P/Libros", "Precio vs Valor contable (Activos-Pasivos).", "Valor de liquidación teórico. Vital en bancos.", "Menos relevante en tech/intangibles.", "Bajo es mejor", "V: <=3 | A: 3-6 | R: >6",

    # CRECIMIENTO
    "EPS Y/Y TTM", "EPS interanual TTM", "Crecimiento de utilidades último año.", "Motor principal del precio a largo plazo.", "Comparaciones fáciles inflan el %.", "Alto es mejor", "V: >=15 | A: 5-15 | R: <5",
    "Sales Y/Y TTM", "Ventas interanual TTM", "Crecimiento de ingresos último año.", "Valida demanda real por productos/servicios.", "Crecimiento sin ganancias es riesgoso.", "Alto es mejor", "V: >=12 | A: 4-12 | R: <4",
    "EPS next Y", "EPS próximo año", "Crecimiento estimado sgte año fiscal.", "Expectativa corto plazo del mercado.", "Estimaciones pueden revisarse a la baja.", "Alto es mejor", "V: >=15 | A: 5-15 | R: <5",
    "EPS next 5Y", "EPS prox. 5 años", "Crecimiento anual compuesto estimado.", "Potencial a largo plazo. Clave para PEG.", "Muy incierto y especulativo.", "Alto es mejor", "V: >=15 | A: 8-15 | R: <8",
    "Sales Q/Q", "Ventas Trim/Trim", "Crecimiento ventas trimestre vs año anterior.", "Momentum actual de ingresos.", "Puede ser estacional.", "Alto es mejor", "V: >=8 | A: 3-8 | R: <3",
    "EPS Q/Q", "EPS Trim/Trim", "Crecimiento EPS trimestre vs año anterior.", "Momentum actual de utilidades.", "Eventos únicos pueden distorsionar.", "Alto es mejor", "V: >=15 | A: 5-15 | R: <5",

    # RENTABILIDAD
    "Gross Margin", "Margen bruto", "Ventas menos costo de ventas (%).", "Poder de fijación de precios y eficiencia productiva.", "Varía mucho por industria (Sw vs Retail).", "Alto es mejor", "V: >=45 | A: 25-45 | R: <25",
    "Oper. Margin", "Margen operativo", "Utilidad operativa sobre ventas (%).", "Eficiencia del negocio principal (antes de impuestos).", "Costos fijos altos reducen este margen.", "Alto es mejor", "V: >=20 | A: 10-20 | R: <10",
    "Profit Margin", "Margen neto", "Utilidad neta sobre ventas (%).", "Rentabilidad final para el accionista.", "Afectado por impuestos y deuda.", "Alto es mejor", "V: >=15 | A: 8-15 | R: <8",
    "ROE", "ROE", "Retorno sobre Patrimonio.", "Eficiencia usando capital de accionistas.", "Deuda alta infla el ROE artificialmente.", "Alto es mejor", "V: >=18 | A: 12-18 | R: <12",
    "ROIC", "ROIC", "Retorno sobre Capital Invertido.", "La mejor métrica de calidad de gestión.", "Debe ser mayor al costo de capital (WACC).", "Alto es mejor", "V: >=15 | A: 10-15 | R: <10",
    "ROA", "ROA", "Retorno sobre Activos totales.", "Eficiencia usando todos los recursos.", "Bajo para industrias pesadas.", "Alto es mejor", "V: >=7 | A: 4-7 | R: <4",

    # SALUD
    "Debt/Eq", "Deuda/Patrimonio", "Relación Deuda Total / Capital Accionista.", "Apalancamiento financiero y riesgo de quiebra.", "Utilities/Telcos suelen tener deuda alta.", "Bajo es mejor", "V: <=0.5 | A: 0.5-1.5 | R: >1.5",
    "Current Ratio", "Liquidez corriente", "Activos Corrientes / Pasivos Corrientes.", "Capacidad de pagar deudas a corto plazo.", "Muy alto puede ser ineficiencia de caja.", "Alto es mejor", "V: >=1.5 | A: 1.0-1.5 | R: <1.0",

    # TÉCNICA
    "SMA20", "Distancia SMA20", "Precio vs Promedio móvil 20 días (%)", "Tendencia de muy corto plazo.", "Muy extendido = reversión.", "Alto (Positivo) es mejor", "V: >=0 | A: -5 a 0 | R: < -5",
    "SMA50", "Distancia SMA50", "Precio vs Promedio móvil 50 días (%)", "Tendencia de mediano plazo.", "Soporte clave institucional.", "Alto (Positivo) es mejor", "V: >=0 | A: -5 a 0 | R: < -5",
    "SMA200", "Distancia SMA200", "Precio vs Promedio móvil 200 días (%)", "Tendencia de largo plazo (bull/bear).", "Debajo de 200 suele ser bajista.", "Alto (Positivo) es mejor", "V: >=0 | A: -5 a 0 | R: < -5",
    "RSI (14)", "RSI (14)", "Índice de Fuerza Relativa (0-100).", "Momentum. Sobrecompra (>70) / Sobreventa (<30).", "En tendencia fuerte puede seguir extremo.", "Rango óptimo", "V: 45-65 | A: 35-45/65-75 | R: <35/>75",

    # SENTIMIENTO
    "Short Float", "Interés Corto", "% de acciones prestadas para venta.", "Sentimiento bajista extremo o potencial squeeze.", "Arriba de 20% es peligroso/volátil.", "Bajo es mejor", "V: <=5 | A: 5-15 | R: >15",
    "Inst Own", "Propiedad Institucional", "% en manos de fondos/bancos.", "Respaldo del 'dinero inteligente'.", "Muy alto (>90%) limita compradores futuros.", "Alto es mejor", "V: >=60 | A: 40-60 | R: <40",
    "Insider Trans", "Transacciones Internas", "% cambio tenencia insiders últ. 6m.", "Confianza de la directiva en su empresa.", "Ventas pueden ser personales, compras son señal.", "Alto (Positivo) es mejor", "V: >=0 | A: -1 a 0 | R: < -1",

    # DIVIDENDOS
    "Payout", "Payout Ratio", "% utilidades pagadas como dividendo.", "Sostenibilidad del dividendo.", "REITs tienen payouts altos (90%) por ley.", "Rango óptimo", "V: 20-60 | A: 10-20/60-80 | R: <10/>80",

    # ANALISTAS
    "Upside to Target %", "Upside Potencial", "Distancia a precio objetivo promedio.", "Expectativa de subida según consenso.", "Analistas suelen ser optimistas/retrasados.", "Alto es mejor", "V: >=15 | A: 5-15 | R: <5",
    "Recom", "Recomendación", "1=Compra Fuerte, 5=Venta.", "Opinión consenso de Wall St.", "Contrarian: Venta fuerte puede ser piso.", "Bajo (cercano a 1) es mejor", "V: <=1.7 | A: 1.7-2.5 | R: >2.5"
  )
}

# ------------------- Lógica de Evaluación (Semáforo + Score) -------------------
eval_indicator <- function(ind_en, val) {
  # Defaults
  sem <- "N/A"
  score <- NA_real_ # Usaremos NA para score faltante
  razon <- ""

  if (is.na(val)) {
    return(list(sem = "Amarillo", score = 50, razon = "Sin datos disponibles (N/A)"))
  }

  v <- val

  # Interpolador simple (opcional, pero el prompt pide base 100/60/20)
  # Usaremos asignacion directa segun prompt.
  # Verde=100, Amarillo=60, Rojo=20
  S_V <- 100
  S_A <- 60
  S_R <- 20

  # --- VALUACIÓN ---
  if (ind_en == "P/E") {
    if (v <= 22) {
      sem <- "Verde"
      score <- S_V
    } else if (v <= 35) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "P/U: precio vs utilidades. Alto = valuación exigente."
  } else if (ind_en == "PEG") {
    if (v <= 1.2) {
      sem <- "Verde"
      score <- S_V
    } else if (v <= 2.5) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "PEG: P/U ajustado por crecimiento. >2.5 suele ser caro."
  } else if (ind_en == "Forward P/E") {
    if (v <= 22) {
      sem <- "Verde"
      score <- S_V
    } else if (v <= 35) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "P/U futuro: basado en utilidades esperadas."
  } else if (ind_en == "P/S") {
    if (v <= 6) {
      sem <- "Verde"
      score <- S_V
    } else if (v <= 10) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "P/Ventas: muy alto requiere márgenes fuertes."
  } else if (ind_en == "P/FCF") {
    if (v <= 25) {
      sem <- "Verde"
      score <- S_V
    } else if (v <= 45) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "P/FCF: precio vs flujo libre. Alto = expectativas altas."
  } else if (ind_en == "EV/EBITDA") {
    if (v <= 18) {
      sem <- "Verde"
      score <- S_V
    } else if (v <= 25) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "EV/EBITDA: valuación operativa ajustada por deuda."
  } else if (ind_en == "P/B") {
    if (v <= 3) {
      sem <- "Verde"
      score <- S_V
    } else if (v <= 6) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "P/Libro: depende del sector; alto suele ser caro."
  }

  # --- CRECIMIENTO ---
  else if (ind_en == "EPS Y/Y TTM") {
    if (v >= 15) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 5) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "EPS interanual TTM: crecimiento de utilidades."
  } else if (ind_en == "Sales Y/Y TTM") {
    if (v >= 12) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 4) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Ventas interanual TTM: expansión del negocio."
  } else if (ind_en == "EPS next Y") {
    if (v >= 15) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 5) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "EPS próximo año: crecimiento esperado."
  } else if (ind_en == "EPS next 5Y") {
    if (v >= 15) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 8) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "EPS 5 años: crecimiento esperado (promedio anual)."
  } else if (ind_en == "Sales Q/Q") {
    if (v >= 8) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 3) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Ventas Q/Q: momentum de ingresos."
  } else if (ind_en == "EPS Q/Q") {
    if (v >= 15) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 5) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "EPS Q/Q: momentum de utilidades."
  }

  # --- RENTABILIDAD ---
  else if (ind_en == "Gross Margin") {
    if (v >= 45) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 25) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Margen bruto: poder de precio/costos."
  } else if (ind_en == "Oper. Margin") {
    if (v >= 20) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 10) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Margen operativo: eficiencia del negocio."
  } else if (ind_en == "Profit Margin") {
    if (v >= 15) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 8) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Margen neto: rentabilidad final."
  } else if (ind_en == "ROE") {
    if (v >= 18) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 12) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "ROE: retorno al accionista (ojo con deuda)."
  } else if (ind_en == "ROIC") {
    if (v >= 15) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 10) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "ROIC: calidad del negocio (capital invertido)."
  } else if (ind_en == "ROA") {
    if (v >= 7) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 4) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "ROA: eficiencia sobre activos."
  }

  # --- SALUD ---
  else if (ind_en == "Debt/Eq") {
    if (v <= 0.5) {
      sem <- "Verde"
      score <- S_V
    } else if (v <= 1.5) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Deuda/Patrimonio: más alto = más riesgo."
  } else if (ind_en == "Current Ratio") {
    if (v >= 1.5) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 1.0) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Liquidez corto plazo: <1 indica presión."
  }

  # --- TÉCNICA ---
  else if (ind_en %in% c("SMA20", "SMA50", "SMA200")) {
    if (v >= 0) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= -5) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Tendencia: arriba de SMA=fortaleza; bajo SMA=debilidad."
  } else if (ind_en == "RSI (14)") {
    if (v >= 45 && v <= 65) {
      sem <- "Verde"
      score <- S_V
    } else if ((v >= 35 && v < 45) || (v > 65 && v <= 75)) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Momentum: extremos = sobreventa/sobrecompra."
  }

  # --- SENTIMIENTO ---
  else if (ind_en == "Short Float") {
    if (v <= 5) {
      sem <- "Verde"
      score <- S_V
    } else if (v <= 15) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Short float: alto eleva volatilidad y riesgo."
  } else if (ind_en == "Inst Own") {
    if (v >= 60) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 40) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Propiedad institucional: alto suele dar estabilidad."
  } else if (ind_en == "Insider Trans") {
    if (v >= 0) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= -1) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Insiders: compras netas son mejor señal que ventas."
  }

  # --- DIVIDENDOS ---
  else if (ind_en == "Payout") {
    if (v >= 20 && v <= 60) {
      sem <- "Verde"
      score <- S_V
    } else if ((v >= 10 && v < 20) || (v > 60 && v <= 80)) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Payout: demasiado alto puede ser insostenible."
  }

  # --- ANALISTAS ---
  else if (ind_en == "Upside to Target %") {
    if (v >= 15) {
      sem <- "Verde"
      score <- S_V
    } else if (v >= 5) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Upside vs target: confirmación (no motor)."
  } else if (ind_en == "Recom") {
    if (v <= 1.7) {
      sem <- "Verde"
      score <- S_V
    } else if (v <= 2.5) {
      sem <- "Amarillo"
      score <- S_A
    } else {
      sem <- "Rojo"
      score <- S_R
    }
    razon <- "Recomendación analistas: menor es mejor (1=compra fuerte)."
  } else {
    sem <- "N/A"
    score <- 50
    razon <- "Indicador no definido en reglas."
  }

  list(sem = sem, score = score, razon = razon)
}

# ------------------- Yahoo (precios) -------------------
.yf_xt <- function(ticker, from = Sys.Date() - 365, to = Sys.Date()) {
  # Yahoo usa guión para clases (p.ej. BRK-B), mientras Finviz usa punto (BRK.B)
  y_ticker <- gsub("\\.", "-", ticker)
  suppressWarnings({
    xt <- try(
      quantmod::getSymbols(
        Symbols = y_ticker, src = "yahoo",
        from = from, to = to, auto.assign = FALSE, warnings = FALSE
      ),
      silent = TRUE
    )
  })
  if (inherits(xt, "try-error")) {
    return(NULL)
  }
  xt
}

# ------------------- Caching Wrapper -------------------
.read_finviz_cached <- function(sym) {
  # 1. Check Cache
  if (exists("db_get_ticker_cache")) {
    cached_json <- tryCatch(db_get_ticker_cache(sym), error = function(e) NULL)
    if (!is.null(cached_json) && nzchar(cached_json)) {
      # Return deserialized list
      return(jsonlite::fromJSON(cached_json, simplifyVector = FALSE))
    }
  }

  # 2. Fetch Fresh
  raw_data <- .read_finviz_table(sym)

  # 3. Save Cache
  if (!is.null(raw_data) && length(raw_data) > 0 && exists("db_save_ticker_cache")) {
    json_str <- jsonlite::toJSON(raw_data, auto_unbox = TRUE)
    tryCatch(db_save_ticker_cache(sym, json_str), error = function(e) warning("Cache save failed: ", e$message))
  }

  raw_data
}

# ------------------- Scoring (sin dividendos) -------------------
score_analysts <- function(recom) {
  r <- suppressWarnings(as.numeric(recom))
  if (is.na(r)) {
    return(50)
  }
  pmin(100, pmax(0, (5 - r) / 4 * 100))
}

score_growth <- function(eps_next5y, sales_qq) {
  e <- .num(eps_next5y)
  s <- .num(sales_qq)
  e_sc <- if (is.na(e)) 50 else pmin(100, pmax(0, (e + 10) / 20 * 100))
  s_sc <- if (is.na(s)) 50 else pmin(100, pmax(0, (s + 30) / 60 * 100))
  round(0.6 * e_sc + 0.4 * s_sc, 1)
}

score_health <- function(debt_eq, current_ratio) {
  d <- .num(debt_eq)
  cr <- .num(current_ratio)
  d_sc <- if (is.na(d)) 50 else pmin(100, pmax(0, (2 - pmin(d, 2)) / 2 * 100))
  cr_sc <- if (is.na(cr)) 50 else pmin(100, pmax(0, (pmin(cr, 3) / 3 * 100)))
  round(0.6 * d_sc + 0.4 * cr_sc, 1)
}

score_profitability <- function(roe, margin, gross) {
  r <- .num(roe)
  m <- .num(margin)
  g <- .num(gross)
  r_sc <- if (is.na(r)) 50 else pmin(100, pmax(0, (r + 10) / 30 * 100))
  m_sc <- if (is.na(m)) 50 else pmin(100, pmax(0, (m + 20) / 40 * 100))
  g_sc <- if (is.na(g)) 50 else pmin(100, pmax(0, (g) / 60 * 100))
  round(0.5 * r_sc + 0.35 * m_sc + 0.15 * g_sc, 1)
}

score_sentiment <- function(ins_trans, inst_trans) {
  in_ <- .num(ins_trans)
  it_ <- .num(inst_trans)
  in_sc <- if (is.na(in_)) 50 else pmin(100, pmax(0, (in_ + 20) / 40 * 100))
  it_sc <- if (is.na(it_)) 50 else pmin(100, pmax(0, (it_ + 20) / 40 * 100))
  round(0.5 * in_sc + 0.5 * it_sc, 1)
}

score_technical <- function(px, s20, s50, s100, s200, rsi) {
  above <- sum(c(px > s20, px > s50, px > s100, px > s200), na.rm = TRUE)
  rsi_sc <- if (is.na(rsi)) 50 else if (rsi >= 55 && rsi <= 70) 85 else if (rsi > 70) 65 else 45
  round((above / 4) * 70 + 0.3 * rsi_sc, 1)
}

score_valuation <- function(pe, ps, peg) {
  pe_ <- .num(pe)
  ps_ <- .num(ps)
  peg_ <- .num(peg)
  pe_sc <- if (is.na(pe_)) 50 else pmin(100, pmax(0, 100 - pmin(pe_, 60) / 60 * 100))
  ps_sc <- if (is.na(ps_)) 50 else pmin(100, pmax(0, 100 - pmin(ps_, 30) / 30 * 100))
  peg_sc <- if (is.na(peg_)) 50 else pmin(100, pmax(0, 100 - pmin(peg_, 3) / 3 * 100))
  round(0.5 * pe_sc + 0.2 * ps_sc + 0.3 * peg_sc, 1)
}

verdict_from_score <- function(total) {
  if (is.na(total)) {
    return("En observación")
  }
  dplyr::case_when(
    total >= 75 ~ "Comprar/Acumular (largo plazo)",
    total >= 60 ~ "Comprar parcial en retrocesos",
    total >= 50 ~ "Mantener / En observación",
    TRUE ~ "Evitar / Esperar"
  )
}

# ------------------- Fetch + métricas -------------------
fetch_finviz_metrics <- function(ticker) {
  info <- .read_finviz_cached(ticker)
  xt <- .yf_xt(ticker)

  px <- NA_real_
  rsi <- NA_real_
  s20 <- s50 <- s100 <- s200 <- NA_real_
  if (!is.null(xt)) {
    px <- as.numeric(Cl(xt)[NROW(xt)])
    rsi <- as.numeric(TTR::RSI(Cl(xt), 14)[NROW(xt)])
    s20 <- as.numeric(TTR::SMA(Cl(xt), 20)[NROW(xt)])
    s50 <- as.numeric(TTR::SMA(Cl(xt), 50)[NROW(xt)]) # <- CORRECTO: 50
    s100 <- as.numeric(TTR::SMA(Cl(xt), 100)[NROW(xt)])
    s200 <- as.numeric(TTR::SMA(Cl(xt), 200)[NROW(xt)])
  }

  list(
    raw = info %||% list(),
    px = if (!is.null(info) && !is.null(info$Price)) .num(info$Price) else px,
    rsi = rsi, s20 = s20, s50 = s50, s100 = s100, s200 = s200,
    xt = xt
  )
}

# ------------------- Búsqueda robusta de claves -------------------
getv_robust <- function(raw_list, patterns) {
  if (is.null(raw_list) || length(raw_list) == 0) {
    return(NA_character_)
  }
  for (k in patterns) {
    if (!is.null(raw_list[[k]]) && nzchar(raw_list[[k]])) {
      return(raw_list[[k]])
    }
  }
  rk <- raw_list$raw_keys
  rv <- raw_list$raw_vals
  if (!is.null(rk) && !is.null(rv) && length(rk) == length(rv)) {
    for (p in patterns) {
      p2 <- gsub("\\.", ".*", p)
      hit <- which(grepl(p2, make.names(gsub("[$/%().-]", ".", rk)), ignore.case = TRUE))
      if (length(hit)) {
        v <- rv[hit[1]]
        if (length(v) && nzchar(v)) {
          return(v)
        }
      }
    }
  }
  NA_character_
}

# ------------------- Indicadores por ticker (31) -------------------
# ------------------- Indicadores por ticker (31) -------------------
dist_pct <- function(px, s) ifelse(is.na(px) || is.na(s) || px == 0, NA_real_, round(100 * (px - s) / px, 2))

build_indicator_table <- function(tk, m, raw) {
  r <- raw %||% list()
  price <- m$px
  if (is.na(price)) price <- .num(r$Price)

  # Extraction Logic (Reusable)
  vals <- list(
    "P/E" = .num(getv_robust(r, c("P.E", "P.E."))),
    "PEG" = .num(getv_robust(r, c("PEG"))),
    "Forward P/E" = .num(getv_robust(r, c("Forward.P.E", "Forward.P.E."))),
    "P/S" = .num(getv_robust(r, c("P.S", "Price.to.Sales"))),
    "P/FCF" = .num(getv_robust(r, c("P.FCf", "P.Free.Cashflow", "P.FCF"))),
    "EV/EBITDA" = .num(getv_robust(r, c("EV.EBITDA", "EV.to.EBITDA"))),
    "P/B" = .num(getv_robust(r, c("P.B", "Price.to.Book"))),

    # crecimiento
    "EPS Y/Y TTM" = .num(getv_robust(r, c("EPS.y.y", "EPS..this.Y", "EPS.this.Y", "EPS.growth.this.Y", "EPS.TTM", "EPS.this.Year"))),
    "Sales Y/Y TTM" = .num(getv_robust(r, c("Sales.y.y", "Sales..ttm", "Sales.this.Y", "Sales.past.5Y", "Sales.growth", "Sales.this.Year"))),
    "EPS next Y" = .num(getv_robust(r, c("EPS.next.Y", "EPS.next.Year"))),
    "EPS next 5Y" = .num(getv_robust(r, c("EPS.next.5Y", "EPS.growth.next.5Y"))),
    "Sales Q/Q" = .num(getv_robust(r, c("Sales.Q.Q", "Sales.QQ"))),
    "EPS Q/Q" = .num(getv_robust(r, c("EPS.Q.Q", "EPS.QQ"))),

    # rentabilidad
    "Oper. Margin" = .num(getv_robust(r, c("Oper..Margin", "Operating.Margin"))),
    "Profit Margin" = .num(getv_robust(r, c("Profit.Margin", "Net.Profit.Margin"))),
    "ROE" = .num(getv_robust(r, c("ROE", "Return.on.Equity"))),
    "ROIC" = .num(getv_robust(r, c("ROIC", "Return.on.Invested.Capital"))),
    "Gross Margin" = .num(getv_robust(r, c("Gross.Margin"))),
    "ROA" = .num(getv_robust(r, c("ROA", "Return.on.Assets"))),

    # salud
    "Debt/Eq" = .num(getv_robust(r, c("Debt.Eq", "Debt.to.Equity"))),
    "Current Ratio" = .num(getv_robust(r, c("Current.Ratio", "Quick.Ratio"))),

    # técnica
    "SMA20" = dist_pct(price, m$s20),
    "SMA50" = dist_pct(price, m$s50),
    "SMA200" = dist_pct(price, m$s200),
    "RSI (14)" = m$rsi,

    # sentimiento
    "Short Float" = .num(getv_robust(r, c("Short.Float"))),
    "Inst Own" = .num(getv_robust(r, c("Inst.Own", "Institutional.Own"))),
    "Insider Trans" = .num(getv_robust(r, c("Insider.Trans", "Insider.Transaction"))),

    # dividendos
    "Payout" = .num(getv_robust(r, c("Payout"))),

    # analistas
    "Upside to Target %" = if (!is.na(price)) {
      t <- .num(getv_robust(r, c("Target.Price")))
      if (!is.na(t)) round(100 * (t / price - 1), 2) else NA_real_
    } else {
      NA_real_
    },
    "Recom" = suppressWarnings(as.numeric(getv_robust(r, c("Recom"))))
  )

  # Metadata (Category, ES Name, Weight)
  # Keeping weights from original code as they weren't changed in prompt
  rows <- tribble(
    ~cat, ~en, ~es, ~peso,
    "valoración", "P/E", "P/U", 1.5,
    "valoración", "PEG", "PEG (Precio/Utilidad vs Crec.)", 2.0,
    "valoración", "Forward P/E", "P/U futuro", 1.5,
    "valoración", "P/S", "P/Ventas", 1.5,
    "valoración", "P/FCF", "P/Flujo de Caja Libre", 1.5,
    "valoración", "EV/EBITDA", "EV/EBITDA", 1.5,
    "valoración", "P/B", "P/Valor en libros", 0.5,
    "crecimiento", "EPS Y/Y TTM", "EPS interanual (TTM) %", 2.0,
    "crecimiento", "Sales Y/Y TTM", "Ventas interanual (TTM) %", 1.5,
    "crecimiento", "EPS next Y", "EPS próximo año %", 1.5,
    "crecimiento", "EPS next 5Y", "EPS próximos 5 años %", 1.5,
    "crecimiento", "Sales Q/Q", "Ventas Trim/Trim %", 1.2,
    "crecimiento", "EPS Q/Q", "Sorpresa EPS %", 0.5,
    "rentabilidad", "Gross Margin", "Margen bruto %", 1.0,
    "rentabilidad", "Oper. Margin", "Margen operativo %", 1.5,
    "rentabilidad", "Profit Margin", "Margen neto %", 1.5,
    "rentabilidad", "ROE", "ROE %", 1.5,
    "rentabilidad", "ROIC", "ROIC %", 1.5,
    "rentabilidad", "ROA", "ROA %", 1.0,
    "salud", "Debt/Eq", "Deuda/Patrimonio", 1.5,
    "salud", "Current Ratio", "Razón corriente", 1.2,
    "técnica", "SMA20", "Distancia a SMA20 %", 1.0,
    "técnica", "SMA50", "Distancia a SMA50 %", 1.0,
    "técnica", "SMA200", "Distancia a SMA200 %", 1.2,
    "técnica", "RSI (14)", "RSI(14)", 0.8,
    "sentimiento", "Short Float", "Corto sobre float %", 1.2,
    "sentimiento", "Inst Own", "Propiedad institucional %", 0.5,
    "sentimiento", "Insider Trans", "Transacciones de insiders %", 0.8,
    "dividendos", "Payout", "Payout %", 0.6,
    "analistas", "Upside to Target %", "Upside a precio objetivo %", 1.2,
    "analistas", "Recom", "Recomendación (1=Compra fuerte)", 1.2
  )

  # Populate values and evaluate
  rows$valor_raw <- unlist(vals[rows$en])

  evals <- purrr::map2(rows$en, rows$valor_raw, eval_indicator)

  rows$Semáforo <- purrr::map_chr(evals, "sem")
  rows$Score <- purrr::map_dbl(evals, "score")
  rows$Razón <- purrr::map_chr(evals, "razon")

  rows %>%
    transmute(
      `Categoría` = cat,
      `Indicador EN` = en,
      `Indicador ES` = es,
      `Valor` = round(as.numeric(valor_raw), 2),
      `Semáforo` = Semáforo,
      `Peso` = as.numeric(peso),
      `Razón` = Razón,
      `Score_Num` = Score # Internal use for aggregation
    )
}

infer_trend <- function(px, s20, s50, s200, rsi) {
  if (is.na(px) || is.na(s200)) {
    return("Lateral")
  }
  if (!is.na(s50) && !is.na(s20) && px > s200 && s50 > s200 && s20 > s50 && !is.na(rsi) && rsi >= 55) {
    return("Alcista fuerte")
  }
  if (px > s200 && (!is.na(rsi) && rsi >= 50)) {
    return("Alcista")
  }
  if (!is.na(s200) && abs(px - s200) / px <= 0.03 || (!is.na(rsi) && rsi >= 45 && rsi <= 55)) {
    return("Lateral")
  }
  "Bajista"
}

analyze_stock <- function(tk, m) {
  r <- m$raw %||% list()

  # 1. Build table with NEW logic
  indicators <- build_indicator_table(tk, m, r)

  # 2. Calculate Category Scores (Weighted Average)
  cat_scores <- indicators %>%
    filter(!is.na(Score_Num)) %>%
    group_by(`Categoría`) %>%
    summarise(
      score = sum(Score_Num * Peso) / sum(Peso),
      .groups = "drop"
    )

  get_cat_score <- function(cname) {
    s <- cat_scores$score[cat_scores$`Categoría` == cname]
    if (length(s) == 0 || is.na(s)) 50 else round(s, 1)
  }

  sc_anal <- get_cat_score("analistas")
  sc_grow <- get_cat_score("crecimiento")
  sc_health <- get_cat_score("salud")
  sc_prof <- get_cat_score("rentabilidad")
  sc_sent <- get_cat_score("sentimiento")
  sc_tech <- get_cat_score("técnica")
  sc_val <- get_cat_score("valoración")

  # 3. Calculate Total Score (Weighted Global Average)
  # Sum of (Score * Weight) for ALL valid indicators
  valid_inds <- indicators %>% filter(!is.na(Score_Num))
  if (nrow(valid_inds) > 0) {
    total <- round(sum(valid_inds$Score_Num * valid_inds$Peso) / sum(valid_inds$Peso))
  } else {
    total <- 50
  }

  # 4. Safety Rule / Verdict
  # Si Debt/Eq es Rojo OR ROIC es Rojo OR SMA200 es Rojo -> max “Neutral/Esperar”
  verdict <- verdict_from_score(total)

  # Validar regla de seguridad
  semaforo_map <- setNames(indicators$Semáforo, indicators$`Indicador EN`)
  is_red <- function(k) isTRUE(semaforo_map[k] == "Rojo")

  danger_flag <- is_red("Debt/Eq") || is_red("ROIC") || is_red("SMA200")

  if (danger_flag) {
    # Downgrade if verdict is Buy
    if (grepl("Comprar", verdict)) {
      verdict <- "Neutral/Esperar (Riesgo Alto)"
    }
  }

  # Conclusión Text
  concl <- sprintf(
    "Puntaje total: %s/100. Puntos fuertes: %s%s%s%s%s%s.",
    total,
    if (sc_prof >= 75) "rentabilidad, " else "",
    if (sc_grow >= 75) "crecimiento, " else "",
    if (sc_health >= 75) "salud, " else "",
    if (sc_anal >= 75) "analistas, " else "",
    if (sc_tech >= 75) "técnica, " else "",
    if (sc_val >= 75) "valoración" else ""
  )

  # Market Data
  price <- m$px
  if (is.na(price)) price <- .num(getv_robust(r, c("Price")))
  Target <- getv_robust(r, c("Target.Price"))
  tgt <- .num(Target)
  upside <- if (!is.na(tgt) && !is.na(price) && price > 0) round(100 * (tgt / price - 1), 2) else NA_real_
  trend <- infer_trend(price, m$s20, m$s50, m$s200, m$rsi)

  # Final Summary Row
  # Semaforo Score (visual summary of text lights)
  greens <- sum(indicators$Semáforo == "Verde", na.rm = TRUE)
  ambers <- sum(indicators$Semáforo == "Amarillo", na.rm = TRUE)
  reds <- sum(indicators$Semáforo == "Rojo", na.rm = TRUE)
  sem_txt <- sprintf("🟢 %d   🟡 %d   🔴 %d", greens, ambers, reds)
  sem_score_vis <- if ((greens + ambers + reds) > 0) round((greens * 100 + ambers * 60 + reds * 20) / ((greens + ambers + reds) * 100) * 100) else 50

  row <- tibble::tibble(
    Fecha = as.Date(Sys.Date()),
    Ticker = tk,
    Rank = NA_integer_,
    `Puntaje (0-100)` = as.numeric(total),
    Veredicto = verdict,
    Conclusión = concl,
    `Semáforo` = sem_txt,
    Price = if (!is.na(price)) round(price, 2) else NA_real_,
    Target = if (!is.na(tgt)) round(tgt, 2) else NA_real_,
    `Target(Price-1 %)` = as.numeric(upside),
    Tendencia = trend,
    SemaforoScore = as.numeric(sem_score_vis),
    `Analistas %` = as.numeric(sc_anal),
    `Crecimiento %` = as.numeric(sc_grow),
    `Salud %` = as.numeric(sc_health),
    `Rentabilidad %` = as.numeric(sc_prof),
    `Sentimiento %` = as.numeric(sc_sent),
    `Técnica %` = as.numeric(sc_tech),
    `Valoración %` = as.numeric(sc_val)
  )

  # Clean indicators for details (remove Score_Num if not needed in Excel directly, but maybe useful for debugging?
  # Prompt says: "En cada hoja... actualizar columnas Semáforo y Razón...".
  # The table in Excel expects: Categoría | Indicador EN | Indicador ES | Valor | Semáforo | Peso | Razón
  # We should remove Score_Num from the final 'indicators' list that goes to Excel logic
  out_ind <- indicators %>% select(-Score_Num)

  list(summary = row, indicators = out_ind)
}

# ------------------- Pipeline -------------------
# ------------------- Pipeline Estructurado (Paralelo) -------------------
run_pipeline <- function(tickers, progress_cb = NULL) {
  n <- length(tickers)

  pcall <- function(step = NULL, total = NULL, msg = NULL, pct = NULL) {
    if (is.function(progress_cb)) try(progress_cb(step, total, msg, pct), silent = TRUE)
  }

  # 1. Identificar qué tenemos en CACHE y qué falta (Batch lookup)
  pcall(0, n, "Verificando caché...", 1)

  # Listas de trabajo
  metrics_map <- vector("list", n)
  names(metrics_map) <- tickers
  to_fetch <- character()

  cached_data <- list()
  if (exists("db_get_ticker_cache_batch")) {
    cached_data <- tryCatch(db_get_ticker_cache_batch(tickers), error = function(e) list())
  }

  for (tk in tickers) {
    c_json <- cached_data[[tk]]
    if (!is.null(c_json) && nzchar(c_json)) {
      # Tenemos datos raw en caché de Finviz
      cached_list <- jsonlite::fromJSON(c_json, simplifyVector = FALSE)
      metrics_map[[tk]] <- list(from_cache = TRUE, raw = cached_list)
    } else {
      to_fetch <- c(to_fetch, tk)
      metrics_map[[tk]] <- list(from_cache = FALSE)
    }
  }

  # 2. Fetch en Paralelo (Chunked for Keep-Alive/Stability)
  fetched_results <- list()

  # Workers definition
  worker_fetch_full <- function(tk) {
    # 1. Finviz Web Scrape
    r_info <- tryCatch(.read_finviz_table(tk), error = function(e) NULL)
    # 2. Yahoo Data
    xt <- .yf_xt(tk)
    list(ticker = tk, info = r_info, xt = xt)
  }

  worker_fetch_yahoo_only <- function(tk) {
    xt <- .yf_xt(tk)
    list(ticker = tk, xt = xt)
  }

  items_to_parallel <- list()
  # Add tickers that need full fetch
  for (tk in to_fetch) items_to_parallel[[tk]] <- list(type = "full", ticker = tk)

  # Add tickers that need just Yahoo (from cache)
  cached_tickers <- setdiff(tickers, to_fetch)
  for (tk in cached_tickers) items_to_parallel[[tk]] <- list(type = "yahoo", ticker = tk)

  if (length(items_to_parallel) > 0) {
    n_p <- length(items_to_parallel)
    pcall(0, n, sprintf("Descargando datos para %d tickers...", n_p), 10)

    # Combinamos todo en un solo parallel map para máxima eficiencia de workers
    chunk_size <- 10 # Slightly larger batches for efficiency
    parallel_keys <- names(items_to_parallel)
    chunks <- split(parallel_keys, ceiling(seq_along(parallel_keys) / chunk_size))

    processed_count <- 0
    for (i in seq_along(chunks)) {
      batch_keys <- chunks[[i]]

      batch_res <- future_map(batch_keys, function(k) {
        item <- items_to_parallel[[k]]
        if (item$type == "full") {
          return(worker_fetch_full(item$ticker))
        }
        return(worker_fetch_yahoo_only(item$ticker))
      }, .options = furrr_options(seed = TRUE))
      names(batch_res) <- batch_keys

      fetched_results <- c(fetched_results, batch_res)

      processed_count <- processed_count + length(batch_keys)
      current_pct <- 10 + (processed_count / n_p) * 40
      pcall(processed_count, n_p, sprintf("Descargando: Lote %d/%d...", i, length(chunks)), current_pct)
    }
  }

  # 3. Consolidar y Guardar Caché (Main Thread)
  pcall(n, n, "Consolidando datos...", 50)

  final_metrics <- vector("list", n)
  names(final_metrics) <- tickers
  cache_queue <- list()

  for (tk in tickers) {
    res <- fetched_results[[tk]]
    m_info <- metrics_map[[tk]]

    if (is.null(res)) {
      # Fallback a lo que ya teníamos o vacío
      final_metrics[[tk]] <- list(raw = m_info$raw %||% list(), px = NA, rsi = NA, s20 = NA, s50 = NA, s100 = NA, s200 = NA, xt = NULL)
      next
    }

    # Determinamos el 'raw' (o del fetch o del cache inicial)
    raw_data <- if (!is.null(res$info)) res$info else m_info$raw
    if (is.null(raw_data)) raw_data <- list()

    # Si fue fetch fresco, lo encolamos para cache
    if (!is.null(res$info) && length(res$info) > 0) {
      cache_queue[[tk]] <- jsonlite::toJSON(res$info, auto_unbox = TRUE)
    }

    # Calculate Tech indicators from xt
    xt <- res$xt
    px <- NA_real_
    rsi <- NA_real_
    s20 <- s50 <- s100 <- s200 <- NA_real_

    if (!is.null(xt)) {
      cl <- tryCatch(Cl(xt), error = function(e) NULL)
      if (!is.null(cl)) {
        idx <- NROW(cl)
        px <- tryCatch(as.numeric(cl[idx]), error = function(e) NA_real_)
        safe_ind <- function(expr) tryCatch(expr, error = function(e) NA_real_)
        rsi <- safe_ind(as.numeric(TTR::RSI(cl, 14)[idx]))
        s20 <- safe_ind(as.numeric(TTR::SMA(cl, 20)[idx]))
        s50 <- safe_ind(as.numeric(TTR::SMA(cl, 50)[idx]))
        s100 <- safe_ind(as.numeric(TTR::SMA(cl, 100)[idx]))
        s200 <- safe_ind(as.numeric(TTR::SMA(cl, 200)[idx]))
      }
    }

    final_metrics[[tk]] <- list(
      raw = raw_data, xt = xt, px = px, rsi = rsi,
      s20 = s20, s50 = s50, s100 = s100, s200 = s200
    )
  }

  # BATCH CACHE SAVE (Transactional)
  if (length(cache_queue) > 0 && exists("db_save_ticker_cache_batch")) {
    pcall(n, n, "Guardando caché...", 55)
    tryCatch(db_save_ticker_cache_batch(cache_queue), error = function(e) NULL)
  }


  # 4. Análisis (CPU bound, fast)
  pcall(n, n, "Analizando métricas...", 80)

  summ <- list()
  ind_list <- list()

  # Esto es muy rápido en memoria, loop secuencial está bien.
  # Si fueran 5000 tickers, usaríamos parallel. Para 500 es instántaneo.
  i_anal <- 0
  for (tk in tickers) {
    i_anal <- i_anal + 1
    # Heartbeat every 10 items (Analysis - Step 4)
    if (i_anal %% 10 == 0) {
      current_pct <- 55 + (i_anal / n) * 40
      pcall(i_anal, n, sprintf("Analizando %s (%d/%d)...", tk, i_anal, n), current_pct)
    }

    # Wrap entire analysis step in tryCatch to prevent crashing on single ticker
    tryCatch(
      {
        m <- final_metrics[[tk]]
        if (is.null(m)) m <- list(raw = list(), px = NA)

        # SANITIZACIÓN: Reemplazar Inf/-Inf/NaN con NA antes de analizar
        sanit <- function(x) {
          if (is.list(x)) {
            return(x)
          }
          x[is.infinite(x) | is.nan(x)] <- NA
          x
        }
        m <- lapply(m, sanit)

        r <- try(analyze_stock(tk, m), silent = TRUE)

        if (inherits(r, "try-error") || is.null(r)) {
          # Fallback below
          stop("Analysis failed")
        } else {
          # Sanitize summary row specifically for JSON compatibility
          row <- r$summary
          clean_col <- function(x) {
            if (is.numeric(x)) x[is.infinite(x) | is.nan(x)] <- NA
            x
          }
          row[] <- lapply(row, clean_col)
          summ[[tk]] <- row
          ind_list[[tk]] <- r$indicators
        }
      },
      error = function(e) {
        # Ticker failed completely - Add Empty Row
        neutral <- tibble::tibble(
          Fecha = as.Date(Sys.Date()), Ticker = tk, Rank = NA_integer_,
          `Puntaje (0-100)` = 50, Veredicto = "Error Datos",
          Conclusión = paste("Error:", e$message),
          `Semáforo` = "🟢 0   🟡 0   🔴 0", SemaforoScore = 50,
          `Analistas %` = 50, `Crecimiento %` = 50, `Salud %` = 50,
          `Rentabilidad %` = 50, `Sentimiento %` = 50, `Técnica %` = 50, `Valoración %` = 50,
          Price = NA_real_, Target = NA_real_, `Target(Price-1 %)` = NA_real_, Tendencia = "Lateral"
        )
        summ[[tk]] <- neutral
        ind_list[[tk]] <- tibble::tibble()
      }
    )
  }

  pcall(n, n, "Finalizando...", 95)
  df <- suppressWarnings(dplyr::bind_rows(summ))

  if (!is.null(df) && nrow(df) > 0) {
    df <- df %>% arrange(desc(`Puntaje (0-100)`))
    df$Rank <- seq_len(nrow(df))
    df <- df %>% arrange(Rank)
  } else {
    df <- tibble::tibble()
  }

  pcall(n, n, "Completado", 100)
  list(summary = df, details = ind_list)
}

# ------------------- Wrapper con defaults y normalización de mayúsc/minúsc -------------------
run_pipeline_default <- function(tickers = NULL, progress_cb = NULL) {
  tks <- tickers
  if (is.null(tks) || (is.character(tks) && length(tks) == 1 && !nzchar(tks))) {
    tks <- DEFAULT_TICKERS
  } else if (is.character(tks) && length(tks) == 1 && nzchar(tks)) {
    tks <- parse_tickers(tks) # string -> parsea y MAYÚSCULAS
  } else if (is.character(tks) && length(tks) > 1) {
    tks <- toupper(trimws(tks)) # vector -> normaliza
    tks <- gsub("^[^A-Z0-9.-]+|[^A-Z0-9.-]+$", "", tks)
    tks <- unique(tks[nzchar(tks)])
  }
  run_pipeline(tks, progress_cb = progress_cb)
}

# ------------------- Excel helpers -------------------
.safe_cf <- function(wb, ...) {
  try(openxlsx::conditionalFormatting(wb, ...), silent = TRUE)
}

# Degradado estándar: rojo (bajo) -> amarillo -> verde (alto)
.apply_colour_scale <- function(wb, sheet, cols, rows) {
  if (length(cols) == 0 || length(rows) == 0) {
    return(invisible(NULL))
  }
  .safe_cf(
    wb,
    sheet = sheet, cols = cols, rows = rows,
    type = "colourScale",
    style = c("#ff6b6b", "#ffd166", "#2ecc71")
  )
}

# Degradado inverso: verde (bajo) -> amarillo -> rojo (alto)
.apply_colour_scale_inverted <- function(wb, sheet, cols, rows) {
  if (length(cols) == 0 || length(rows) == 0) {
    return(invisible(NULL))
  }
  .safe_cf(
    wb,
    sheet = sheet, cols = cols, rows = rows,
    type = "colourScale",
    style = c("#2ecc71", "#ffd166", "#ff6b6b")
  )
}

# Aplicar por COLUMNA (min/max independientes)
.apply_colour_scale_per_column <- function(wb, sheet, cols, rows, inverted = FALSE) {
  for (c in cols) {
    if (inverted) {
      .apply_colour_scale_inverted(wb, sheet, cols = c, rows = rows)
    } else {
      .apply_colour_scale(wb, sheet, cols = c, rows = rows)
    }
  }
}

.paint_semaforo_cells <- function(wb, sheet, col_idx, values) {
  if (is.null(values) || length(values) == 0) {
    return(invisible(NULL))
  }

  n <- length(values)
  # Escribimos el círculo (●) sobre el texto
  openxlsx::writeData(wb, sheet, rep("●", n), startCol = col_idx, startRow = 2, colNames = FALSE)

  v <- as.character(values)
  rows_red <- which(v == "Rojo") + 1
  rows_yellow <- which(v == "Amarillo") + 1
  rows_green <- which(v == "Verde") + 1

  size <- 18

  # Estilo Rojo
  if (length(rows_red) > 0) {
    st_red <- openxlsx::createStyle(fontColour = "#ff4d4d", fontSize = size, halign = "center", valign = "center")
    openxlsx::addStyle(wb, sheet, style = st_red, rows = rows_red, cols = col_idx, gridExpand = FALSE)
  }
  # Estilo Amarillo
  if (length(rows_yellow) > 0) {
    st_yellow <- openxlsx::createStyle(fontColour = "#ffcc00", fontSize = size, halign = "center", valign = "center")
    openxlsx::addStyle(wb, sheet, style = st_yellow, rows = rows_yellow, cols = col_idx, gridExpand = FALSE)
  }
  # Estilo Verde
  if (length(rows_green) > 0) {
    st_green <- openxlsx::createStyle(fontColour = "#2ecc71", fontSize = size, halign = "center", valign = "center")
    openxlsx::addStyle(wb, sheet, style = st_green, rows = rows_green, cols = col_idx, gridExpand = FALSE)
  }
}

# ------------------- Guardado Excel -------------------
save_results_to_excel <- function(summary_df, details_list, file) {
  wb <- openxlsx::createWorkbook()

  # ===== Glosario =====
  glosario <- build_glossary_df()
  openxlsx::addWorksheet(wb, "Glosario")
  openxlsx::writeData(wb, "Glosario", glosario)
  openxlsx::addStyle(wb, "Glosario", openxlsx::createStyle(textDecoration = "bold"),
    rows = 1, cols = 1:ncol(glosario), gridExpand = TRUE
  )
  openxlsx::freezePane(wb, "Glosario", firstRow = TRUE)
  openxlsx::setColWidths(wb, "Glosario", cols = 1:ncol(glosario), widths = "auto")

  # ===== Resumen =====
  openxlsx::addWorksheet(wb, "Resumen")

  # Columnas numéricas (para conversión; colores se eligen abajo)
  num_cols_all <- c(
    "Rank", "Puntaje (0-100)", "SemaforoScore",
    "Analistas %", "Crecimiento %", "Salud %", "Rentabilidad %",
    "Sentimiento %", "Técnica %", "Valoración %",
    "Target(Price-1 %)", "Price", "Target"
  )

  if (!is.null(summary_df) && nrow(summary_df) > 0) {
    for (cn in intersect(num_cols_all, names(summary_df))) {
      suppressWarnings(summary_df[[cn]] <- as.numeric(summary_df[[cn]]))
    }
  }

  if (!is.null(summary_df) && nrow(summary_df) > 0) {
    openxlsx::writeData(wb, "Resumen", summary_df)
    openxlsx::addStyle(wb, "Resumen", openxlsx::createStyle(textDecoration = "bold"),
      rows = 1, cols = 1:ncol(summary_df), gridExpand = TRUE
    )
    openxlsx::freezePane(wb, "Resumen", firstRow = TRUE)
    openxlsx::setColWidths(wb, "Resumen", cols = 1:ncol(summary_df), widths = "auto")

    cols_names <- colnames(summary_df)
    rows_rng <- 2:(nrow(summary_df) + 1)

    # 1) Rank con degradado INVERSO por COLUMNA
    rank_col <- which(cols_names == "Rank")
    if (length(rank_col)) .apply_colour_scale_per_column(wb, "Resumen", cols = rank_col, rows = rows_rng, inverted = TRUE)

    # 2) Otras numéricas con degradado normal por COLUMNA, excluyendo Price y Target
    grad_cols <- which(cols_names %in% setdiff(num_cols_all, c("Rank", "Price", "Target")))
    if (length(grad_cols)) .apply_colour_scale_per_column(wb, "Resumen", cols = grad_cols, rows = rows_rng, inverted = FALSE)

    # 3) Price y Target: SIN colores
  } else {
    openxlsx::writeData(wb, "Resumen", tibble::tibble(Info = "Sin datos"))
  }

  # ===== Hojas por ticker =====
  if (!is.null(details_list) && length(details_list) > 0) {
    for (tk in names(details_list)) {
      df <- details_list[[tk]]
      if (is.null(df) || nrow(df) == 0) next

      # Conversiones
      if ("Valor" %in% names(df)) df$`Valor` <- suppressWarnings(as.numeric(df$`Valor`))
      if ("Peso" %in% names(df)) df$`Peso` <- suppressWarnings(as.numeric(df$`Peso`))
      # NO convertimos Semáforo a numeric, es texto

      sheet_name <- substr(tk, 1, 31)
      openxlsx::addWorksheet(wb, sheet_name)
      openxlsx::writeData(wb, sheet_name, df)
      openxlsx::addStyle(wb, sheet_name, openxlsx::createStyle(textDecoration = "bold"),
        rows = 1, cols = 1:ncol(df), gridExpand = TRUE
      )
      openxlsx::freezePane(wb, sheet_name, firstRow = TRUE)
      openxlsx::setColWidths(wb, sheet_name, cols = 1:ncol(df), widths = "auto")

      # Semáforo: Colorear según texto
      if ("Semáforo" %in% names(df)) {
        col_sem <- which(colnames(df) == "Semáforo")
        .paint_semaforo_cells(wb, sheet = sheet_name, col_idx = col_sem, values = df$`Semáforo`)
      }
    }
  }

  openxlsx::saveWorkbook(wb, file, overwrite = TRUE)
}
