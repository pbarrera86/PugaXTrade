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
  library(jsonlite)
  library(lubridate)
  library(future)
  library(furrr)
})

# Shared %||% comes from helpers.R (or db.R in background workers).

# =========================================================
# 1) CONFIGURACIÓN GENERAL
# =========================================================

INST_DEFAULT_TICKERS <- c(
  "GPRO", "GT", "NU", "NVDA", "IBTA", "INTC", "LQDA", "PROK", "MARA", "MU", "BMNR", "RUN", "CIFR",
  "PLUG", "TSLA", "U", "ORCL", "SOFI", "SPCX", "AMC", "GLW", "META", "CLSK", "AMZN", "RR", "EOSE",
  "CRWV", "SPCE", "SMCI", "MSFT", "AMD", "GOOGL", "IONQ", "CPB", "NFLX", "MSTR", "SBET", "UBER",
  "FLGT", "PTON", "AVGO", "GOOG", "NCLH", "GRAB", "DNUT", "APP", "OSCR", "RIOT", "DDOG", "PLTR",
  "AAPL", "WULF", "VST", "CPRT", "HPE", "BAC", "NBIS", "BE", "NKE", "MELI", "PYPL", "RKLB", "QCOM",
  "QUBT", "RGTI", "FCEL", "CLF", "GLOB", "OCGN", "CRCL", "BTBT", "LULU", "ASTS", "MP", "VIAV",
  "COIN", "TE", "TBBB", "BBAI", "INO", "OKLO", "CPNG", "V", "PFE", "GEVO", "USAR", "CRWD", "ADBE",
  "HIMS", "LCID", "DIS", "CRDO", "UAL", "AAL", "ONON", "PANW", "MCD", "RIVN", "ZETA", "HOOD",
  "PEP", "C", "FCX", "CMG", "FSLY", "MPWR", "VZ", "QBTS", "LLY", "SOUN", "CELH", "SPOT", "NVTS",
  "UNH", "MCHP", "MRNA", "LYFT", "WMT", "MA", "CMCSA", "RY", "VRT", "SYM", "AMAT", "PICS", "ERNA",
  "CHTR", "CIEN", "SNOW", "AI", "YEXT", "KHC", "LRCX", "SBUX", "COST", "T", "AIR", "XYZ", "CVS",
  "WFC", "IBM", "ENR", "CSCO", "RXRX", "TDOC", "ATER", "LOW", "WDC", "KO", "CCJ", "COHR", "AXP",
  "TTD", "CAT", "ACN", "PINS", "GD", "URI", "RDDT", "CRM", "ABBV", "TTWO", "CBRS", "ANET", "MO",
  "AFRM", "JBL", "UPST", "PWR", "GEV", "BB", "LITE", "XOM", "DBX", "BA", "NEE", "BNGO", "HD",
  "STLA", "RCL", "BMY", "QTWO", "INTU", "ACHR", "F", "UAA", "TEM", "ADM", "ABT", "DECK", "APH",
  "SHW", "JPM", "TXN", "GME", "ROKU", "DUOL", "JNJ", "UPS", "HL", "STZ", "FOUR", "GS", "FSLR",
  "CCEP", "GIS", "PG", "WYNN", "FFIV", "CEG", "TMUS", "BKNG", "NVAX", "TSN", "SHOP", "SU", "SFM",
  "KLAC", "AMKR", "NET", "PATH", "FISV", "VMC", "JBLU", "CVX", "ASML", "TRMB", "STX", "PAAS",
  "ISRG", "BSX", "RTX", "CDE", "AMGN", "BX", "BLK", "CL", "BYND", "STLD", "WOOF", "TMO", "RACE",
  "SPGI", "BBWI", "GEHC", "COF", "CB", "ECHO", "ABNB", "DPZ", "TKR", "FVRR", "COP", "GE", "CHRD",
  "ZBRA", "MGM", "TGT", "MSCI", "MOS", "MRK", "CVNA", "AEM", "SNPS", "TDG", "ADSK", "FDXF", "EPAM",
  "RBLX", "ALAB", "AEP", "HPQ", "MNST", "TER", "CLX", "QS", "SNAP", "CME", "KMB", "CNC", "GILD",
  "SKYD", "LVS", "FDX", "ZTS", "MCO", "FTNT", "AMSC", "KD", "PGR", "NEM", "DHT", "ENPH", "MET",
  "KTOS", "DVN", "GPN", "DAL", "TRIP", "VEEV", "HSY", "MBLY", "AZO", "ALB", "DB", "KGC", "ILMN",
  "ZM", "RKT", "AVAV", "HONA", "MMM", "GM", "FLEX", "HON", "ON", "EW", "INSW", "CI", "NOC", "ATOS",
  "TJX", "LMT", "FLNA", "FDS", "BBY", "ZS", "MDLZ", "CRSP", "MTG", "WU", "TPR", "HUBS", "PBF",
  "MS", "VSNT", "MPC", "FIX", "PM", "VLO", "MBGL", "CNK", "ARCO", "SEDG", "UUUU", "MAT", "ORLY",
  "TRV", "EVTC", "LHX", "NDAQ", "HWM", "EL", "B", "APA", "OPK", "YUMC", "DHR", "TOST", "SOLV",
  "HBAN", "DLTR", "NTAP", "KMI", "FIS", "NTR", "GDDY", "BKR", "ODFL", "LEVI", "MDT", "LMND",
  "OKTA", "XYL", "WIX", "AON", "ULTA", "DXC", "KR", "NRG", "BTG", "EXPE", "ICE", "DDD", "MKC",
  "GNRC", "LDOS", "CHWY", "CROX", "NTRS", "NUE", "HUM", "TEAM", "CNI", "CEVA", "FCN", "BCPC",
  "EBS", "DJT", "UBS", "MUSA", "HE", "LIN", "NOW", "SCHW", "ETN", "SYK", "KKR", "UNP", "DE", "ADP",
  "ADI", "DASH", "SO", "VRTX", "CDNS", "HCA", "PH", "DUK", "MCK", "DELL", "WM", "TT", "APO",
  "CTAS", "PNC", "ITW", "AJG", "ECL", "MSI", "USB", "EMR", "JCI", "WMB", "MAR", "RSG", "ELV",
  "WDAY", "NSC", "HLT", "TEL", "APD", "EOG", "CMI", "AFL", "AXON", "REGN", "TFC", "CSX", "COR",
  "PCAR", "CARR", "SLB", "ALL", "IDXX", "PSX", "BDX", "FAST", "SRE", "ROP", "NXPI", "D", "AMP",
  "OKE", "WBD", "ROST", "GWW", "PAYX", "CTVA", "OXY", "GRMN", "FANG", "PEG", "ETR", "RMD", "AME",
  "EXC", "LYV", "SYY", "CCL", "AIG", "EBAY", "YUM", "XEL", "PRU", "A", "OTIS", "ACGL", "PCG",
  "RJF", "CTSH", "KVUE", "HIG", "KDP", "MLM", "FICO", "ROK", "WEC", "TRGP", "VRSK", "ED", "CAH",
  "VRSN", "AEE", "ROL", "AWK", "MTB", "IR", "TSCO", "STT", "EQT", "IQV", "DD", "WAB", "EFX", "WTW",
  "WRB", "IBKR", "SYF", "DTE", "BR", "DXCM", "KEYS", "FITB", "BRO", "ES", "DOV", "STE", "DRI",
  "CBOE", "RF", "CINF", "WSM", "PTC", "CNP", "IP", "PPG", "FE", "MTD", "ATO", "TDY", "VLTO", "PPL",
  "CHD", "CFG", "PODD", "LH", "BIIB", "CMS", "TPL", "EIX", "CDW", "CPAY", "HUBB", "TROW", "SW",
  "DG", "TYL", "EXE", "L", "DGX", "KEY", "LUV", "ERIE", "INCY", "TKO", "FTV", "IFF", "EVRG", "BG",
  "LNT", "CHRW", "MAS", "ALLE", "BLDR", "OMC", "TXT", "GEN", "J", "DOW", "SNA", "LYB", "EXPD",
  "ZBH", "GPC", "AMCR", "LII", "PKG", "HAL", "IT", "NI", "RL", "WST", "PNR", "WAT", "APTV", "PFG",
  "RVTY", "PNW", "DVA", "CAG", "MTCH", "KMX", "AES", "AOS", "AIZ", "COO", "NDSN", "BEN", "FOX",
  "FOXA", "SWK", "IEX", "ALGN", "TAP", "MOH", "HAS", "IVZ", "EG", "HRL", "CF", "BALL", "JBHT",
  "AVY", "PAYC", "UHS", "JKHY", "GL", "VTRS", "NWSA", "SJM", "SWKS", "AKAM", "POOL", "BAX", "HII",
  "EMN", "CZR", "NWS", "CRL", "MKTX", "LW", "LKQ", "TECH", "MHK", "HSIC"
)

inst_default_tickers_text <- function() {
  paste(INST_DEFAULT_TICKERS, collapse = ", ")
}

source("R/utils_core.R", local = TRUE)

inst_parse_tickers <- utils_parse_tickers
as_num <- utils_as_num
as_pct <- utils_as_pct
.http_get <- utils_http_get


# =========================================================
# 2) CACHÉ LOCAL / POSTGRES
# =========================================================

.cache_dir <- function() {
  path <- file.path(getwd(), "cache")
  if (!dir.exists(path)) dir.create(path, recursive = TRUE, showWarnings = FALSE)
  path
}

cache_local_file <- function(ticker) {
  file.path(.cache_dir(), paste0(ticker, ".rds"))
}

cache_get_local <- function(ticker, max_age_hours = 12) {
  f <- cache_local_file(ticker)
  if (!file.exists(f)) {
    return(NULL)
  }

  obj <- tryCatch(readRDS(f), error = function(e) NULL)
  if (is.null(obj)) {
    return(NULL)
  }

  age_hours <- as.numeric(difftime(Sys.time(), obj$fetched_at, units = "hours"))
  if (is.na(age_hours) || age_hours > max_age_hours) {
    return(NULL)
  }

  obj$data
}

cache_set_local <- function(ticker, data) {
  f <- cache_local_file(ticker)
  tryCatch(
    saveRDS(list(fetched_at = Sys.time(), data = data), f),
    error = function(e) invisible(FALSE)
  )
  invisible(TRUE)
}

inst_can_use_postgres_cache <- function() {
  tryCatch(
    {
      nzchar(Sys.getenv("PGHOST")) &&
        nzchar(Sys.getenv("PGDATABASE")) &&
        nzchar(Sys.getenv("PGUSER")) &&
        nzchar(Sys.getenv("PGPASSWORD")) &&
        requireNamespace("DBI", quietly = TRUE) &&
        requireNamespace("RPostgres", quietly = TRUE) &&
        requireNamespace("pool", quietly = TRUE)
    },
    error = function(e) FALSE
  )
}

pg_pool_global <- NULL
pg_cache_initialized <- FALSE # Flag para evitar CREATE TABLE repetido

pg_pool <- function() {
  if (!inst_can_use_postgres_cache()) stop("No hay configuración PG válida para caché Postgres.")

  if (!is.null(pg_pool_global)) {
    ok <- tryCatch(DBI::dbIsValid(pg_pool_global), error = function(e) FALSE)
    if (ok) {
      return(pg_pool_global)
    }
  }

  pg_pool_global <<- pool::dbPool(
    drv = RPostgres::Postgres(),
    host = Sys.getenv("PGHOST"),
    port = as.integer(Sys.getenv("PGPORT", "5432")),
    dbname = Sys.getenv("PGDATABASE"),
    user = Sys.getenv("PGUSER"),
    password = Sys.getenv("PGPASSWORD"),
    sslmode = Sys.getenv("PGSSLMODE", "require"),
    idleTimeout = 300,
    minSize = 1,
    maxSize = 2
  )

  pg_pool_global
}

pg_cache_init <- function() {
  # Solo ejecutar una vez por sesión para evitar NOTICEs repetidos de Postgres
  if (isTRUE(pg_cache_initialized)) {
    return(invisible(TRUE))
  }

  p <- pg_pool()
  DBI::dbGetQuery(p, "SELECT ticker FROM ticker_cache LIMIT 0")
  pg_cache_initialized <<- TRUE
  invisible(TRUE)
}

cache_get_pg <- function(ticker, max_age_hours = 12) {
  if (!inst_can_use_postgres_cache()) {
    return(NULL)
  }

  tryCatch(
    {
      pg_cache_init()
      p <- pg_pool()

      q <- DBI::dbGetQuery(
        p,
        "
      SELECT payload, fetched_at
      FROM ticker_cache
      WHERE ticker = $1
      LIMIT 1
      ",
        params = list(ticker)
      )

      if (nrow(q) == 0) {
        return(NULL)
      }

      age_hours <- as.numeric(difftime(Sys.time(), q$fetched_at[[1]], units = "hours"))
      if (is.na(age_hours) || age_hours > max_age_hours) {
        return(NULL)
      }

      jsonlite::fromJSON(as.character(q$payload[[1]]), simplifyVector = FALSE)
    },
    error = function(e) {
      NULL
    }
  )
}

cache_set_pg <- function(ticker, data) {
  if (!inst_can_use_postgres_cache()) {
    return(invisible(FALSE))
  }

  tryCatch(
    {
      pg_cache_init()
      p <- pg_pool()
      js <- jsonlite::toJSON(data, auto_unbox = TRUE, null = "null")

      DBI::dbExecute(
        p,
        "
      INSERT INTO ticker_cache (ticker, fetched_at, payload)
      VALUES ($1, now(), $2::jsonb)
      ON CONFLICT (ticker)
      DO UPDATE SET fetched_at = EXCLUDED.fetched_at, payload = EXCLUDED.payload
      ",
        params = list(ticker, js)
      )
      invisible(TRUE)
    },
    error = function(e) {
      invisible(FALSE)
    }
  )
}

cache_get <- function(ticker, use_postgres_cache = FALSE, max_age_hours = 12) {
  if (use_postgres_cache) {
    x <- cache_get_pg(ticker, max_age_hours)
    if (!is.null(x)) {
      return(x)
    }
  }
  cache_get_local(ticker, max_age_hours)
}

cache_set <- function(ticker, data, use_postgres_cache = FALSE) {
  cache_set_local(ticker, data)
  if (use_postgres_cache) cache_set_pg(ticker, data)
  invisible(TRUE)
}

# =========================================================
# 3) GLOSARIO Y UNIDADES
# =========================================================

inst_build_glossary_df <- function() {
  tibble::tribble(
    ~Indicador, ~Unidad, ~Definición, ~Interpretación,
    "P/U", "", "Precio actual dividido entre utilidad por acción.", "Menor suele ser mejor, pero depende del crecimiento.",
    "PEG", "", "Relación entre P/U y crecimiento esperado.", "Menor suele ser mejor.",
    "P/U futuro", "", "Precio actual frente a utilidad futura estimada.", "Menor suele ser mejor.",
    "P/Ventas", "", "Precio respecto a ventas por acción.", "Menor suele ser mejor.",
    "P/FCF", "", "Precio respecto a flujo de caja libre.", "Menor suele ser mejor.",
    "EV/EBITDA", "", "Valor empresa frente a EBITDA.", "Menor suele ser mejor.",
    "P/Libros", "", "Precio respecto a valor en libros.", "Menor suele ser mejor en negocios intensivos en activos.",
    "EPS interanual TTM", "%", "Crecimiento de utilidad por acción en último año.", "Mayor suele ser mejor.",
    "Ventas interanual TTM", "%", "Crecimiento de ventas en último año.", "Mayor suele ser mejor.",
    "EPS próximo año", "%", "Crecimiento esperado de utilidad por acción el próximo año.", "Mayor suele ser mejor.",
    "EPS próximos 5 años", "%", "Crecimiento compuesto esperado a cinco años.", "Mayor suele ser mejor.",
    "Ventas Trim/Trim", "%", "Crecimiento trimestral de ventas vs año anterior.", "Mayor suele ser mejor.",
    "EPS Trim/Trim", "%", "Crecimiento trimestral de EPS vs año anterior.", "Mayor suele ser mejor.",
    "Margen bruto", "%", "Rentabilidad antes de gastos operativos.", "Mayor suele ser mejor.",
    "Margen operativo", "%", "Rentabilidad operativa sobre ventas.", "Mayor suele ser mejor.",
    "Margen neto", "%", "Utilidad neta sobre ventas.", "Mayor suele ser mejor.",
    "ROE", "%", "Retorno sobre patrimonio.", "Mayor suele ser mejor.",
    "ROIC", "%", "Retorno sobre capital invertido.", "Mayor suele ser mejor.",
    "ROA", "%", "Retorno sobre activos.", "Mayor suele ser mejor.",
    "Deuda/Patrimonio", "", "Apalancamiento financiero.", "Menor suele ser mejor.",
    "Liquidez corriente", "", "Activos corrientes sobre pasivos corrientes.", "Mayor suele ser mejor.",
    "Dist. SMA20", "%", "Distancia del precio respecto a media de 20 días.", "Positivo suele ser mejor.",
    "Dist. SMA50", "%", "Distancia del precio respecto a media de 50 días.", "Positivo suele ser mejor.",
    "Dist. SMA200", "%", "Distancia del precio respecto a media de 200 días.", "Positivo suele ser mejor.",
    "RSI (14)", "pts", "Índice de fuerza relativa.", "Zona media suele ser más sana que extremos.",
    "Interés corto", "%", "Porcentaje de acciones vendidas en corto.", "Menor suele ser mejor.",
    "Propiedad institucional", "%", "Porcentaje en manos institucionales.", "Moderado-alto suele ser positivo.",
    "Transacciones internas", "%", "Cambio en propiedad insider.", "Positivo suele ser mejor.",
    "Payout", "%", "Porcentaje de utilidad distribuida.", "Zona media suele ser más sana.",
    "Potencial a objetivo", "%", "Distancia al precio objetivo promedio.", "Mayor suele ser mejor.",
    "Recomendación", "", "Escala de analistas donde 1 es compra fuerte y 5 es venta.", "Menor suele ser mejor."
  )
}

metric_unit_map <- function() {
  c(
    "Precio actual (USD)" = "USD",
    "Precio objetivo (USD)" = "USD",
    "P/U" = "",
    "PEG" = "",
    "P/U futuro" = "",
    "P/Ventas" = "",
    "P/FCF" = "",
    "EV/EBITDA" = "",
    "P/Libros" = "",
    "EPS interanual TTM" = "%",
    "Ventas interanual TTM" = "%",
    "EPS próximo año" = "%",
    "EPS próximos 5 años" = "%",
    "Ventas Trim/Trim" = "%",
    "EPS Trim/Trim" = "%",
    "Margen bruto" = "%",
    "Margen operativo" = "%",
    "Margen neto" = "%",
    "ROE" = "%",
    "ROIC" = "%",
    "ROA" = "%",
    "Deuda/Patrimonio" = "",
    "Liquidez corriente" = "",
    "Dist. SMA20" = "%",
    "Dist. SMA50" = "%",
    "Dist. SMA200" = "%",
    "RSI (14)" = "pts",
    "Interés corto" = "%",
    "Propiedad institucional" = "%",
    "Transacciones internas" = "%",
    "Payout" = "%",
    "Potencial a objetivo" = "%",
    "Recomendación" = ""
  )
}

# =========================================================
# 4) FINVIZ + YAHOO
# =========================================================

.read_finviz_table <- function(sym) {
  Sys.sleep(runif(1, min = 0.5, max = 2.0)) # Jitter to prevent 429 bans
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

  kv <- html_elements(nodes, "td")
  txt <- html_text(kv, trim = TRUE)

  if (length(txt) %% 2 != 0) txt <- head(txt, -1)
  if (length(txt) < 4) {
    return(NULL)
  }

  keys <- txt[seq(1, length(txt), by = 2)]
  vals <- txt[seq(2, length(txt), by = 2)]

  out <- setNames(as.list(vals), keys)
  out$raw_keys <- keys
  out$raw_vals <- vals

  # 1) Obtener Sector - por href (f=sec_...), no por posición del link.
  # Finviz reutiliza la clase "a.tab-link" también en las filas de Peers/Held by,
  # así que indexar por posición (ej. links[2]) es frágil y termina devolviendo
  # texto de esas otras filas (ej. "Peers") en vez del sector real.
  sector <- tryCatch(
    {
      sec_links <- rvest::html_elements(html, "a[href*='f=sec_']")
      v <- NA_character_
      if (length(sec_links) >= 1) {
        txt <- trimws(html_text(sec_links[1]))
        if (nchar(txt) > 0 && nchar(txt) < 40) v <- txt
      }
      v
    },
    error = function(e) NA_character_
  )

  # Fallback local dict para principales tickers si falla el web scraping
  if (is.na(sector) || sector == "") {
    local_dict <- c(
      "NVDA" = "Tecnología",
      "MSFT" = "Tecnología",
      "AAPL" = "Tecnología",
      "AMZN" = "Consumo discrecional",
      "META" = "Servicios de comunicación",
      "AVGO" = "Tecnología",
      "GOOGL" = "Servicios de comunicación",
      "GOOG" = "Servicios de comunicación",
      "TSLA" = "Consumo discrecional",
      "JPM" = "Finanzas",
      "ORCL" = "Tecnología",
      "WMT" = "Consumo básico",
      "LLY" = "Salud",
      "V" = "Finanzas",
      "MA" = "Finanzas",
      "NFLX" = "Servicios de comunicación",
      "XOM" = "Energía",
      "JNJ" = "Salud",
      "HD" = "Consumo discrecional",
      "COST" = "Consumo básico",
      "ABBV" = "Salud",
      "PLTR" = "Tecnología",
      "BAC" = "Finanzas",
      "PG" = "Consumo básico",
      "CVX" = "Energía",
      "UNH" = "Salud",
      "GE" = "Industriales",
      "KO" = "Consumo básico",
      "CSCO" = "Tecnología",
      "TMUS" = "Servicios de comunicación",
      "WFC" = "Finanzas",
      "AMD" = "Tecnología",
      "PM" = "Consumo básico",
      "MS" = "Finanzas",
      "IBM" = "Tecnología",
      "GS" = "Finanzas",
      "ABT" = "Salud",
      "NVO" = "Salud",
      "FCN" = "Industriales",
      "BCPC" = "Materiales",
      "RIVN" = "Consumo discrecional",
      "BABA" = "Consumo discrecional",
      "TSM" = "Tecnología",
      "EBS" = "Salud",
      "ASML" = "Tecnología",
      "DJT" = "Servicios de comunicación",
      "MSTR" = "Tecnología",
      "MARA" = "Finanzas",
      "UBS" = "Finanzas",
      "SHEL" = "Energía",
      "CLSK" = "Tecnología",
      "AVAV" = "Industriales",
      "MUSA" = "Consumo discrecional",
      "BBVA" = "Finanzas",
      "HE" = "Servicios públicos",
      "SOFI" = "Finanzas",
      "NIO" = "Consumo discrecional",
      "TAL" = "Consumo básico",
      "NVAX" = "Salud",
      "AXP" = "Finanzas",
      "CRM" = "Tecnología",
      "LIN" = "Materiales",
      "MCD" = "Consumo discrecional",
      "RTX" = "Industriales",
      "T" = "Servicios de comunicación",
      "CAT" = "Industriales",
      "DIS" = "Servicios de comunicación",
      "MRK" = "Salud",
      "UBER" = "Tecnología",
      "PEP" = "Consumo básico",
      "NOW" = "Tecnología",
      "C" = "Finanzas",
      "INTU" = "Tecnología",
      "VZ" = "Servicios de comunicación",
      "QCOM" = "Tecnología",
      "ANET" = "Tecnología",
      "MU" = "Tecnología",
      "TMO" = "Salud",
      "BKNG" = "Consumo discrecional",
      "BLK" = "Finanzas",
      "GEV" = "Servicios públicos",
      "SPGI" = "Finanzas",
      "BA" = "Industriales",
      "SCHW" = "Finanzas",
      "TXN" = "Tecnología",
      "TJX" = "Consumo discrecional",
      "ISRG" = "Salud",
      "LRCX" = "Tecnología",
      "ADBE" = "Tecnología",
      "LOW" = "Consumo discrecional",
      "ACN" = "Tecnología",
      "NEE" = "Servicios públicos",
      "AMGN" = "Salud",
      "ETN" = "Industriales",
      "PGR" = "Finanzas",
      "BX" = "Finanzas",
      "APH" = "Tecnología",
      "AMAT" = "Tecnología",
      "SYK" = "Salud",
      "COF" = "Finanzas",
      "BSX" = "Salud",
      "HON" = "Industriales",
      "DHR" = "Salud",
      "PANW" = "Tecnología",
      "GILD" = "Salud",
      "PFE" = "Salud",
      "KKR" = "Finanzas",
      "KLAC" = "Tecnología",
      "UNP" = "Industriales",
      "DE" = "Industriales",
      "INTC" = "Tecnología",
      "COP" = "Energía",
      "ADP" = "Industriales",
      "CMCSA" = "Servicios de comunicación",
      "ADI" = "Tecnología",
      "MDT" = "Salud",
      "LMT" = "Industriales",
      "CRWD" = "Tecnología",
      "WELL" = "Bienes raíces",
      "DASH" = "Servicios de comunicación",
      "NKE" = "Consumo discrecional",
      "MO" = "Consumo básico",
      "PLD" = "Bienes raíces",
      "CB" = "Finanzas",
      "SO" = "Servicios públicos",
      "ICE" = "Finanzas",
      "CEG" = "Servicios públicos",
      "VRTX" = "Salud",
      "CDNS" = "Tecnología",
      "HCA" = "Salud",
      "MCO" = "Finanzas",
      "PH" = "Industriales",
      "CME" = "Finanzas",
      "AMT" = "Bienes raíces",
      "SBUX" = "Consumo discrecional",
      "MMC" = "Finanzas",
      "BMY" = "Salud",
      "CVS" = "Salud",
      "DUK" = "Servicios públicos",
      "GD" = "Industriales",
      "MCK" = "Salud",
      "NEM" = "Materiales",
      "ORLY" = "Consumo discrecional",
      "DELL" = "Tecnología",
      "SHW" = "Materiales",
      "WM" = "Industriales",
      "TT" = "Industriales",
      "RCL" = "Consumo discrecional",
      "COIN" = "Tecnología",
      "NOC" = "Industriales",
      "APO" = "Finanzas",
      "CTAS" = "Industriales",
      "PNC" = "Finanzas",
      "MDLZ" = "Consumo básico",
      "MMM" = "Industriales",
      "ITW" = "Industriales",
      "EQIX" = "Bienes raíces",
      "BK" = "Finanzas",
      "AJG" = "Finanzas",
      "ABNB" = "Servicios de comunicación",
      "ECL" = "Materiales",
      "AON" = "Finanzas",
      "SNPS" = "Tecnología",
      "CI" = "Salud",
      "MSI" = "Tecnología",
      "USB" = "Finanzas",
      "HWM" = "Industriales",
      "TDG" = "Industriales",
      "UPS" = "Industriales",
      "FI" = "Energía",
      "EMR" = "Industriales",
      "AZO" = "Consumo discrecional",
      "JCI" = "Industriales",
      "WMB" = "Energía",
      "VST" = "Servicios públicos",
      "MAR" = "Consumo discrecional",
      "RSG" = "Industriales",
      "ELV" = "Salud",
      "WDAY" = "Tecnología",
      "NSC" = "Industriales",
      "HLT" = "Consumo discrecional",
      "MNST" = "Consumo básico",
      "PYPL" = "Finanzas",
      "TEL" = "Tecnología",
      "APD" = "Materiales",
      "CL" = "Consumo básico",
      "GLW" = "Tecnología",
      "ZTS" = "Salud",
      "FCX" = "Materiales",
      "EOG" = "Energía",
      "ADSK" = "Tecnología",
      "CMI" = "Industriales",
      "AFL" = "Finanzas",
      "FTNT" = "Tecnología",
      "KMI" = "Energía",
      "AXON" = "Industriales",
      "SPG" = "Bienes raíces",
      "TRV" = "Finanzas",
      "REGN" = "Salud",
      "TFC" = "Finanzas",
      "DLR" = "Bienes raíces",
      "URI" = "Industriales",
      "CSX" = "Industriales",
      "COR" = "Bienes raíces",
      "AEP" = "Servicios públicos",
      "NDAQ" = "Finanzas",
      "CMG" = "Consumo discrecional",
      "FDX" = "Industriales",
      "PCAR" = "Industriales",
      "VLO" = "Energía",
      "CARR" = "Industriales",
      "MET" = "Finanzas",
      "SLB" = "Energía",
      "ALL" = "Finanzas",
      "IDXX" = "Salud",
      "PSX" = "Energía",
      "BDX" = "Salud",
      "FAST" = "Industriales",
      "SRE" = "Servicios públicos",
      "O" = "Bienes raíces",
      "ROP" = "Industriales",
      "GM" = "Consumo discrecional",
      "MPC" = "Energía",
      "PWR" = "Industriales",
      "NXPI" = "Tecnología",
      "LHX" = "Industriales",
      "D" = "Servicios públicos",
      "DHI" = "Consumo discrecional",
      "MSCI" = "Finanzas",
      "AMP" = "Finanzas",
      "OKE" = "Energía",
      "STX" = "Tecnología",
      "WBD" = "Servicios de comunicación",
      "CPRT" = "Industriales",
      "PSA" = "Bienes raíces",
      "ROST" = "Consumo discrecional",
      "CBRE" = "Bienes raíces",
      "GWW" = "Industriales",
      "PAYX" = "Industriales",
      "DDOG" = "Tecnología",
      "CTVA" = "Materiales",
      "XYZ" = "Finanzas",
      "BKR" = "Energía",
      "OXY" = "Energía",
      "F" = "Consumo discrecional",
      "GRMN" = "Tecnología",
      "TTWO" = "Servicios de comunicación",
      "FANG" = "Energía",
      "PEG" = "Servicios públicos",
      "HSY" = "Consumo básico",
      "VMC" = "Materiales",
      "ETR" = "Servicios públicos",
      "RMD" = "Salud",
      "AME" = "Industriales",
      "EXC" = "Servicios públicos",
      "EW" = "Salud",
      "KR" = "Consumo básico",
      "LYV" = "Servicios de comunicación",
      "SYY" = "Consumo básico",
      "CCI" = "Bienes raíces",
      "KMB" = "Consumo básico",
      "CCL" = "Consumo discrecional",
      "AIG" = "Finanzas",
      "TGT" = "Consumo básico",
      "EBAY" = "Consumo discrecional",
      "MPWR" = "Tecnología",
      "YUM" = "Consumo discrecional",
      "EA" = "Servicios de comunicación",
      "XEL" = "Servicios públicos",
      "PRU" = "Finanzas",
      "A" = "Salud",
      "GEHC" = "Salud",
      "OTIS" = "Industriales",
      "ACGL" = "Finanzas",
      "PCG" = "Servicios públicos",
      "RJF" = "Finanzas",
      "UAL" = "Industriales",
      "CTSH" = "Tecnología",
      "XYL" = "Industriales",
      "KVUE" = "Consumo básico",
      "LVS" = "Consumo discrecional",
      "CHTR" = "Servicios de comunicación",
      "HIG" = "Finanzas",
      "KDP" = "Consumo básico",
      "MLM" = "Materiales",
      "FICO" = "Tecnología",
      "CSGP" = "Bienes raíces",
      "DAL" = "Industriales",
      "ROK" = "Industriales",
      "NUE" = "Materiales",
      "LEN" = "Consumo discrecional",
      "WEC" = "Servicios públicos",
      "TRGP" = "Energía",
      "MCHP" = "Tecnología",
      "VRSK" = "Industriales",
      "WDC" = "Tecnología",
      "VICI" = "Bienes raíces",
      "ED" = "Servicios públicos",
      "CAH" = "Salud",
      "FIS" = "Tecnología",
      "PHM" = "Consumo discrecional",
      "VRSN" = "Tecnología",
      "AEE" = "Servicios públicos",
      "K" = "Consumo básico",
      "ROL" = "Consumo discrecional",
      "AWK" = "Servicios públicos",
      "MTB" = "Finanzas",
      "IR" = "Industriales",
      "VTR" = "Bienes raíces",
      "TSCO" = "Consumo discrecional",
      "STT" = "Finanzas",
      "NRG" = "Servicios públicos",
      "EQT" = "Energía",
      "IQV" = "Salud",
      "EL" = "Consumo básico",
      "DD" = "Materiales",
      "WAB" = "Industriales",
      "EFX" = "Industriales",
      "HUM" = "Salud",
      "WTW" = "Consumo discrecional",
      "HPE" = "Tecnología",
      "AVB" = "Bienes raíces",
      "WRB" = "Finanzas",
      "IBKR" = "Finanzas",
      "EXPE" = "Consumo discrecional",
      "SYF" = "Finanzas",
      "DTE" = "Servicios públicos",
      "BR" = "Tecnología",
      "IRM" = "Bienes raíces",
      "DXCM" = "Salud",
      "KEYS" = "Tecnología",
      "ADM" = "Consumo básico",
      "FITB" = "Finanzas",
      "BRO" = "Finanzas",
      "EXR" = "Bienes raíces",
      "ODFL" = "Industriales",
      "KHC" = "Consumo básico",
      "ES" = "Servicios públicos",
      "DOV" = "Industriales",
      "ULTA" = "Consumo discrecional",
      "STE" = "Salud",
      "DRI" = "Consumo discrecional",
      "CBOE" = "Finanzas",
      "STZ" = "Consumo básico",
      "RF" = "Finanzas",
      "CINF" = "Finanzas",
      "WSM" = "Consumo discrecional",
      "PTC" = "Tecnología",
      "CNP" = "Servicios públicos",
      "EQR" = "Bienes raíces",
      "IP" = "Consumo discrecional",
      "NTAP" = "Tecnología",
      "PPG" = "Materiales",
      "NTRS" = "Finanzas",
      "HBAN" = "Finanzas",
      "FE" = "Servicios públicos",
      "MTD" = "Salud",
      "HPQ" = "Tecnología",
      "ATO" = "Servicios públicos",
      "GIS" = "Consumo básico",
      "TDY" = "Tecnología",
      "VLTO" = "Industriales",
      "PPL" = "Servicios públicos",
      "SMCI" = "Tecnología",
      "DVN" = "Energía",
      "CHD" = "Consumo básico",
      "FSLR" = "Tecnología",
      "CFG" = "Finanzas",
      "PODD" = "Salud",
      "JBL" = "Tecnología",
      "LH" = "Salud",
      "TPR" = "Consumo discrecional",
      "BIIB" = "Salud",
      "CMS" = "Servicios públicos",
      "TPL" = "Energía",
      "EIX" = "Servicios públicos",
      "SBAC" = "Bienes raíces",
      "CDW" = "Tecnología",
      "CPAY" = "Tecnología",
      "TTD" = "Tecnología",
      "NVR" = "Consumo discrecional",
      "HUBB" = "Industriales",
      "TROW" = "Finanzas",
      "SW" = "Consumo discrecional",
      "DG" = "Consumo básico",
      "TYL" = "Tecnología",
      "EXE" = "Energía",
      "LDOS" = "Tecnología",
      "GDDY" = "Tecnología",
      "DLTR" = "Consumo básico",
      "L" = "Finanzas",
      "ON" = "Tecnología",
      "STLD" = "Materiales",
      "DGX" = "Salud",
      "KEY" = "Finanzas",
      "GPN" = "Industriales",
      "LUV" = "Industriales",
      "ERIE" = "Finanzas",
      "INCY" = "Salud",
      "TKO" = "Servicios de comunicación",
      "FTV" = "Tecnología",
      "IFF" = "Materiales",
      "EVRG" = "Servicios públicos",
      "MAA" = "Bienes raíces",
      "BG" = "Consumo básico",
      "LNT" = "Servicios públicos",
      "ZBRA" = "Tecnología",
      "CHRW" = "Industriales",
      "BBY" = "Consumo discrecional",
      "CNC" = "Salud",
      "MAS" = "Industriales",
      "CLX" = "Consumo básico",
      "ALLE" = "Industriales",
      "BLDR" = "Industriales",
      "DPZ" = "Consumo discrecional",
      "KIM" = "Bienes raíces",
      "OMC" = "Servicios de comunicación",
      "TXT" = "Industriales",
      "MKC" = "Consumo básico",
      "WY" = "Bienes raíces",
      "GEN" = "Salud",
      "J" = "Industriales",
      "DECK" = "Consumo discrecional",
      "DOW" = "Materiales",
      "SNA" = "Industriales",
      "ESS" = "Bienes raíces",
      "LYB" = "Materiales",
      "EXPD" = "Industriales",
      "ZBH" = "Salud",
      "PSKY" = "Servicios de comunicación",
      "GPC" = "Consumo discrecional",
      "LULU" = "Consumo discrecional",
      "TSN" = "Consumo básico",
      "AMCR" = "Consumo discrecional",
      "LII" = "Industriales",
      "PKG" = "Consumo discrecional",
      "TRMB" = "Tecnología",
      "HAL" = "Energía",
      "IT" = "Tecnología",
      "NI" = "Servicios públicos",
      "RL" = "Consumo discrecional",
      "FFIV" = "Tecnología",
      "WST" = "Salud",
      "CTRA" = "Energía",
      "INVH" = "Bienes raíces",
      "PNR" = "Industriales",
      "TER" = "Tecnología",
      "WAT" = "Salud",
      "APTV" = "Consumo discrecional",
      "PFG" = "Finanzas",
      "RVTY" = "Salud",
      "PNW" = "Servicios públicos",
      "ALB" = "Materiales",
      "DVA" = "Salud",
      "CAG" = "Consumo básico",
      "MTCH" = "Servicios de comunicación",
      "KMX" = "Consumo discrecional",
      "AES" = "Servicios públicos",
      "AOS" = "Industriales",
      "AIZ" = "Finanzas",
      "COO" = "Salud",
      "REG" = "Bienes raíces",
      "DOC" = "Bienes raíces",
      "SOLV" = "Salud",
      "NDSN" = "Industriales",
      "WYNN" = "Consumo discrecional",
      "BEN" = "Finanzas",
      "FOX" = "Servicios de comunicación",
      "UDR" = "Bienes raíces",
      "FOXA" = "Servicios de comunicación",
      "BXP" = "Bienes raíces",
      "SWK" = "Industriales",
      "IEX" = "Industriales",
      "MGM" = "Consumo discrecional",
      "ALGN" = "Salud",
      "TAP" = "Consumo básico",
      "MOH" = "Salud",
      "MRNA" = "Salud",
      "HAS" = "Consumo discrecional",
      "IPG" = "Servicios de comunicación",
      "IVZ" = "Finanzas",
      "CPB" = "Consumo básico",
      "ARE" = "Bienes raíces",
      "HOLX" = "Salud",
      "EG" = "Finanzas",
      "HRL" = "Consumo básico",
      "CF" = "Materiales",
      "BALL" = "Consumo discrecional",
      "JBHT" = "Industriales",
      "AVY" = "Industriales",
      "FDS" = "Finanzas",
      "PAYC" = "Tecnología",
      "UHS" = "Salud",
      "JKHY" = "Tecnología",
      "NCLH" = "Consumo discrecional",
      "CPT" = "Bienes raíces",
      "GL" = "Finanzas",
      "VTRS" = "Salud",
      "NWSA" = "Servicios de comunicación",
      "SJM" = "Consumo básico",
      "SWKS" = "Tecnología",
      "DAY" = "Tecnología",
      "MOS" = "Materiales",
      "AKAM" = "Tecnología",
      "POOL" = "Consumo discrecional",
      "HST" = "Bienes raíces",
      "BAX" = "Salud",
      "GNRC" = "Industriales",
      "HII" = "Industriales",
      "EMN" = "Materiales",
      "CZR" = "Consumo discrecional",
      "NWS" = "Servicios de comunicación",
      "CRL" = "Salud",
      "MKTX" = "Finanzas",
      "LW" = "Consumo básico",
      "EPAM" = "Tecnología",
      "APA" = "Energía",
      "LKQ" = "Consumo discrecional",
      "TECH" = "Salud",
      "FRT" = "Bienes raíces",
      "MHK" = "Consumo discrecional",
      "HSIC" = "Salud",
      "ENPH" = "Tecnología",
      "CRCL" = "Finanzas",
      "NU" = "Finanzas",
      "NET" = "Tecnología",
      "JD" = "Consumo discrecional",
      "SHOP" = "Tecnología",
      "MELI" = "Consumo discrecional",
      "PTON" = "Consumo discrecional"
    )
    if (sym %in% names(local_dict)) sector <- unname(local_dict[sym]) # Changed 'ticker' to 'sym'
  }

  out[["raw_Sector"]] <- sector

  out
}

getv_robust <- function(x, keys) {
  if (is.null(x)) {
    return(NA_character_)
  }
  for (k in keys) {
    if (!is.null(x[[k]])) {
      return(x[[k]])
    }
  }
  NA_character_
}

# ---------------------------------------------------------
# Yahoo Finanzas: fundamentales para tickers .MX (Bolsa Mexicana / SIC)
# Finviz solo cubre EE. UU.; para .MX se arma el mismo formato de "payload"
# (lista de textos con las mismas llaves que Finviz) desde quoteSummary de Yahoo.
# ---------------------------------------------------------
.yahoo_quote_summary <- function(sym) {
  modules <- "price,summaryDetail,defaultKeyStatistics,financialData,assetProfile,earningsTrend,quoteType"
  base <- sprintf("https://query2.finance.yahoo.com/v10/finance/quoteSummary/%s?modules=%s",
    utils::URLencode(sym, reserved = TRUE), modules)
  ses <- tryCatch(quantmod:::.yahooSession(), error = function(e) NULL)
  txt <- NULL
  for (intento in 1:2) {
    txt <- tryCatch({
      if (!is.null(ses) && !is.null(ses$h)) {
        url <- if (!is.null(ses$crumb)) paste0(base, "&crumb=", utils::URLencode(ses$crumb, reserved = TRUE)) else base
        r <- curl::curl_fetch_memory(url, handle = ses$h)
        if (r$status_code == 200) rawToChar(r$content) else NULL
      } else {
        r <- httr::GET(base, httr::user_agent("Mozilla/5.0"), httr::timeout(20))
        if (httr::status_code(r) == 200) httr::content(r, as = "text", encoding = "UTF-8") else NULL
      }
    }, error = function(e) NULL)
    if (!is.null(txt)) break
    Sys.sleep(1)
    ses <- tryCatch(quantmod:::.yahooSession(is.retry = TRUE), error = function(e) ses)
  }
  if (is.null(txt)) return(NULL)
  js <- tryCatch(jsonlite::fromJSON(txt, simplifyVector = FALSE), error = function(e) NULL)
  res <- js$quoteSummary$result
  if (is.null(res) || length(res) == 0) return(NULL)
  res[[1]]
}

.read_yahoo_fundamentals <- function(sym) {
  j <- .yahoo_quote_summary(sym)
  if (is.null(j)) return(NULL)
  v <- function(mod, key) {
    x <- j[[mod]][[key]]
    if (is.list(x)) x <- x$raw
    if (is.null(x) || length(x) == 0 || !is.numeric(x)) NA_real_ else as.numeric(x)
  }
  num <- function(x, d = 4) if (is.na(x)) "-" else formatC(x, format = "f", digits = d)
  pct <- function(x) if (is.na(x)) "-" else paste0(formatC(100 * x, format = "f", digits = 2), "%")

  price <- v("financialData", "currentPrice")
  if (is.na(price)) price <- v("price", "regularMarketPrice")
  if (is.na(price)) return(NULL)

  mcap <- v("price", "marketCap")
  if (is.na(mcap)) mcap <- v("summaryDetail", "marketCap")
  fcf <- v("financialData", "freeCashflow")
  de <- v("financialData", "debtToEquity") # Yahoo lo da en %, Finviz como razón
  trend <- j$earningsTrend$trend
  tg <- function(per) {
    if (is.null(trend)) return(NA_real_)
    for (t in trend) if (identical(t$period, per)) {
      g <- t$growth
      if (is.list(g)) g <- g$raw
      return(if (is.null(g) || !is.numeric(g)) NA_real_ else as.numeric(g))
    }
    NA_real_
  }
  fpe <- v("summaryDetail", "forwardPE")
  if (is.na(fpe)) fpe <- v("defaultKeyStatistics", "forwardPE")

  out <- list(
    "Price" = num(price, 4),
    "Target Price" = num(v("financialData", "targetMeanPrice"), 4),
    "EPS (ttm)" = num(v("defaultKeyStatistics", "trailingEps"), 4),
    "Book/sh" = num(v("defaultKeyStatistics", "bookValue"), 4),
    "Cash/sh" = num(v("financialData", "totalCashPerShare"), 4),
    "Debt/Eq" = num(if (is.na(de)) NA_real_ else de / 100, 4),
    "P/E" = num(v("summaryDetail", "trailingPE"), 4),
    "PEG" = num(v("defaultKeyStatistics", "pegRatio"), 4),
    "Forward P/E" = num(fpe, 4),
    "P/S" = num(v("summaryDetail", "priceToSalesTrailing12Months"), 4),
    "P/FCF" = num(if (!is.na(mcap) && !is.na(fcf) && fcf > 0) mcap / fcf else NA_real_, 4),
    "EV/EBITDA" = num(v("defaultKeyStatistics", "enterpriseToEbitda"), 4),
    "P/B" = num(v("defaultKeyStatistics", "priceToBook"), 4),
    "EPS this Y" = pct(v("financialData", "earningsGrowth")),
    "Sales Y/Y TTM" = pct(v("financialData", "revenueGrowth")),
    "EPS next Y" = pct(tg("+1y")),
    "EPS next 5Y" = pct(tg("+5y")),
    "Sales Q/Q" = pct(v("financialData", "revenueGrowth")),
    "EPS Q/Q" = pct(v("defaultKeyStatistics", "earningsQuarterlyGrowth")),
    "Gross Margin" = pct(v("financialData", "grossMargins")),
    "Oper. Margin" = pct(v("financialData", "operatingMargins")),
    "Profit Margin" = pct(v("financialData", "profitMargins")),
    "ROE" = pct(v("financialData", "returnOnEquity")),
    "ROA" = pct(v("financialData", "returnOnAssets")),
    "Current Ratio" = num(v("financialData", "currentRatio"), 4),
    "Short Float" = pct(v("defaultKeyStatistics", "shortPercentOfFloat")),
    "Inst Own" = pct(v("defaultKeyStatistics", "heldPercentInstitutions")),
    "Payout" = pct(v("summaryDetail", "payoutRatio")),
    "Recom" = num(v("financialData", "recommendationMean"), 2),
    "Beta" = num(v("summaryDetail", "beta"), 4),
    "Dividend %" = pct(v("summaryDetail", "dividendYield"))
  )
  qtype <- j$quoteType$quoteType %||% j$price$quoteType %||% ""
  nombre <- toupper(paste(j$price$longName %||% "", j$price$shortName %||% ""))
  if (!nzchar(qtype) && grepl("ISHARES|TRAC|ETF|FONDO|FUND", nombre)) qtype <- "ETF"
  sector <- j$assetProfile$sector
  out$raw_Sector <- if (!is.null(sector) && nzchar(sector)) {
    sector
  } else if (qtype %in% c("ETF", "MUTUALFUND")) {
    "ETF / Fondo"
  } else {
    "Desconocido"
  }
  out$raw_Currency <- j$price$currency %||% "MXN"
  out$raw_Source <- "Yahoo Finanzas"
  out$raw_keys <- names(out)[!startsWith(names(out), "raw_")]
  out$raw_vals <- unname(unlist(out[out$raw_keys]))
  out
}

get_finviz_payload <- function(ticker, use_postgres_cache = FALSE) {
  cached <- cache_get(ticker, use_postgres_cache = use_postgres_cache)
  if (!is.null(cached)) {
    return(cached)
  }

  # Tickers mexicanos (.MX): Finviz no los cubre -> fundamentales de Yahoo Finanzas
  raw <- if (utils_is_mx_ticker(ticker)) .read_yahoo_fundamentals(ticker) else .read_finviz_table(ticker)
  if (is.null(raw)) {
    return(NULL)
  }

  cache_set(ticker, raw, use_postgres_cache = use_postgres_cache)
  raw
}

.yf_xt <- function(ticker, from = Sys.Date() - 365, to = Sys.Date()) {
  y_ticker <- utils_yahoo_symbol(ticker) # .MX se conserva; BRK.B -> BRK-B

  suppressWarnings({
    xt <- try(
      quantmod::getSymbols(
        Symbols = y_ticker,
        src = "yahoo",
        from = from,
        to = to,
        auto.assign = FALSE,
        warnings = FALSE
      ),
      silent = TRUE
    )
  })

  if (inherits(xt, "try-error")) {
    return(NULL)
  }
  xt
}

.get_spy_history <- function(from = Sys.Date() - 365, to = Sys.Date(), existing_spy = NULL) {
  if (!is.null(existing_spy)) {
    return(existing_spy)
  }

  if (exists(".spy_cache", envir = .GlobalEnv)) {
    return(get(".spy_cache", envir = .GlobalEnv))
  }

  xt <- .yf_xt("SPY", from, to)
  if (!is.null(xt)) {
    cl <- tryCatch(Cl(xt), error = function(e) NULL)
    if (!is.null(cl)) {
      spy_ret <- tryCatch(quantmod::dailyReturn(na.omit(cl)), error = function(e) NULL)
      if (!is.null(spy_ret)) {
        assign(".spy_cache", spy_ret, envir = .GlobalEnv)
        return(spy_ret)
      }
    }
  }
  NULL
}

get_history_metrics <- function(ticker, spy_data = NULL) {
  xt <- .yf_xt(ticker)
  if (is.null(xt) || nrow(xt) < 220) {
    return(list(history = NULL, tech = NULL))
  }

  cl <- tryCatch(Cl(xt), error = function(e) NULL)
  if (is.null(cl)) {
    return(list(history = NULL, tech = NULL))
  }

  cl <- na.omit(cl)
  if (nrow(cl) < 200) {
    return(list(history = NULL, tech = NULL))
  }

  hist_df <- tibble(
    Fecha = as.Date(index(cl)),
    Cierre = as.numeric(cl[, 1]),
    SMA20 = as.numeric(TTR::SMA(cl, 20)),
    SMA50 = as.numeric(TTR::SMA(cl, 50)),
    SMA200 = as.numeric(TTR::SMA(cl, 200)),
    RSI14 = as.numeric(TTR::RSI(cl, 14))
  )

  last_row <- tail(hist_df, 1)
  last_seq <- tail(hist_df, 5)
  cruce <- "Ninguno"
  if (nrow(last_seq) == 5) {
    if (last_seq$SMA50[5] > last_seq$SMA200[5] && any(last_seq$SMA50[1:4] < last_seq$SMA200[1:4], na.rm = TRUE)) {
      cruce <- "Golden Cross"
    } else if (last_seq$SMA50[5] < last_seq$SMA200[5] && any(last_seq$SMA50[1:4] > last_seq$SMA200[1:4], na.rm = TRUE)) cruce <- "Death Cross"
  }

  divergencia <- "Ninguna"
  if (nrow(hist_df) >= 20) {
    p_now <- last_row$Cierre
    rsi_now <- last_row$RSI14
    p_20 <- hist_df$Cierre[nrow(hist_df) - 19]
    rsi_20 <- hist_df$RSI14[nrow(hist_df) - 19]

    if (!is.na(p_now) && !is.na(p_20) && !is.na(rsi_now) && !is.na(rsi_20)) {
      if (p_now > p_20 * 1.05 && rsi_now < rsi_20 - 5) {
        divergencia <- "Bajista"
      } else if (p_now < p_20 * 0.95 && rsi_now > rsi_20 + 5) divergencia <- "Alcista"
    }
  }

  tech <- list(
    price = last_row$Cierre,
    s20 = last_row$SMA20,
    s50 = last_row$SMA50,
    s200 = last_row$SMA200,
    rsi = last_row$RSI14,
    dist_s20 = if (!is.na(last_row$SMA20) && last_row$SMA20 != 0) ((last_row$Cierre / last_row$SMA20) - 1) * 100 else NA_real_,
    dist_s50 = if (!is.na(last_row$SMA50) && last_row$SMA50 != 0) ((last_row$Cierre / last_row$SMA50) - 1) * 100 else NA_real_,
    dist_s200 = if (!is.na(last_row$SMA200) && last_row$SMA200 != 0) ((last_row$Cierre / last_row$SMA200) - 1) * 100 else NA_real_,
    cruce_sma = if (cruce == "Golden Cross") 1 else if (cruce == "Death Cross") -1 else 0,
    cruce_sma_lbl = cruce,
    divergencia_rsi = if (divergencia == "Alcista") 1 else if (divergencia == "Bajista") -1 else 0,
    divergencia_rsi_lbl = divergencia
  )

  # Calculate Risk metrics (Volatility, Drawdown, Beta)
  ret <- tryCatch(quantmod::dailyReturn(cl), error = function(e) NULL)
  spy_ret <- .get_spy_history(existing_spy = spy_data)

  if (!is.null(ret) && length(ret) > 10) {
    tech$volatility <- sd(ret, na.rm = TRUE) * sqrt(252) * 100
    cum_ret <- exp(cumsum(log(1 + as.numeric(ret))))
    cum_max <- cummax(cum_ret)
    drawdown <- (cum_ret - cum_max) / cum_max
    tech$max_drawdown <- min(drawdown, na.rm = TRUE) * 100

    if (!is.null(spy_ret)) {
      merged_ret <- merge(ret, spy_ret, all = FALSE)
      if (nrow(merged_ret) > 10) {
        cov_val <- cov(as.numeric(merged_ret[, 1]), as.numeric(merged_ret[, 2]), use = "complete.obs")
        var_spy <- var(as.numeric(merged_ret[, 2]), na.rm = TRUE)
        if (var_spy > 0) tech$beta <- cov_val / var_spy else tech$beta <- NA_real_
      } else {
        tech$beta <- NA_real_
      }
    } else {
      tech$beta <- NA_real_
    }
  } else {
    tech$volatility <- NA_real_
    tech$max_drawdown <- NA_real_
    tech$beta <- NA_real_
  }

  list(history = hist_df, tech = tech)
}

# =========================================================
# 5) MAPEO DE MÉTRICAS
# =========================================================

extract_metrics <- function(raw, tech) {
  price <- as_num(getv_robust(raw, c("Price")))
  target <- as_num(getv_robust(raw, c("Target Price")))

  upside <- if (!is.na(target) && !is.na(price) && price > 0) round((target / price - 1) * 100, 2) else NA_real_

  eps_ttm <- as_num(getv_robust(raw, c("EPS (ttm)", "EPS(ttm)")))
  bvps <- as_num(getv_robust(raw, c("Book/sh")))
  p_fcf <- as_num(getv_robust(raw, c("P/FCF")))
  cash_sh <- as_num(getv_robust(raw, c("Cash/sh")))
  debt_eq <- as_num(getv_robust(raw, c("Debt/Eq")))
  eps_5y <- as_pct(getv_robust(raw, c("EPS next 5Y")))

  graham_num <- graham_number(eps_ttm, bvps)
  graham_margin <- graham_margin_pct(graham_num, price)
  dcf_val <- dcf_value_per_share(price, p_fcf, cash_sh, bvps, debt_eq, eps_5y)
  graham_adj <- graham_adjusted_value(eps_ttm, eps_5y)
  iv_final <- dcf_val
  margin_final <- margin_of_safety_pct(iv_final, price)

  tibble::tribble(
    ~Categoria, ~Indicador, ~Valor, ~Unidad,
    "Valuación", "Precio actual (USD)", price, "USD",
    "Valuación", "Precio objetivo (USD)", target, "USD",
    "Valuación", "P/U", as_num(getv_robust(raw, c("P/E"))), "",
    "Valuación", "PEG", as_num(getv_robust(raw, c("PEG"))), "",
    "Valuación", "P/U futuro", as_num(getv_robust(raw, c("Forward P/E"))), "",
    "Valuación", "P/Ventas", as_num(getv_robust(raw, c("P/S"))), "",
    "Valuación", "P/FCF", as_num(getv_robust(raw, c("P/FCF"))), "",
    "Valuación", "EV/EBITDA", as_num(getv_robust(raw, c("EV/EBITDA"))), "",
    "Valuación", "P/Libros", as_num(getv_robust(raw, c("P/B"))), "",
    "Valuación", "Valor Graham (USD)", graham_num %||% NA_real_, "USD",
    "Valuación", "Margen Graham", graham_margin, "%",
    "Valuación", "Valor DCF (USD)", dcf_val %||% NA_real_, "USD",
    "Valuación", "Valor Graham Ajustado (USD)", graham_adj %||% NA_real_, "USD",
    "Valuación", "Valor Intrínseco Final (USD)", iv_final %||% NA_real_, "USD",
    "Valuación", "Margen Seguridad", margin_final, "%",
    "Crecimiento", "EPS interanual TTM", as_pct(getv_robust(raw, c("EPS this Y"))), "%",
    "Crecimiento", "Ventas interanual TTM", as_pct(getv_robust(raw, c("Sales Y/Y TTM", "Sales past 5Y"))), "%",
    "Crecimiento", "EPS próximo año", as_pct(getv_robust(raw, c("EPS next Y"))), "%",
    "Crecimiento", "EPS próximos 5 años", as_pct(getv_robust(raw, c("EPS next 5Y"))), "%",
    "Crecimiento", "Ventas Trim/Trim", as_pct(getv_robust(raw, c("Sales Q/Q"))), "%",
    "Crecimiento", "EPS Trim/Trim", as_pct(getv_robust(raw, c("EPS Q/Q"))), "%",
    "Rentabilidad", "Margen bruto", as_pct(getv_robust(raw, c("Gross Margin"))), "%",
    "Rentabilidad", "Margen operativo", as_pct(getv_robust(raw, c("Oper. Margin"))), "%",
    "Rentabilidad", "Margen neto", as_pct(getv_robust(raw, c("Profit Margin"))), "%",
    "Rentabilidad", "ROE", as_pct(getv_robust(raw, c("ROE"))), "%",
    "Rentabilidad", "ROIC", as_pct(getv_robust(raw, c("ROIC", "ROI"))), "%",
    "Rentabilidad", "ROA", as_pct(getv_robust(raw, c("ROA"))), "%",
    "Salud", "Deuda/Patrimonio", as_num(getv_robust(raw, c("Debt/Eq"))), "",
    "Salud", "Liquidez corriente", as_num(getv_robust(raw, c("Current Ratio"))), "",
    "Riesgo", "Volatilidad Anual", round(tech$volatility %||% NA_real_, 2), "%",
    "Riesgo", "Max Drawdown", round(tech$max_drawdown %||% NA_real_, 2), "%",
    "Riesgo", "Beta (1A)", round(tech$beta %||% NA_real_, 2), "vs SPY",
    "Técnica", "Dist. SMA20", tech$dist_s20 %||% NA_real_, "%",
    "Técnica", "Dist. SMA50", tech$dist_s50 %||% NA_real_, "%",
    "Técnica", "Dist. SMA200", tech$dist_s200 %||% NA_real_, "%",
    "Técnica", "RSI (14)", tech$rsi %||% NA_real_, "pts",
    "Técnica", "Cruce Media Móvil", tech$cruce_sma %||% NA_real_, tech$cruce_sma_lbl %||% "",
    "Técnica", "Divergencia RSI", tech$divergencia_rsi %||% NA_real_, tech$divergencia_rsi_lbl %||% "",
    "Sentimiento", "Interés corto", as_pct(getv_robust(raw, c("Short Float"))), "%",
    "Sentimiento", "Propiedad institucional", as_pct(getv_robust(raw, c("Inst Own"))), "%",
    "Sentimiento", "Transacciones internas", as_pct(getv_robust(raw, c("Insider Trans"))), "%",
    "Sentimiento", "Payout", as_pct(getv_robust(raw, c("Payout"))), "%",
    "Analistas", "Potencial a objetivo", upside, "%",
    "Analistas", "Recomendación", as_num(getv_robust(raw, c("Recom"))), ""
  )
}

# =========================================================
# 6) SEMÁFORO Y PESOS
# =========================================================

score_rule <- function(indicador, valor) {
  if (is.na(valor)) {
    return(list(semaforo = "Amarillo", score = 50, razon = "Sin dato disponible."))
  }

  v <- valor
  sem <- "Amarillo"
  score <- 50
  razon <- ""

  if (indicador == "P/U") {
    if (v <= 22) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 35) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Menor suele ser mejor. (\u226422: Verde, 22-35: Amarillo, >35: Rojo)"
  } else if (indicador == "PEG") {
    if (v <= 1.2) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 2.5) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Relaciona valuaci\u00f3n con crecimiento. (\u22641.2: Verde, 1.2-2.5: Amarillo, >2.5: Rojo)"
  } else if (indicador == "P/U futuro") {
    if (v <= 22) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 35) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Menor suele ser mejor. (\u226422: Verde, 22-35: Amarillo, >35: Rojo)"
  } else if (indicador == "P/Ventas") {
    if (v <= 6) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 10) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "M\u00e1s \u00fatil cuando la utilidad es d\u00e9bil. (\u22646: Verde, 6-10: Amarillo, >10: Rojo)"
  } else if (indicador == "P/FCF") {
    if (v <= 25) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 45) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Menor suele ser mejor. (\u226425: Verde, 25-45: Amarillo, >45: Rojo)"
  } else if (indicador == "EV/EBITDA") {
    if (v <= 18) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 25) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Menor suele ser mejor. (\u226418: Verde, 18-25: Amarillo, >25: Rojo)"
  } else if (indicador == "P/Libros") {
    if (v <= 3) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 6) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "En financieras o intensivos en activos. (\u22643: Verde, 3-6: Amarillo, >6: Rojo)"
  } else if (indicador == "Valor Graham (USD)") {
    sem <- "Amarillo"
    score <- 50
    razon <- "Métrica de referencia absoluta."
  } else if (indicador == "Margen Graham") {
    if (v >= 0) {
      sem <- "Verde"
      score <- 100
    } else if (v >= -20) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Positivo es subvaluado. (\u22650: Verde, 0 a -20: Amarillo, <-20: Rojo)"
  } else if (indicador %in% c("Valor DCF (USD)", "Valor Graham Ajustado (USD)", "Valor Intr\u00ednseco Final (USD)")) {
    sem <- "Amarillo"
    score <- 50
    razon <- "M\u00e9trica de referencia absoluta."
  } else if (indicador == "Margen Seguridad") {
    if (v > 10) {
      sem <- "Verde"
      score <- 100
    } else if (v >= -10) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Margen vs. Valor Intr\u00ednseco (DCF). (>10%: Verde/Infravalorada, -10 a 10%: Amarillo/Valor justo, <-10%: Rojo/Sobrevalorada)"
  } else if (indicador == "EPS interanual TTM") {
    if (v >= 15) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 5) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u226515: Verde, 5-15: Amarillo, <5: Rojo)"
  } else if (indicador == "Ventas interanual TTM") {
    if (v >= 12) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 4) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u226512: Verde, 4-12: Amarillo, <4: Rojo)"
  } else if (indicador == "EPS próximo año") {
    if (v >= 15) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 5) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u226515: Verde, 5-15: Amarillo, <5: Rojo)"
  } else if (indicador == "EPS próximos 5 años") {
    if (v >= 15) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 8) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u226515: Verde, 8-15: Amarillo, <8: Rojo)"
  } else if (indicador == "Ventas Trim/Trim") {
    if (v >= 8) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 3) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u22658: Verde, 3-8: Amarillo, <3: Rojo)"
  } else if (indicador == "EPS Trim/Trim") {
    if (v >= 15) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 5) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u226515: Verde, 5-15: Amarillo, <5: Rojo)"
  } else if (indicador == "Margen bruto") {
    if (v >= 45) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 25) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u226545: Verde, 25-45: Amarillo, <25: Rojo)"
  } else if (indicador == "Margen operativo") {
    if (v >= 20) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 10) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u226520: Verde, 10-20: Amarillo, <10: Rojo)"
  } else if (indicador == "Margen neto") {
    if (v >= 15) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 8) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u226515: Verde, 8-15: Amarillo, <8: Rojo)"
  } else if (indicador == "ROE") {
    if (v >= 18) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 12) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u226518: Verde, 12-18: Amarillo, <12: Rojo)"
  } else if (indicador == "ROIC") {
    if (v >= 15) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 10) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u226515: Verde, 10-15: Amarillo, <10: Rojo)"
  } else if (indicador == "ROA") {
    if (v >= 7) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 4) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u22657: Verde, 4-7: Amarillo, <4: Rojo)"
  } else if (indicador == "Deuda/Patrimonio") {
    if (v <= 0.5) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 1.5) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Menor suele ser mejor. (\u22640.5: Verde, 0.5-1.5: Amarillo, >1.5: Rojo)"
  } else if (indicador == "Liquidez corriente") {
    if (v >= 1.5) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 1.0) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u22651.5: Verde, 1.0-1.5: Amarillo, <1.0: Rojo)"
  } else if (indicador == "Volatilidad Anual") {
    if (v <= 25) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 40) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Menor volatilidad indica menor riesgo. (\u226425: Verde, 25-40: Amarillo, >40: Rojo)"
  } else if (indicador == "Max Drawdown") {
    if (v >= -15) {
      sem <- "Verde"
      score <- 100
    } else if (v >= -30) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Menor ca\u00edda indica resiliencia. (\u2265-15: Verde, -15 a -30: Amarillo, <-30: Rojo)"
  } else if (indicador == "Beta (1A)") {
    if (v <= 0.8) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 1.2) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Menor a 1 es menor convulsi\u00f3n. (\u22640.8: Verde, 0.8-1.2: Amarillo, >1.2: Rojo)"
  } else if (indicador %in% c("Dist. SMA20", "Dist. SMA50", "Dist. SMA200")) {
    if (v >= 0) {
      sem <- "Verde"
      score <- 100
    } else if (v >= -5) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Arriba de la media es mejor. (\u22650%: Verde, 0 a -5%: Amarillo, <-5%: Rojo)"
  } else if (indicador == "RSI (14)") {
    if (v >= 45 && v <= 65) {
      sem <- "Verde"
      score <- 100
    } else if ((v >= 35 && v < 45) || (v > 65 && v <= 75)) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Favorable en zonas medias. (45-65: Verde, Extremos sanos: Amarillo, Sobrecompra/Venta: Rojo)"
  } else if (indicador == "Cruce Media Móvil") {
    if (v == 1) {
      sem <- "Verde"
      score <- 100
      razon <- "Golden Cross (Señal Alcista de Largo Plazo)"
    } else if (v == -1) {
      sem <- "Rojo"
      score <- 20
      razon <- "Death Cross (Señal Bajista de Largo Plazo)"
    } else {
      sem <- "Amarillo"
      score <- 50
      razon <- "Sin cruces recientes"
    }
  } else if (indicador == "Divergencia RSI") {
    if (v == 1) {
      sem <- "Verde"
      score <- 100
      razon <- "Divergencia Alcista Detectada"
    } else if (v == -1) {
      sem <- "Rojo"
      score <- 20
      razon <- "Divergencia Bajista Detectada"
    } else {
      sem <- "Amarillo"
      score <- 50
      razon <- "Sin divergencia inminente"
    }
  } else if (indicador == "Interés corto") {
    if (v <= 5) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 15) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Menor suele ser mejor. (\u22645: Verde, 5-15: Amarillo, >15: Rojo)"
  } else if (indicador == "Propiedad institucional") {
    if (v >= 60) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 40) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Moderado-alto es favorable. (\u226560: Verde, 40-60: Amarillo, <40: Rojo)"
  } else if (indicador == "Transacciones internas") {
    if (v >= 0) {
      sem <- "Verde"
      score <- 100
    } else if (v >= -1) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Positivo suele ser mejor. (\u22650: Verde, -1 a 0: Amarillo, <-1: Rojo)"
  } else if (indicador == "Payout") {
    if (v >= 20 && v <= 60) {
      sem <- "Verde"
      score <- 100
    } else if ((v >= 10 && v < 20) || (v > 60 && v <= 80)) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Medio es sostenible. (20-60: Verde, 10-20 o 60-80: Amarillo, Resto: Rojo)"
  } else if (indicador == "Potencial a objetivo") {
    if (v >= 15) {
      sem <- "Verde"
      score <- 100
    } else if (v >= 5) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "Mayor suele ser mejor. (\u226515: Verde, 5-15: Amarillo, <5: Rojo)"
  } else if (indicador == "Recomendación") {
    if (v <= 1.7) {
      sem <- "Verde"
      score <- 100
    } else if (v <= 2.5) {
      sem <- "Amarillo"
      score <- 60
    } else {
      sem <- "Rojo"
      score <- 20
    }
    razon <- "M\u00e1s cercano a 1 es mejor. (\u22641.7: Verde, 1.7-2.5: Amarillo, >2.5: Rojo)"
  }

  list(semaforo = sem, score = score, razon = razon)
}

weights_by_profile <- function(profile = "Balanceado") {
  profile <- tolower(profile)

  if (profile == "conservador") {
    c(
      "Valuación" = 1.1,
      "Crecimiento" = 0.9,
      "Rentabilidad" = 1.2,
      "Salud" = 1.4,
      "Riesgo" = 1.3,
      "Técnica" = 0.9,
      "Sentimiento" = 0.7,
      "Analistas" = 0.6
    )
  } else if (profile == "agresivo") {
    c(
      "Valuación" = 0.9,
      "Crecimiento" = 1.4,
      "Rentabilidad" = 1.1,
      "Salud" = 0.8,
      "Riesgo" = 0.7,
      "Técnica" = 1.2,
      "Sentimiento" = 1.0,
      "Analistas" = 0.8
    )
  } else {
    c(
      "Valuación" = 1.0,
      "Crecimiento" = 1.2,
      "Rentabilidad" = 1.2,
      "Salud" = 1.1,
      "Riesgo" = 1.0,
      "Técnica" = 1.0,
      "Sentimiento" = 0.8,
      "Analistas" = 0.7
    )
  }
}

indicator_base_weight <- function(indicador) {
  dplyr::case_when(
    indicador %in% c("P/U", "PEG", "P/U futuro") ~ 1.2,
    indicador %in% c("P/Ventas", "P/FCF", "EV/EBITDA", "P/Libros") ~ 0.9,
    indicador %in% c("EPS interanual TTM", "EPS próximo año", "EPS próximos 5 años", "EPS Trim/Trim") ~ 1.3,
    indicador %in% c("Ventas interanual TTM", "Ventas Trim/Trim") ~ 1.1,
    indicador %in% c("Margen operativo", "Margen neto", "ROIC") ~ 1.3,
    indicador %in% c("Margen bruto", "ROE", "ROA") ~ 1.0,
    indicador %in% c("Deuda/Patrimonio") ~ 1.3,
    indicador %in% c("Liquidez corriente") ~ 0.9,
    indicador %in% c("Dist. SMA200") ~ 1.3,
    indicador %in% c("Dist. SMA50") ~ 1.0,
    indicador %in% c("Dist. SMA20", "RSI (14)") ~ 0.8,
    indicador %in% c("Margen Graham", "Volatilidad Anual", "Max Drawdown", "Beta (1A)") ~ 1.2,
    indicador %in% c("Margen Seguridad") ~ 2.0,
    indicador %in% c("Cruce Media Móvil", "Divergencia RSI") ~ 1.0,
    indicador %in% c("Valor Graham (USD)", "Precio actual (USD)", "Precio objetivo (USD)") ~ 0.9,
    indicador %in% c("Valor DCF (USD)", "Valor Graham Ajustado (USD)", "Valor Intrínseco Final (USD)") ~ 0.5,
    indicador %in% c("Interés corto") ~ 0.9,
    indicador %in% c("Propiedad institucional", "Transacciones internas") ~ 0.7,
    indicador %in% c("Payout") ~ 0.5,
    indicador %in% c("Potencial a objetivo", "Recomendación") ~ 0.8,
    TRUE ~ 1.0
  )
}

apply_scores <- function(metric_df, profile = "Balanceado") {
  cat_weights <- weights_by_profile(profile)

  evals <- purrr::map2(metric_df$Indicador, metric_df$Valor, score_rule)

  metric_df %>%
    mutate(
      `Nombre Finviz` = dplyr::case_when(
        Indicador == "Precio actual (USD)" ~ "Price",
        Indicador == "Precio objetivo (USD)" ~ "Target Price",
        Indicador == "P/U" ~ "P/E",
        Indicador == "PEG" ~ "PEG",
        Indicador == "P/U futuro" ~ "Forward P/E",
        Indicador == "P/Ventas" ~ "P/S",
        Indicador == "P/FCF" ~ "P/FCF",
        Indicador == "EV/EBITDA" ~ "EV/EBITDA",
        Indicador == "P/Libros" ~ "P/B",
        Indicador == "Valor Graham (USD)" ~ "Graham Number",
        Indicador == "Margen Graham" ~ "Graham Margin",
        Indicador == "Valor DCF (USD)" ~ "DCF Value",
        Indicador == "Valor Graham Ajustado (USD)" ~ "Graham Adjusted Value",
        Indicador == "Valor Intrínseco Final (USD)" ~ "Intrinsic Value Final",
        Indicador == "Margen Seguridad" ~ "Margin of Safety",
        Indicador == "EPS interanual TTM" ~ "EPS this Y",
        Indicador == "Ventas interanual TTM" ~ "Sales Y/Y TTM",
        Indicador == "EPS próximo año" ~ "EPS next Y",
        Indicador == "EPS próximos 5 años" ~ "EPS next 5Y",
        Indicador == "Ventas Trim/Trim" ~ "Sales Q/Q",
        Indicador == "EPS Trim/Trim" ~ "EPS Q/Q",
        Indicador == "Margen bruto" ~ "Gross Margin",
        Indicador == "Margen operativo" ~ "Oper. Margin",
        Indicador == "Margen neto" ~ "Profit Margin",
        Indicador == "ROE" ~ "ROE",
        Indicador == "ROIC" ~ "ROIC",
        Indicador == "ROA" ~ "ROA",
        Indicador == "Deuda/Patrimonio" ~ "Debt/Eq",
        Indicador == "Liquidez corriente" ~ "Current Ratio",
        Indicador == "Volatilidad Anual" ~ "Volatility",
        Indicador == "Max Drawdown" ~ "Max Drawdown",
        Indicador == "Beta (1A)" ~ "Beta",
        Indicador == "Dist. SMA20" ~ "Dist. SMA20",
        Indicador == "Dist. SMA50" ~ "Dist. SMA50",
        Indicador == "Dist. SMA200" ~ "Dist. SMA200",
        Indicador == "RSI (14)" ~ "RSI (14)",
        Indicador == "Cruce Media Móvil" ~ "MA Cross",
        Indicador == "Divergencia RSI" ~ "RSI Div",
        Indicador == "Interés corto" ~ "Short Float",
        Indicador == "Propiedad institucional" ~ "Inst Own",
        Indicador == "Transacciones internas" ~ "Insider Trans",
        Indicador == "Payout" ~ "Payout",
        Indicador == "Potencial a objetivo" ~ "Target Upside",
        Indicador == "Recomendaci\u00f3n" ~ "Recom",
        TRUE ~ Indicador
      ),
      Semaforo = purrr::map_chr(evals, "semaforo"),
      Score = purrr::map_dbl(evals, "score"),
      Razon = purrr::map_chr(evals, "razon"),
      PesoBase = vapply(Indicador, indicator_base_weight, numeric(1)),
      PesoCategoria = unname(cat_weights[.data[["Categoria"]]]),
      Peso = round(PesoBase * PesoCategoria, 2),
      ScorePond = Score * Peso
    ) %>%
    relocate(`Nombre Finviz`, .after = Indicador)
}

infer_trend <- function(tech) {
  px <- tech$price
  s20 <- tech$s20
  s50 <- tech$s50
  s200 <- tech$s200
  rsi <- tech$rsi

  if (is.na(px) || is.na(s200)) {
    return("Sin datos")
  }
  if (!is.na(s20) && !is.na(s50) && px > s200 && s20 > s50 && s50 > s200 && !is.na(rsi) && rsi >= 50 && rsi <= 70) {
    return("Alcista fuerte")
  }
  if (px > s200 && !is.na(rsi) && rsi >= 45) {
    return("Alcista")
  }
  if (abs(px - s200) / px <= 0.03) {
    return("Lateral")
  }
  "Bajista"
}

build_interpretation <- function(ticker, score_total, semaforo, tendencia, details_df) {
  top_green <- details_df %>%
    filter(Semaforo == "Verde", Peso > 0) %>%
    arrange(desc(ScorePond)) %>%
    slice_head(n = 4) %>%
    pull(Indicador)

  top_red <- details_df %>%
    filter(Semaforo == "Rojo", Peso > 0) %>%
    arrange(desc(ScorePond)) %>%
    slice_head(n = 3) %>%
    pull(Indicador)

  txt <- paste0(
    "La empresa ", ticker,
    " tiene una calificación global de ", round(score_total, 1), "/100",
    " y un semáforo ", tolower(semaforo), ". ",
    "La tendencia actual es ", tolower(tendencia), ". "
  )

  if (length(top_green) > 0) {
    txt <- paste0(txt, "Sus principales fortalezas son: ", paste(top_green, collapse = ", "), ". ")
  }

  if (length(top_red) > 0) {
    txt <- paste0(txt, "Los puntos que requieren mayor atención son: ", paste(top_red, collapse = ", "), ". ")
  }

  if (score_total >= 75) {
    txt <- paste0(txt, "En conjunto, la acción presenta un perfil atractivo para seguimiento o entrada selectiva.")
  } else if (score_total >= 55) {
    txt <- paste0(txt, "En conjunto, muestra señales mixtas; conviene esperar confirmación adicional.")
  } else {
    txt <- paste0(txt, "En conjunto, todavía no presenta una combinación sólida de fortaleza fundamental y técnica.")
  }

  txt
}

# =========================================================
# 7) ANÁLISIS DE UN TICKER
# =========================================================

analyze_one_ticker <- function(ticker, investor_profile = "Balanceado", use_postgres_cache = FALSE, progress_cb = NULL, base_pct = 0, step_pct = 100, spy_data = NULL) {
  if (is.function(progress_cb)) progress_cb(msg = paste(ticker, if (utils_is_mx_ticker(ticker)) "- Yahoo Finanzas fundamentales (1/3)" else "- Finviz (1/3)"), pct = base_pct + step_pct * 0.15)
  raw <- get_finviz_payload(ticker, use_postgres_cache = use_postgres_cache)

  if (is.function(progress_cb)) progress_cb(msg = paste(ticker, "- Yahoo Finance (2/3)"), pct = base_pct + step_pct * 0.45)
  hist <- get_history_metrics(ticker, spy_data = spy_data)

  if (is.function(progress_cb)) progress_cb(msg = paste(ticker, "- Evaluando métricas (3/3)"), pct = base_pct + step_pct * 0.80)
  if (is.null(raw)) {
    return(list(
      summary = tibble(
        Ticker = ticker,
        Perfil = investor_profile,
        `Precio actual (USD)` = NA_real_,
        `Puntaje total` = NA_real_,
        Semaforo = "Rojo",
        Tendencia = "Sin datos",
        `RSI (14)` = NA_real_,
        `Dist. SMA20 (%)` = NA_real_,
        `Dist. SMA50 (%)` = NA_real_,
        `Dist. SMA200 (%)` = NA_real_,
        `Potencial a objetivo (%)` = NA_real_,
        `Valor Intrínseco Final (USD)` = NA_real_,
        `Veredicto Valor` = "Sin datos",
        Sector = "Desconocido",
        Interpretacion = if (utils_is_mx_ticker(ticker)) {
          "No fue posible obtener los datos de Yahoo Finanzas para este ticker (.MX). Verifica que el símbolo exista en Yahoo Finanzas."
        } else {
          "No fue posible obtener correctamente los datos de Finviz para este ticker."
        }
      ),
      details = tibble(),
      history = hist$history
    ))
  }

  metric_df <- extract_metrics(raw, hist$tech %||% list())
  scored <- apply_scores(metric_df, profile = investor_profile)

  valid_rows <- scored %>% filter(Peso > 0)
  total_weight <- sum(valid_rows$Peso, na.rm = TRUE)
  score_total <- if (total_weight > 0) sum(valid_rows$ScorePond, na.rm = TRUE) / total_weight else NA_real_

  semaforo <- if (is.na(score_total)) "Rojo" else if (score_total >= 75) "Verde" else if (score_total >= 55) "Amarillo" else "Rojo"
  tendencia <- infer_trend(hist$tech %||% list(price = NA, s20 = NA, s50 = NA, s200 = NA, rsi = NA))
  interpretation <- build_interpretation(ticker, score_total, semaforo, tendencia, scored)

  price_val <- scored %>%
    filter(Indicador == "Precio actual (USD)") %>%
    pull(Valor)
  upside_val <- scored %>%
    filter(Indicador == "Potencial a objetivo") %>%
    pull(Valor)
  rsi_val <- scored %>%
    filter(Indicador == "RSI (14)") %>%
    pull(Valor)
  d20_val <- scored %>%
    filter(Indicador == "Dist. SMA20") %>%
    pull(Valor)
  d50_val <- scored %>%
    filter(Indicador == "Dist. SMA50") %>%
    pull(Valor)
  d200_val <- scored %>%
    filter(Indicador == "Dist. SMA200") %>%
    pull(Valor)
  iv_final_val <- scored %>%
    filter(Indicador == "Valor Intrínseco Final (USD)") %>%
    pull(Valor)
  margin_final_val <- scored %>%
    filter(Indicador == "Margen Seguridad") %>%
    pull(Valor)

  iv_final_val <- iv_final_val %||% NA_real_
  margin_final_val <- margin_final_val %||% NA_real_
  veredicto_val <- veredicto_valor(margin_final_val)

  summary <- tibble(
    Ticker = ticker,
    Perfil = investor_profile,
    `Precio actual (USD)` = price_val %||% NA_real_,
    `Puntaje total` = round(score_total, 1),
    Semaforo = semaforo,
    Tendencia = tendencia,
    `RSI (14)` = rsi_val %||% NA_real_,
    `Dist. SMA20 (%)` = d20_val %||% NA_real_,
    `Dist. SMA50 (%)` = d50_val %||% NA_real_,
    `Dist. SMA200 (%)` = d200_val %||% NA_real_,
    `Potencial a objetivo (%)` = upside_val %||% NA_real_,
    `Valor Intrínseco Final (USD)` = iv_final_val,
    `Veredicto Valor` = veredicto_val,
    Sector = raw$raw_Sector %||% "Desconocido",
    Interpretacion = interpretation
  )
  if (utils_is_mx_ticker(ticker)) {
    summary$Interpretacion <- paste0(
      "[Emisora mexicana · datos de Yahoo Finanzas · precios en ", raw$raw_Currency %||% "MXN", "] ",
      summary$Interpretacion
    )
  }

  list(summary = summary, details = scored, history = hist$history)
}

# =========================================================
# 8) ESTRATEGIAS AVANZADAS
# =========================================================

calculate_correlation <- function(history_list) {
  valid_hist <- purrr::keep(history_list, ~ !is.null(.x) && nrow(.x) > 0)
  if (length(valid_hist) < 2) {
    return(NULL)
  }

  closes <- purrr::imap(valid_hist, function(df, tk) {
    df_sub <- df[, c("Fecha", "Cierre")]
    names(df_sub)[2] <- tk
    df_sub
  })

  merged <- purrr::reduce(closes, ~ full_join(.x, .y, by = "Fecha")) %>%
    arrange(Fecha) %>%
    tidyr::drop_na()

  if (nrow(merged) < 10) {
    return(NULL)
  }

  ret_mat <- apply(merged[, -1, drop = FALSE], 2, function(x) diff(log(x)))
  cor_mat <- cor(ret_mat, use = "pairwise.complete.obs")
  cor_mat
}

run_backtest <- function(history_df, cost_bps = 10) {
  stopifnot(length(cost_bps)==1L, is.finite(cost_bps), cost_bps>=0, cost_bps<10000)
  if (is.null(history_df) || nrow(history_df) < 50) {
    return(tibble(
      Estrategia = c("Buy & Hold", "Sistema Semáforo (RSI)"),
      `Retorno Total (%)` = c(NA_real_, NA_real_),
      `Max Drawdown (%)` = c(NA_real_, NA_real_)
    ))
  }

  hist <- history_df %>%
    arrange(Fecha) %>%
    mutate(
      Retorno_Diario = Cierre / dplyr::lag(Cierre) - 1,
      Senal = dplyr::case_when(
        RSI14 < 40 & Cierre > SMA200 ~ 1,
        RSI14 > 70 ~ 0,
        TRUE ~ NA_real_
      )
    ) %>%
    tidyr::fill(Senal, .direction = "down") %>%
    mutate(
      Senal = tidyr::replace_na(Senal, 0),
      Posicion = dplyr::lag(Senal, default = 0),
      Rotacion = abs(Posicion - dplyr::lag(Posicion, default = 0)),
      Retorno_Estrategia = Posicion * Retorno_Diario - Rotacion * cost_bps / 10000
    ) %>%
    tidyr::drop_na(Retorno_Diario)

  bnh_cum <- c(1, (1-cost_bps/10000) * cumprod(1 + hist$Retorno_Diario))
  sys_cum <- c(1, cumprod(1 + hist$Retorno_Estrategia))

  bnh_ret <- (tail(bnh_cum, 1) - 1) * 100
  sys_ret <- (tail(sys_cum, 1) - 1) * 100

  bnh_dd <- min((bnh_cum - cummax(bnh_cum)) / cummax(bnh_cum), na.rm = TRUE) * 100
  sys_dd <- min((sys_cum - cummax(sys_cum)) / cummax(sys_cum), na.rm = TRUE) * 100

  tibble(
    Estrategia = c("Buy & Hold", "Sistema Semáforo (RSI)"),
    `Retorno Total (%)` = round(c(bnh_ret, sys_ret), 2),
    `Max Drawdown (%)` = round(c(bnh_dd, sys_dd), 2)
  )
}

# =========================================================
# 9) PIPELINE
# =========================================================

inst_run_pipeline <- function(tickers, investor_profile = "Balanceado", use_postgres_cache = FALSE, progress_cb = NULL) {
  tickers <- inst_parse_tickers(paste(tickers, collapse = ","))
  if (length(tickers) == 0) {
    return(list(
      summary     = tibble(),
      details     = list(),
      history     = list(),
      ranking     = tibble(),
      correlation = NULL,
      backtest    = tibble()
    ))
  }

  n <- length(tickers)

  if (is.function(progress_cb)) progress_cb(msg = "Preparando análisis...", pct = 1)

  # Esta función corre dentro de un proceso en segundo plano (mirai/future).
  # Los tickers se procesan uno a uno para poder reportar el avance real
  # (de 10% a 90%) al módulo, que lo muestra en la barra de progreso.
  if (is.function(progress_cb)) progress_cb(msg = "Descargando datos de mercado base (SPY)...", pct = 5)
  spy_data <- .get_spy_history()

  res_list <- vector("list", n)
  for (k in seq_len(n)) {
    tk <- tickers[k]
    if (is.function(progress_cb)) {
      progress_cb(msg = sprintf("Analizando %s (%d de %d)", tk, k, n), pct = 10 + floor(80 * (k - 1) / n))
    }
    # Un ticker con error no detiene el análisis completo
    res_list[k] <- list(tryCatch(
      analyze_one_ticker(
        ticker = tk,
        investor_profile = investor_profile,
        use_postgres_cache = use_postgres_cache,
        progress_cb = NULL,
        spy_data = spy_data
      ),
      error = function(e) {
        message("[INST] Error en ", tk, ": ", conditionMessage(e))
        NULL
      }
    ))
  }
  ok <- !vapply(res_list, function(r) is.null(r) || is.null(r$summary), logical(1))
  if (!any(ok)) stop("No se pudo analizar ningún ticker. Verifica los símbolos o la conexión con las fuentes de datos.")
  res_list <- res_list[ok]
  tickers <- tickers[ok]
  if (is.function(progress_cb)) progress_cb(msg = "Consolidando resultados por ticker...", pct = 90)

  names(res_list) <- tickers

  summary_df <- bind_rows(lapply(res_list, `[[`, "summary")) %>%
    arrange(desc(`Puntaje total`))

  ranking_df <- summary_df %>%
    transmute(
      Rank = row_number(),
      Ticker,
      Perfil,
      `Puntaje total`,
      Semaforo
    )

  details_list <- setNames(lapply(res_list, `[[`, "details"), tickers)
  history_list <- setNames(lapply(res_list, `[[`, "history"), tickers)

  if (is.function(progress_cb)) progress_cb(msg = "Calculando correlación y backtesting...", pct = 95)

  correlation_mat <- calculate_correlation(history_list)
  backtest_df <- purrr::imap_dfr(history_list, function(hist, tk) {
    bt <- run_backtest(hist)
    bt %>%
      mutate(Ticker = tk) %>%
      select(Ticker, everything())
  })

  if (is.function(progress_cb)) progress_cb(msg = "Evaluando reglas del Playbook...", pct = 98)

  playbook_signals <- sapply(summary_df$Ticker, function(tk) {
    sem_gen <- summary_df$Semaforo[summary_df$Ticker == tk]
    if (length(sem_gen) == 0 || is.na(sem_gen)) {
      return("Desconocido")
    }
    if (sem_gen == "Rojo") {
      return("❌ Descartar")
    }
    if (sem_gen == "Amarillo") {
      return("🟡 Observar")
    }

    bt_sub <- backtest_df[backtest_df$Ticker == tk, ]
    pass_bt <- FALSE
    if (nrow(bt_sub) == 2) {
      ret_bnh <- bt_sub$"Retorno Total (%)"[bt_sub$Estrategia == "Buy & Hold"]
      ret_sys <- bt_sub$"Retorno Total (%)"[bt_sub$Estrategia == "Sistema Semáforo (RSI)"]
      dd_bnh <- bt_sub$"Max Drawdown (%)"[bt_sub$Estrategia == "Buy & Hold"]
      dd_sys <- bt_sub$"Max Drawdown (%)"[bt_sub$Estrategia == "Sistema Semáforo (RSI)"]
      pass_bt <- (!is.na(ret_sys) && !is.na(ret_bnh) && ret_sys >= ret_bnh) &&
        (!is.na(dd_sys) && !is.na(dd_bnh) && dd_sys >= dd_bnh)
    }

    det_sub <- details_list[[tk]]
    margen_graham <- det_sub$Valor[det_sub$Indicador == "Margen Graham"]
    if (length(margen_graham) > 0 && is.character(margen_graham)) margen_graham <- as.numeric(gsub("[^0-9.-]", "", margen_graham))
    pass_gr <- (length(margen_graham) > 0 && !is.na(margen_graham[1]) && margen_graham[1] >= -10)

    pass_cor <- TRUE
    if (!is.null(correlation_mat) && tk %in% rownames(correlation_mat)) {
      others <- correlation_mat[tk, colnames(correlation_mat) != tk]
      if (length(others) > 0) {
        cor_avg <- mean(others, na.rm = TRUE)
        pass_cor <- (!is.na(cor_avg) && cor_avg < 0.6)
      }
    }

    if (pass_bt && pass_gr && pass_cor) {
      return("🚀 Compra Fuerte")
    }
    if (pass_bt && pass_gr && !pass_cor) {
      return("🟢 Compra (Alta Correlac.)")
    }
    return("🟡 Riesgo Moderado (Revise Detalle)")
  })

  summary_df[["Senal Playbook"]] <- unname(playbook_signals)
  ranking_df[["Senal Playbook"]] <- unname(playbook_signals)

  summary_df <- summary_df %>% dplyr::select(Ticker, Perfil, matches("Precio"), matches("Puntaje"), matches("Semaforo"), matches("Playbook"), dplyr::everything())
  ranking_df <- ranking_df %>% dplyr::select(Rank, Ticker, Perfil, matches("Puntaje"), matches("Semaforo"), matches("Playbook"))

  if (is.function(progress_cb)) progress_cb(msg = "Consolidando resultados...", pct = 100)

  list(
    summary = summary_df,
    details = details_list,
    history = history_list,
    ranking = ranking_df,
    correlation = correlation_mat,
    backtest = backtest_df
  )
}

# =========================================================
# 9) EXPORTACIÓN A EXCEL
# =========================================================

inst_export_analysis_to_excel <- function(result, file) {
  wb <- createWorkbook()

  headerStyle <- createStyle(
    textDecoration = "bold",
    fgFill = "#0f62fe",
    fontColour = "#FFFFFF",
    halign = "center",
    border = "Bottom"
  )

  greenFill <- createStyle(fgFill = "#00B050")
  yellowFill <- createStyle(fgFill = "#FFC000")
  redFill <- createStyle(fgFill = "#FF0000")

  # 1) Resumen
  addWorksheet(wb, "Resumen")
  res_df <- result$summary %>%
    mutate(
      `Puntaje total` = round(`Puntaje total`, 0),
      `Precio actual (USD)` = round(`Precio actual (USD)`, 2),
      `RSI (14)` = round(`RSI (14)`, 2),
      `Dist. SMA20 (%)` = round(`Dist. SMA20 (%)`, 2),
      `Dist. SMA50 (%)` = round(`Dist. SMA50 (%)`, 2),
      `Dist. SMA200 (%)` = round(`Dist. SMA200 (%)`, 2),
      `Potencial a objetivo (%)` = round(`Potencial a objetivo (%)`, 2)
    )
  sem_vals_res <- res_df$Semaforo
  res_df$Semaforo <- ""

  writeData(wb, "Resumen", res_df)
  freezePane(wb, "Resumen", firstRow = TRUE)
  setColWidths(wb, "Resumen", cols = 1:ncol(res_df), widths = "auto")

  sem_col_res <- which(names(res_df) == "Semaforo")
  if (length(sem_col_res) == 1) {
    for (i in seq_len(nrow(res_df))) {
      st <- sem_vals_res[[i]]
      if (is.na(st)) next
      sty <- if (st == "Verde") greenFill else if (st == "Amarillo") yellowFill else redFill
      addStyle(wb, "Resumen", sty, rows = i + 1, cols = sem_col_res, stack = TRUE)
    }
  }

  # 2) Ranking
  addWorksheet(wb, "Ranking")
  rk_df <- result$ranking %>% mutate(`Puntaje total` = round(`Puntaje total`, 0))
  sem_vals_rk <- rk_df$Semaforo
  rk_df$Semaforo <- ""

  writeData(wb, "Ranking", rk_df)
  freezePane(wb, "Ranking", firstRow = TRUE)
  setColWidths(wb, "Ranking", cols = 1:ncol(rk_df), widths = "auto")

  sem_col_rk <- which(names(rk_df) == "Semaforo")
  if (length(sem_col_rk) == 1) {
    for (i in seq_len(nrow(rk_df))) {
      st <- sem_vals_rk[[i]]
      if (is.na(st)) next
      sty <- if (st == "Verde") greenFill else if (st == "Amarillo") yellowFill else redFill
      addStyle(wb, "Ranking", sty, rows = i + 1, cols = sem_col_rk, stack = TRUE)
    }
  }

  # 3) Glosario
  addWorksheet(wb, "Glosario")
  glossary <- inst_build_glossary_df()
  writeData(wb, "Glosario", glossary)
  freezePane(wb, "Glosario", firstRow = TRUE)
  setColWidths(wb, "Glosario", cols = 1:ncol(glossary), widths = "auto")

  # 4) Detalles de Tickers
  tickers <- names(result$details)
  for (tk in tickers) {
    det <- result$details[[tk]]
    if (is.null(det) || !is.data.frame(det) || nrow(det) == 0) next

    sh <- substr(tk, 1, 31)
    addWorksheet(wb, sh)

    df <- det %>%
      mutate(
        Valor = ifelse(is.na(Valor), NA_character_, ifelse(Unidad == "", as.character(round(Valor, 2)), paste0(round(Valor, 2), " ", Unidad)))
      ) %>%
      select(matches("Categ"), Indicador, Valor, matches("Semaforo"), Peso, matches("Razon"), matches("Interpret"))

    col_semafor_ts <- names(df)[grepl("Semaforo", names(df))][1]
    sem_vals_ts <- df[[col_semafor_ts]]
    df[[col_semafor_ts]] <- ""

    writeData(wb, sh, df)
    freezePane(wb, sh, firstRow = TRUE)
    setColWidths(wb, sh, cols = 1:ncol(df), widths = "auto")

    sem_col_ts <- which(grepl("Semaforo", names(df)))[1]
    if (length(sem_col_ts) == 1) {
      for (i in seq_len(nrow(df))) {
        st <- sem_vals_ts[[i]]
        if (is.na(st)) next
        sty <- if (st == "Verde") greenFill else if (st == "Amarillo") yellowFill else if (st == "Rojo") redFill else NULL
        if (!is.null(sty)) {
          addStyle(wb, sh, sty, rows = i + 1, cols = sem_col_ts, stack = TRUE)
        }
      }
    }
  }

  # Estilo a todos los encabezados
  for (sh in names(wb)) {
    addStyle(wb, sh, headerStyle, rows = 1, cols = 1:50, gridExpand = TRUE, stack = TRUE)
  }

  saveWorkbook(wb, file, overwrite = TRUE)
  invisible(file)
}
