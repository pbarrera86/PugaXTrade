# R/extremos_core.R
# =========================================================
# Screener de Extremos Locales (QMin / QMax)
# ---------------------------------------------------------
# 1) Detecta extremos locales (máximos y mínimos) con un zigzag
#    sobre el precio de cierre: un pivote se confirma cuando el
#    precio se aleja del extremo al menos `umbral` (p. ej. 20%).
# 2) Construye la serie de rendimientos entre extremos:
#      - en cada mínimo: caída desde el máximo anterior (negativa)
#      - en cada máximo: subida desde el mínimo anterior (positiva)
# 3) Calcula cuantiles de esas caídas (QMin) y subidas (QMax).
#      QMinP = caída que solo supera el (100-P)% de las caídas históricas
#      QMaxP = subida que solo supera el (100-P)% de las subidas históricas
# 4) Proyecta niveles de precio:
#      Precio Mínimo P = Precio Último Máximo × (1 + QMinP)
#      Precio Máximo P = Precio Último Mínimo × (1 + QMaxP)
#      Posición Actual = Precio Actual − Precio Mínimo P
# 5) Screener (dos modos):
#    - Mínimos: Precio Actual < Precio Mínimo del QMin elegido (p. ej. QMin90)
#      → la caída actual ya es mayor que el 90% de sus caídas históricas.
#    - Máximos: Precio Actual > Precio Máximo del QMax elegido (p. ej. QMax90)
#      → la subida actual ya es mayor que el 90% de sus subidas históricas.
#
# Este archivo es autocontenido para poder cargarse dentro de un worker
# de `future` (solo requiere quantmod para descargar precios).
# =========================================================

EXT_QUANTILES <- c(40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 99)

# Lista por defecto del módulo Extremos Locales: emisoras y ETFs de la Bolsa Mexicana / SIC (.MX)
EXT_DEFAULT_TICKERS <- c(
  "TEAKCPO.MX", "CEMEXCPO.MX", "AMXB.MX", "SIGMAFA.MX", "ALPEKA.MX", "GMEXICOB.MX", "FUNO11.MX",
  "MEXTRAC09.MX", "TLEVISACPO.MX", "GFNORTEO.MX", "FMTY14.MX", "VOLARA.MX", "KIMBERA.MX",
  "LABB.MX", "BIMBOA.MX", "FLOAN.MX", "FEMSAUBD.MX", "BBAJIOO.MX", "LACOMERUBC.MX", "BOLSAA.MX",
  "NEMAKA.MX", "CHDRAUIB.MX", "KOFUBL.MX", "GFINBURO.MX", "OMAB.MX", "GAPB.MX", "NAFTRACISHRS.MX",
  "RA.MX", "MEGACPO.MX", "DANHOS13.MX", "FIBRAMQ12.MX", "FCFE18.MX", "FIBRAPL14.MX", "GRUMAB.MX",
  "CULTIBAB.MX", "ASURB.MX", "VAPUN.MX", "FSHOP13.MX", "GCARSOA1.MX", "FMX23.MX", "IMBAN.MX",
  "NUN.MX", "IBTAN.MX", "IVVPESOISHRS.MX", "DLRTRAC15.MX", "AGGUN.MX", "SDIAN.MX", "IUITN.MX",
  "SPXS1N.MX", "IUISN.MX", "LQDAN.MX", "MFRISCOA-1.MX", "IB01N.MX", "JPEAN.MX", "NION.MX",
  "AXTELCPO.MX", "IHYAN.MX", "FIHO12.MX", "SPYLN.MX", "LIVEPOLC-1.MX", "IUFSN.MX", "NUTRISAA.MX",
  "FNOVA17.MX", "ANGELD10.MX", "IUESN.MX", "VDCAN.MX", "FEXI21.MX", "TRAXIONA.MX", "IBEN.MX",
  "SITES1A-1.MX", "FINN13.MX", "FIBRAHD15.MX", "IUCDN.MX", "GGBN.MX", "RRN.MX", "VAGUN.MX",
  "IUHCN.MX", "XPEVN.MX", "IUUSN.MX", "GISSAA.MX", "IBTMXXN.MX", "BABAN.MX", "CTAXTELA.MX",
  "IB1MXXN.MX", "FRMXNXN.MX", "TSMN.MX", "NOKN.MX", "NCLHN.MX", "GRABN.MX", "CMRB.MX", "VDSTN.MX",
  "SKHYN.MX", "CSPXN.MX", "CSPXXN.MX", "GICSAB.MX", "SEN.MX", "NBISN.MX", "IUCSN.MX", "MELIN.MX",
  "ACTINVRB.MX", "TIGRN.MX", "UCGN.MX", "ISACN.MX", "IUMSN.MX", "GLOBN.MX", "AINFN.MX", "FNCLN.MX",
  "SP20N.MX", "ABEVN.MX", "BTBTN.MX", "CPGN.MX", "VMSTXN.MX", "CJPUN.MX", "VUAAN.MX", "VMEX19.MX",
  "SANN.MX", "IUAAN.MX", "EUNKN.MX", "DIABLOI10.MX", "1211N.MX", "VDTAN.MX", "ANAUN.MX",
  "TBBBN.MX", "NASQN.MX", "SEMIN.MX", "GLOBCA.MX", "NVON.MX", "AUTLANB.MX", "AGUILASCPO.MX",
  "CBU7N.MX", "GLENN.MX", "RYAAYN.MX", "FHIPO14.MX", "EMBMXXN.MX", "FIBRATC14.MX", "JDN.MX",
  "VWCGN.MX", "NEXT25.MX", "VWRAN.MX", "VDPAN.MX", "MB3MN.MX", "SDMXXN.MX", "CNDXN.MX",
  "CIBRUN.MX", "SMARTRC14.MX", "CPXJN.MX", "CRDON.MX", "ONONN.MX", "CCL1N.MX", "VDEAN.MX",
  "1810N.MX", "STORAGE18.MX", "SXLKN.MX", "SPOTN.MX", "VMCAXN.MX", "TMN.MX", "0R87N.MX",
  "FVIA16.MX", "FRAGUAB.MX", "SPORTS.MX", "VISTAA.MX", "NESNN.MX", "CMODN.MX", "ARMN.MX",
  "MBILN.MX", "FPLUS16.MX", "RYN.MX", "SMHUN.MX", "PICSN.MX", "ERNAN.MX", "VFEAN.MX", "VNRAN.MX",
  "AIN.MX", "VITROA.MX", "IHYMXXN.MX", "SIEN.MX", "0JN9N.MX", "CADUA.MX", "VHYDN.MX", "TCEHYN.MX",
  "AIRN.MX", "VJPAN.MX", "ADSN.MX", "ENRN.MX", "ASML1N.MX", "PBRN.MX", "EDUCA18.MX", "CTEC2N.MX",
  "EXPNN.MX", "WSMLN.MX", "SONYN.MX", "CCJN.MX", "CDISN.MX", "ACNN.MX", "SORIANAB.MX", "EIMIN.MX",
  "GASIN.MX", "XUSEN.MX", "ITXN.MX", "SXLVN.MX", "SAFN.MX", "POSADASA.MX", "BBN.MX", "CYBAN.MX",
  "RWEN.MX", "VOW3N.MX", "ESENTIAII.MX", "BBDN.MX", "STLAN.MX", "ROVIN.MX", "RBOTN.MX", "VUDSN.MX",
  "SXLFN.MX", "IHGN.MX", "SAPN.MX", "MNSON.MX", "CCEPN.MX", "CNYAN.MX", "MEDICAB.MX", "ORN.MX",
  "IWDAN.MX", "TMMA.MX", "ULN.MX", "CIEB.MX", "CFRIN.MX", "SHOPN.MX", "SUN.MX", "RMSN.MX",
  "GBMO.MX", "LIN1N.MX", "ETN1N.MX", "PDDN.MX", "LOCKN.MX", "DEON.MX", "AZN1N.MX", "BIDUN.MX",
  "ASMLN.MX", "ZPR1N.MX", "VALEN.MX", "I37MXN.MX", "YPFN.MX", "STXN.MX", "PAASN.MX", "PRYN.MX",
  "IGLNN.MX", "DHYAN.MX", "RACEN.MX", "ESGMEXISHRS.MX", "IJPAN.MX", "CBN.MX", "DB1N.MX",
  "FVRRN.MX", "SGLDN.MX", "EWSPN.MX", "FWRAN.MX", "SIMECB.MX", "AEMN.MX", "BILIN.MX",
  "DIABLOSO.MX", "V3AAN.MX", "BAKN.MX", "VHYAN.MX", "DGTLN.MX", "CONVERA.MX", "CYDSASAA.MX",
  "POCHTECB.MX", "FUTUN.MX", "ICHNN.MX", "DHTN.MX", "NVSN.MX", "ALTERNAB.MX", "AG1N.MX",
  "CEMAN.MX", "EQACN.MX", "IEMAN.MX", "DBN.MX", "KGCN.MX", "FLEXN.MX", "BN1N.MX", "INSWN.MX",
  "SXLPN.MX", "ACCELSAB.MX", "GSKN.MX", "SKMN.MX", "R2USN.MX", "BPN.MX", "MCN.MX", "BAFARB.MX",
  "ITUBN.MX", "BTEC1N.MX", "SHEL1N.MX", "CHNTRAC11.MX", "ERNMXN.MX", "ESPON.MX", "CRSPN.MX",
  "4BRZN.MX", "QANTN.MX", "TCOMN.MX", "SOMA21.MX", "SXLIN.MX", "BMWM5N.MX", "ARCON.MX", "SEDGN.MX",
  "UUUUN.MX", "FIBRAUP18.MX", "EVTCN.MX", "ECARN.MX", "BN.MX", "QVGMEX18.MX", "UMCN.MX", "NTRN.MX",
  "BNTXN.MX", "MDTN.MX", "LIAN.MX", "WIXN.MX", "RION.MX", "SIDN.MX", "AONN.MX", "EMCAN.MX",
  "SMSNN.MX", "BTGN.MX", "MINSAB.MX", "AIAIN.MX", "AGEDN.MX", "DRDN.MX", "FLXKN.MX", "QNTMN.MX",
  "WQTMN.MX", "CNIN.MX", "IAUPN.MX", "DPYAN.MX"
)

ext_default_tickers_text <- function() {
  paste(EXT_DEFAULT_TICKERS, collapse = ", ")
}

# ---------------------------------------------------------
# Descarga de precios (historia completa) con caché en disco
# ---------------------------------------------------------
.ext_cache_file <- function(ticker) {
  dir <- file.path(getwd(), "cache", "extremos")
  if (!dir.exists(dir)) dir.create(dir, recursive = TRUE, showWarnings = FALSE)
  file.path(dir, paste0(gsub("[^A-Za-z0-9._-]", "_", ticker), ".rds"))
}

ext_get_prices <- function(ticker, from = as.Date("1990-01-01"), max_age_hours = 12) {
  f <- .ext_cache_file(ticker)
  if (file.exists(f)) {
    age_h <- as.numeric(difftime(Sys.time(), file.info(f)$mtime, units = "hours"))
    if (is.finite(age_h) && age_h <= max_age_hours) {
      cached <- tryCatch(readRDS(f), error = function(e) NULL)
      if (!is.null(cached) && nrow(cached) > 0) return(cached)
    }
  }

  # .MX (Bolsa Mexicana) se conserva; clases de EE. UU. usan guion (BRK.B -> BRK-B)
  y_ticker <- if (grepl("\\.MX$", toupper(ticker))) toupper(ticker) else gsub(".", "-", ticker, fixed = TRUE)
  xt <- suppressWarnings(tryCatch(
    quantmod::getSymbols(
      Symbols = y_ticker, src = "yahoo", from = from, to = Sys.Date() + 1,
      auto.assign = FALSE, warnings = FALSE
    ),
    error = function(e) NULL
  ))
  if (is.null(xt) || NROW(xt) == 0) return(NULL)

  # Cierre ajustado solo por splits (Yahoo "Close"), igual que en el resto de la app
  cl <- as.numeric(quantmod::Cl(xt))
  df <- data.frame(date = as.Date(zoo::index(xt)), close = cl)
  df <- df[is.finite(df$close) & df$close > 0, , drop = FALSE]
  if (nrow(df) == 0) return(NULL)

  try(saveRDS(df, f), silent = TRUE)
  df
}

# ---------------------------------------------------------
# Zigzag: detección de extremos locales
# ---------------------------------------------------------
# Devuelve data.frame con: idx, date, price, type ("max"/"min"), confirmed
# El último pivote es "tentativo" (confirmed = FALSE): es el extremo del
# tramo en curso, que aún no se ha revertido lo suficiente.
ext_zigzag <- function(dates, close, umbral = 0.20) {
  n <- length(close)
  if (n < 3) return(NULL)

  piv_idx <- integer(0)
  piv_type <- character(0)

  # Fase inicial: esperar el primer movimiento >= umbral
  hi_i <- 1L
  lo_i <- 1L
  trend <- 0L # 1 = subiendo (buscando máximo), -1 = bajando (buscando mínimo)
  i <- 2L
  while (i <= n && trend == 0L) {
    if (close[i] > close[hi_i]) hi_i <- i
    if (close[i] < close[lo_i]) lo_i <- i
    if (close[i] >= close[lo_i] * (1 + umbral) && lo_i < i) {
      piv_idx <- c(piv_idx, lo_i); piv_type <- c(piv_type, "min")
      trend <- 1L; ext_i <- i
    } else if (close[i] <= close[hi_i] * (1 - umbral) && hi_i < i) {
      piv_idx <- c(piv_idx, hi_i); piv_type <- c(piv_type, "max")
      trend <- -1L; ext_i <- i
    }
    i <- i + 1L
  }
  if (trend == 0L) return(NULL)

  while (i <= n) {
    p <- close[i]
    if (trend == 1L) {
      if (p >= close[ext_i]) {
        ext_i <- i
      } else if (p <= close[ext_i] * (1 - umbral)) {
        piv_idx <- c(piv_idx, ext_i); piv_type <- c(piv_type, "max")
        trend <- -1L; ext_i <- i
      }
    } else {
      if (p <= close[ext_i]) {
        ext_i <- i
      } else if (p >= close[ext_i] * (1 + umbral)) {
        piv_idx <- c(piv_idx, ext_i); piv_type <- c(piv_type, "min")
        trend <- 1L; ext_i <- i
      }
    }
    i <- i + 1L
  }

  # Pivote tentativo (tramo en curso)
  piv_idx <- c(piv_idx, ext_i)
  piv_type <- c(piv_type, if (trend == 1L) "max" else "min")

  out <- data.frame(
    idx = piv_idx,
    date = dates[piv_idx],
    price = close[piv_idx],
    type = piv_type,
    confirmed = c(rep(TRUE, length(piv_idx) - 1L), FALSE),
    stringsAsFactors = FALSE
  )
  out$ret <- c(NA_real_, out$price[-1] / out$price[-nrow(out)] - 1)
  out
}

# ---------------------------------------------------------
# Métricas para un ticker
# ---------------------------------------------------------
ext_compute <- function(prices, umbral = 0.20, quantiles = EXT_QUANTILES) {
  if (is.null(prices) || nrow(prices) < 60) return(NULL)
  piv <- ext_zigzag(prices$date, prices$close, umbral)
  if (is.null(piv) || nrow(piv) < 3) return(NULL)

  conf <- piv[piv$confirmed & !is.na(piv$ret), , drop = FALSE]
  drops <- conf$ret[conf$type == "min"]
  rises <- conf$ret[conf$type == "max"]
  if (length(drops) < 1 || length(rises) < 1) return(NULL)

  # Último máximo / mínimo CONFIRMADOS (el tramo en curso aún no cuenta)
  piv_c <- piv[piv$confirmed, , drop = FALSE]
  last_max <- tail(piv_c[piv_c$type == "max", , drop = FALSE], 1)
  last_min <- tail(piv_c[piv_c$type == "min", , drop = FALSE], 1)
  if (nrow(last_max) == 0 || nrow(last_min) == 0) return(NULL)
  last_conf <- tail(piv_c, 1)

  price_now <- tail(prices$close, 1)
  date_now <- tail(prices$date, 1)

  q_min <- stats::quantile(drops, probs = 1 - quantiles / 100, names = FALSE, type = 7)
  q_max <- stats::quantile(rises, probs = quantiles / 100, names = FALSE, type = 7)

  tbl <- data.frame(
    Concepto = c(paste0("Quantil", quantiles), "Media"),
    Minimos = c(q_min, mean(drops)),
    Maximos = c(q_max, mean(rises)),
    stringsAsFactors = FALSE
  )
  tbl$PrecioUltimoMinimo <- last_min$price
  tbl$PrecioUltimoMaximo <- last_max$price
  tbl$PrecioActual <- price_now
  tbl$PreciosMinimos <- last_max$price * (1 + tbl$Minimos)
  tbl$PreciosMaximos <- last_min$price * (1 + tbl$Maximos)
  tbl$PosicionActual <- price_now - tbl$PreciosMinimos

  list(
    prices = prices,
    pivots = piv,
    drops = drops,
    rises = rises,
    table = tbl,
    price_now = price_now,
    date_now = date_now,
    last_max = last_max,
    last_min = last_min,
    caida_actual = price_now / last_max$price - 1,
    rebote_actual = price_now / last_min$price - 1,
    dias_ult_extremo = as.integer(date_now - last_conf$date),
    tramo = if (tail(piv$type, 1) == "min") "Bajista" else "Alcista"
  )
}

# Fila resumen del screener para un ticker
ext_screen_row <- function(ticker, res, q_sel = 90, modo = c("min", "max")) {
  modo <- match.arg(modo)
  if (is.null(res)) return(NULL)
  tb <- res$table
  qrow <- tb[tb$Concepto == paste0("Quantil", q_sel), , drop = FALSE]
  if (nrow(qrow) == 0) return(NULL)
  q_rows <- tb[grepl("^Quantil", tb$Concepto), , drop = FALSE]

  if (modo == "min") {
    # Cuantil más profundo que el precio actual ya perforó (por debajo)
    broken <- q_rows$Concepto[res$price_now < q_rows$PreciosMinimos]
    nivel <- if (length(broken)) sub("Quantil", "QMin", tail(broken, 1)) else "—"
    data.frame(
      Ticker = ticker,
      PrecioActual = res$price_now,
      ExtremoRef = res$last_max$price,
      FechaExtremoRef = res$last_max$date,
      MovActual = res$caida_actual,
      QSel = qrow$Minimos,
      PrecioQSel = qrow$PreciosMinimos,
      DistanciaPct = res$price_now / qrow$PreciosMinimos - 1,
      NivelSuperado = nivel,
      QMedia = tb$Minimos[tb$Concepto == "Media"],
      NumTramos = length(res$drops),
      DiasUltExtremo = res$dias_ult_extremo,
      Tramo = res$tramo,
      stringsAsFactors = FALSE
    )
  } else {
    # Cuantil más alto que el precio actual ya superó (por encima)
    broken <- q_rows$Concepto[res$price_now > q_rows$PreciosMaximos]
    nivel <- if (length(broken)) sub("Quantil", "QMax", tail(broken, 1)) else "—"
    data.frame(
      Ticker = ticker,
      PrecioActual = res$price_now,
      ExtremoRef = res$last_min$price,
      FechaExtremoRef = res$last_min$date,
      MovActual = res$rebote_actual,
      QSel = qrow$Maximos,
      PrecioQSel = qrow$PreciosMaximos,
      DistanciaPct = res$price_now / qrow$PreciosMaximos - 1,
      NivelSuperado = nivel,
      QMedia = tb$Maximos[tb$Concepto == "Media"],
      NumTramos = length(res$rises),
      DiasUltExtremo = res$dias_ult_extremo,
      Tramo = res$tramo,
      stringsAsFactors = FALSE
    )
  }
}

# ---------------------------------------------------------
# Pipeline del screener (lista de tickers)
#   modo = "min": empresas con precio POR DEBAJO del Precio QMin elegido
#                 (caída desde el último máximo mayor que la mayoría de sus caídas)
#   modo = "max": empresas con precio POR ENCIMA del Precio QMax elegido
#                 (subida desde el último mínimo mayor que la mayoría de sus subidas)
# ---------------------------------------------------------
ext_run_screener <- function(tickers, umbral = 0.20, q_sel = 90, min_caidas = 5,
                             max_age_hours = 12, progress_cb = NULL, modo = c("min", "max")) {
  modo <- match.arg(modo)
  tickers <- unique(toupper(trimws(tickers)))
  tickers <- tickers[nzchar(tickers)]
  rows <- vector("list", length(tickers))
  details <- list()
  fallidos <- character(0)

  for (k in seq_along(tickers)) {
    tk <- tickers[k]
    if (is.function(progress_cb)) try(progress_cb(k, length(tickers), tk), silent = TRUE)
    px <- tryCatch(ext_get_prices(tk, max_age_hours = max_age_hours), error = function(e) NULL)
    res <- tryCatch(ext_compute(px, umbral = umbral), error = function(e) NULL)
    if (is.null(res)) {
      fallidos <- c(fallidos, tk)
      next
    }
    details[[tk]] <- res
    rows[[k]] <- ext_screen_row(tk, res, q_sel, modo)
  }

  todos <- do.call(rbind, rows[!vapply(rows, is.null, logical(1))])
  if (is.null(todos)) todos <- data.frame()

  # Orden: lo más extremo primero (más abajo del QMin / más arriba del QMax)
  ordenar <- function(df) {
    if (!nrow(df)) return(df)
    df[order(if (modo == "min") df$DistanciaPct else -df$DistanciaPct), , drop = FALSE]
  }
  candidatos <- if (nrow(todos)) {
    cumple <- if (modo == "min") todos$PrecioActual < todos$PrecioQSel else todos$PrecioActual > todos$PrecioQSel
    ordenar(todos[cumple & todos$NumTramos >= min_caidas, , drop = FALSE])
  } else {
    todos
  }

  list(
    candidatos = candidatos,
    todos = ordenar(todos),
    details = details,
    fallidos = fallidos,
    params = list(umbral = umbral, q_sel = q_sel, min_caidas = min_caidas, modo = modo),
    run_at = Sys.time()
  )
}
