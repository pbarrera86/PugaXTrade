# R/utils_core.R
# Funciones compartidas entre Análisis Estándar e Institucional

# %||% se define en R/helpers.R (versión robusta). No redefinir aquí.

utils_parse_tickers <- function(txt) {
  if (is.null(txt) || !nzchar(txt)) {
    return(character(0))
  }
  xs <- unlist(strsplit(txt, "[,;\\s]+"))
  xs <- toupper(trimws(xs))
  xs <- gsub("^[^A-Z0-9.-]+|[^A-Z0-9.-]+$", "", xs)
  xs <- xs[nzchar(xs) & nchar(xs) >= 1 & nchar(xs) <= 6]
  unique(xs)
}

utils_http_get <- function(url, tries = 3, sleep_sec = 1) {
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
        `Cache-Control` = "no-cache",
        `Pragma` = "no-cache"
      ),
      httr::timeout(10)
    ), silent = TRUE)

    if (!inherits(res, "try-error")) {
      code <- httr::status_code(res)
      if (code %in% 200:299) return(res)
      if (code == 404) return(NULL) 
    }
    Sys.sleep(sleep_sec * (2^(i - 1)))
  }
  NULL
}

utils_read_finviz_table <- function(sym) {
  url <- sprintf("https://finviz.com/quote.ashx?t=%s", utils::URLencode(sym))
  res <- utils_http_get(url)
  if (is.null(res)) return(NULL)
  
  html <- try(xml2::read_html(res), silent = TRUE)
  if (inherits(html, "try-error")) return(NULL)

  nodes <- rvest::html_elements(html, "table.snapshot-table2")
  if (length(nodes) == 0) return(NULL)
  
  kv <- rvest::html_elements(nodes[[1]], "td")
  txt <- rvest::html_text(kv, trim = TRUE)
  if (length(txt) %% 2 != 0) txt <- head(txt, -1)
  if (length(txt) < 4) return(NULL)

  keys <- txt[seq(1, length(txt), by = 2)]
  vals <- txt[seq(2, length(txt), by = 2)]
  
  # IMPORTANTE: Guardar con claves originales (igual que modulo institucional)
  # Esto permite que getv_robust busque "P/E", "Gross Margin" etc. directamente
  out <- setNames(as.list(vals), keys)
  out
}

utils_as_num <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_real_)
  x <- as.character(x[1])
  if (is.na(x) || !nzchar(x) || x %in% c("-", "N/A", "NA", "n/a", "")) return(NA_real_)
  clean <- gsub("[$,\\s]", "", x)
  m <- regmatches(clean, regexpr("^-?[0-9]+(\\.[0-9]+)?", clean))
  if (length(m) == 0 || !nzchar(m)) return(NA_real_)
  suppressWarnings(as.numeric(m))
}

utils_as_pct <- function(x) {
  utils_as_num(x)
}
