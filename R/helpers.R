# ----------------- Helpers cortos -----------------
nzchar2 <- function(x) {
  if (is.null(x) || length(x) == 0) {
    return(FALSE)
  }
  isTRUE(nzchar(x[[1]]))
}
val_or_empty <- function(x) {
  if (is.null(x) || length(x) == 0) "" else x
}
`%||%` <- function(a, b) {
  if (!is.null(a) && length(a) > 0 && !is.na(a)[1] && nzchar(as.character(a[[1]]))) a else b
}

# ----------------- Valoración: Valor Intrínseco (DCF + Graham) -----------------
# Usa solo campos ya scrapeados de Finviz (por acción) - sin necesidad de
# buscar Shares Outstanding/Total Debt/Total Cash en dólares absolutos.

graham_number <- function(eps_ttm, bvps) {
  if (is.na(eps_ttm) || eps_ttm <= 0 || is.na(bvps) || bvps <= 0) return(NA_real_)
  sqrt(22.5 * eps_ttm * bvps)
}

graham_margin_pct <- function(graham_num, price) {
  if (is.na(graham_num) || is.na(price) || price <= 0) return(NA_real_)
  round(100 * (graham_num / price - 1), 2)
}

dcf_value_per_share <- function(price, p_fcf, cash_sh, book_sh, debt_eq, growth_5y_pct,
                                 wacc = 0.10, terminal_growth = 0.025, years = 5) {
  if (is.na(price) || is.na(p_fcf) || p_fcf <= 0) return(NA_real_)
  fcf0 <- price / p_fcf
  if (is.na(fcf0) || fcf0 <= 0) return(NA_real_)
  # Tasa de crecimiento acotada [-30%, 50%] para evitar proyecciones absurdas con outliers de Finviz
  g <- if (is.na(growth_5y_pct)) 0.05 else min(max(growth_5y_pct / 100, -0.30), 0.50)
  t <- seq_len(years)
  fcf_t <- fcf0 * (1 + g)^t
  disc_t <- (1 + wacc)^t
  pv_fcf <- sum(fcf_t / disc_t)
  terminal_value <- fcf_t[years] * (1 + terminal_growth) / (wacc - terminal_growth)
  pv_terminal <- terminal_value / disc_t[years]
  cash_ps <- if (is.na(cash_sh)) 0 else cash_sh
  # Deuda/acción aproximada: Debt/Eq x Equity/acción (Book/sh), ya que Finviz no expone deuda total en $
  debt_ps <- if (is.na(debt_eq) || is.na(book_sh)) 0 else debt_eq * book_sh
  pv_fcf + pv_terminal + cash_ps - debt_ps
}

graham_adjusted_value <- function(eps_ttm, growth_5y_pct, base_pe = 8.5, aaa_yield = 4.5, normal_yield = 4.4) {
  if (is.na(eps_ttm) || eps_ttm <= 0) return(NA_real_)
  g <- if (is.na(growth_5y_pct)) 0 else growth_5y_pct
  eps_ttm * (base_pe + 2 * g) * (normal_yield / aaa_yield)
}

intrinsic_value_final <- function(dcf_val, graham_adj_val) {
  vals <- c(dcf_val, graham_adj_val)
  vals <- vals[!is.na(vals)]
  if (length(vals) == 0) return(NA_real_)
  mean(vals)
}

margin_of_safety_pct <- function(intrinsic_val, price) {
  if (is.na(intrinsic_val) || is.na(price) || price <= 0) return(NA_real_)
  round(100 * (intrinsic_val / price - 1), 2)
}

# Precio de entrada "seguro": el valor intrínseco con un descuento (margen de seguridad clásico de Graham).
# Con safety_margin_pct = 10, solo se considera atractivo comprar por debajo del 90% del valor intrínseco estimado.
safe_buy_price <- function(intrinsic_val, safety_margin_pct = 10) {
  if (is.na(intrinsic_val)) return(NA_real_)
  intrinsic_val * (1 - safety_margin_pct / 100)
}

# INFRAVALORADA (margen > +10%) / EN VALOR JUSTO (-10% a +10%) / SOBREVALORADA (< -10%)
veredicto_valor <- function(margin_pct) {
  if (is.na(margin_pct)) return("Sin datos")
  if (margin_pct > 10) return("Infravalorada")
  if (margin_pct >= -10) return("En valor justo")
  "Sobrevalorada"
}
