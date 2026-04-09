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
