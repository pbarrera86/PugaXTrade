# Intercept POST endpoints before Shiny's GET-only UI handler.
puga_http_handler <- function(ui_handler, parse_query, set_session, logout, webhook, readiness=NULL) {
  force(ui_handler)
  function(req) {
    q <- parse_query(req$QUERY_STRING %||% "")
    path <- req$PATH_INFO %||% "/"
    response <- if(identical(path,"/healthz") && is.function(readiness)) readiness(req) else
      if(identical(path,"/webhook") || identical(q$action,"webhook")) webhook(req) else
      if(identical(path,"/auth/set-session") || identical(q$action,"set-session")) set_session(req) else
      if(identical(path,"/auth/logout") || identical(q$action,"logout")) logout(req) else ui_handler(req)
    if (!is.null(response)) {
      response$headers$`Cache-Control` <- "no-store"
      response$headers$`Referrer-Policy` <- "no-referrer"
      response$headers$`X-Content-Type-Options` <- "nosniff"
      response$headers$`X-Frame-Options` <- "SAMEORIGIN"
      response$headers$`Content-Security-Policy` <- "frame-ancestors 'self'; object-src 'none'; base-uri 'self'"
    }
    response
  }
}
