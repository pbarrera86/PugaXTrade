# Executed in a separate container, never owned by a browser/Shiny session.
puga_analysis_load_core <- function(app_dir) {
  setwd(app_dir)
  suppressPackageStartupMessages({
    library(future); library(dplyr); library(httr); library(rvest)
    library(xml2); library(quantmod); library(TTR); library(plotly); library(openxlsx)
  })
  future::plan(future::sequential)
  core <- new.env(parent=globalenv())
  for(file in c("R/helpers.R","R/db.R","R/finance_core.R","R/institutional_core.R"))
    sys.source(file,envir=core)
  core
}
puga_analysis_execute <- function(app_dir,path,request,runner=NULL) {
  tryCatch({
    check <- function() puga_analysis_check(path,request)
    progress <- function(msg="",pct=0) {
      check()
      puga_analysis_state(path,"running",msg,pct)
    }
    progress("Recuperando avance guardado...",1)
    if(is.null(runner)) {
      core <- puga_analysis_load_core(app_dir)
      on.exit({
        try(core$pool_close(),silent=TRUE)
        if(!is.null(core$pg_pool_global)) try(pool::poolClose(core$pg_pool_global),silent=TRUE)
      },add=TRUE)
      runner <- core$inst_run_pipeline
    }
    result <- runner(tickers=request$tickers,investor_profile=request$profile,
      use_postgres_cache=request$use_pg,progress_cb=progress,
      checkpoint_cb=function(index,compute) {
        check()
        value <- puga_analysis_checkpoint(path,index,compute)
        check()
        value
      })
    check()
    puga_analysis_write(result,file.path(path,"result.rds"))
    puga_analysis_state(path,"ok","Análisis completado. Resultados guardados.",100)
    invisible(NULL)
  },error=function(e) {
    kind <- if(inherits(e,"puga_cancel")) "cancelled" else if(inherits(e,"puga_pause")) "paused" else "error"
    msg <- if(kind=="cancelled") "Análisis cancelado por el usuario." else if(kind=="paused") "Se alcanzaron 6 horas. El avance está guardado para retomarlo." else "El proceso se interrumpió. El avance está guardado; puedes retomarlo."
    puga_analysis_state(path,kind,msg)
    if(kind=="error") message("[AnalysisWorker] Analysis interrupted; checkpoints retained.")
    invisible(NULL)
  })
}
