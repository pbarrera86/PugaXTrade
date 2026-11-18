# Disposable CI database and private fixture workspace only.
stopifnot(identical(Sys.getenv("PUGA_TEST_DATABASE"),"local-disposable"),
  identical(Sys.getenv("PGHOST"),"127.0.0.1"),identical(Sys.getenv("PGPORT"),"55432"),identical(Sys.getenv("PGDATABASE"),"postgres"))
setwd("/srv/shiny-server/app")
source("R/db.R");source("R/job_capacity.R");source("R/analysis_store.R");source("R/analysis_worker.R");source("R/analysis_export.R")
uid <- "00000000-0000-0000-0000-000000000099"
if(identical(Sys.getenv("PUGA_PREPARE_FIXTURE_EXPORT"),"true")) {
  job <- puga_analysis_latest(uid)
  stopifnot(length(job$request$tickers)==500L)
  puga_export_execute(job$path)
  stopifnot(puga_analysis_read(file.path(job$path,"export-state.rds"))$status=="ready")
} else {
  core <- puga_analysis_load_core(getwd())
  core$.get_spy_history <- function() NULL
  core$get_finviz_payload <- function(...) list(Price="100",`Target Price`="120",`EPS (ttm)`="5",`Book/sh`="20",`P/FCF`="15",`Cash/sh`="3",`Debt/Eq`="0.2",`EPS next 5Y`="10%")
  core$get_history_metrics <- function(...) list(history=NULL,tech=list(price=100,s20=98,s50=95,s200=90,rsi=55))
  base <- core$inst_run_pipeline(c("FIXA","FIXB"))
  symbols <- sprintf("FIX%05d",1:500)
  result <- base
  result$summary <- base$summary[rep(1,500),];result$summary$Ticker <- symbols
  result$ranking <- base$ranking[rep(1,500),];result$ranking$Ticker <- symbols;result$ranking$Rank <- seq_along(symbols)
  result$details <- setNames(rep(list(base$details[[1]]),500),symbols)
  result$history <- setNames(rep(list(NULL),500),symbols)
  result$backtest <- base$backtest[rep(1:2,500),];result$backtest$Ticker <- rep(symbols,each=2)
  old <- puga_analysis_latest(uid)
  if(!is.null(old)) {puga_job_release(old$request$ticket);unlink(old$path,recursive=TRUE)}
  job <- puga_analysis_submit(uid,symbols,"Balanceado",FALSE)
  puga_analysis_write(result,file.path(job$path,"result.rds"))
  puga_analysis_state(job$path,"ok","fixture complete",100)
  puga_job_release(job$request$ticket)
}
pool_close()
