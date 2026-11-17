# Real independent mirai processes; deterministic fixture, no market requests.
source("R/job_capacity.R")
source("R/analysis_store.R")
source("R/analysis_worker.R")
local({
  path <- tempfile("process-recovery-")
  dir.create(path)
  on.exit(unlink(path,recursive=TRUE))
  request <- list(tickers=c("ONE","TWO"),profile="Balanceado",use_pg=FALSE,deadline=Sys.time()+60)
  fixture <- function(tickers,investor_profile,use_postgres_cache,progress_cb,checkpoint_cb) {
    for(i in seq_along(tickers)) {
      checkpoint_cb(i,function() {
        cat(tickers[i],"\n",file=file.path(path,"computed"),append=TRUE)
        if(i==2L && !file.exists(file.path(path,"continue"))) {
          file.create(file.path(path,"blocked"))
          repeat Sys.sleep(0.1)
        }
        list(ticker=tickers[i])
      })
    }
    list(summary=data.frame(Ticker=tickers))
  }
  launch <- function() mirai::mirai({
    source("R/analysis_store.R");source("R/analysis_worker.R")
    environment(fixture) <- environment()
    puga_analysis_execute(getwd(),path,request,fixture)
  },path=path,request=request,fixture=fixture,.timeout=30000)
  m <- launch()
  deadline <- Sys.time()+20
  while(!file.exists(file.path(path,"blocked")) && Sys.time()<deadline && mirai::unresolved(m)) Sys.sleep(0.1)
  stopifnot(file.exists(file.path(path,"tickers","00001.rds")),file.exists(file.path(path,"blocked")))
  mirai::stop_mirai(m)
  # Reconstruct the worker in a fresh process, with no first-process memory.
  file.create(file.path(path,"continue"))
  m <- launch()
  deadline <- Sys.time()+20
  while(mirai::unresolved(m) && Sys.time()<deadline) Sys.sleep(0.1)
  stopifnot(!mirai::unresolved(m),!mirai::is_error_value(m$data),
    identical(puga_analysis_read(file.path(path,"state.rds"))$status,"ok"),
    sum(trimws(readLines(file.path(path,"computed")))=="ONE")==1L,
    nrow(puga_analysis_read(file.path(path,"result.rds"))$summary)==2L)
  cat("PASS: real process restart resumes after last committed ticker\n")
})
