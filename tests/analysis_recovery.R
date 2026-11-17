# Offline crash/reconnect tests against the same durable storage and executor.
source("R/job_capacity.R")
source("R/analysis_store.R")
source("R/analysis_worker.R")
local({
  root <- tempfile("analysis-recovery-")
  dir.create(root)
  previous <- Sys.getenv("PUGA_ANALYSIS_DIR",unset=NA_character_)
  Sys.setenv(PUGA_ANALYSIS_DIR=root)
  on.exit({unlink(root,recursive=TRUE);if(is.na(previous)) Sys.unsetenv("PUGA_ANALYSIS_DIR") else Sys.setenv(PUGA_ANALYSIS_DIR=previous)})
  owner <- "00000000-0000-0000-0000-000000000001"
  other <- "00000000-0000-0000-0000-000000000002"
  path <- file.path(puga_analysis_user_dir(owner),paste(rep("A",48),collapse=""))
  request <- list(user_id=owner,ticket=basename(path),tickers=c("ONE","TWO","THREE"),profile="Balanceado",use_pg=FALSE,created=Sys.time(),deadline=Sys.time()+3600)
  puga_analysis_write(request,file.path(path,"request.rds"))
  puga_analysis_write(basename(path),file.path(dirname(path),"latest.rds"))
  puga_analysis_state(path,"queued","fixture")
  calls <- character()
  runner <- function(tickers,investor_profile,use_postgres_cache,progress_cb,checkpoint_cb) {
    out <- vector("list",length(tickers))
    for(i in seq_along(tickers)) {
      progress_cb(tickers[i],i*20)
      out[i] <- list(checkpoint_cb(i,function() {
        calls <<- c(calls,tickers[i])
        if(identical(tickers[i],"TWO") && sum(calls=="TWO")==1L) stop("simulated process interruption")
        list(ticker=tickers[i],value=if(identical(tickers[i],"THREE")) NA_real_ else 42)
      }))
    }
    list(summary=data.frame(Ticker=tickers),values=out)
  }
  puga_analysis_execute(getwd(),path,request,runner)
  check(puga_analysis_read(file.path(path,"state.rds"))$status=="error","interrupted worker preserves recoverable state")
  check(file.exists(file.path(path,"tickers","00001.rds")),"completed ticker committed before crash")
  # A fresh viewer obtains its job from its authenticated owner only.
  check(identical(puga_analysis_latest(owner)$request,request),"reconnected viewer recovers original request")
  check(is.null(puga_analysis_latest(other)),"another owner cannot discover the job")
  check(inherits(try(puga_analysis_user_dir("../../secret"),silent=TRUE),"try-error"),"browser-style path traversal rejected")
  puga_analysis_execute(getwd(),path,request,runner)
  check(sum(calls=="ONE")==1L && sum(calls=="TWO")==2L,"resume computes only uncommitted tickers")
  check(puga_analysis_read(file.path(path,"state.rds"))$status=="ok","resumed analysis completes")
  check(nrow(puga_analysis_read(file.path(path,"result.rds"))$summary)==3L,"final result survives a new viewer")
  puga_analysis_execute(getwd(),path,request,runner)
  check(sum(calls=="THREE")==1L,"completed no-data ticker also survives resume")
  failed_calls <- 0L
  for(attempt in 1:2) puga_analysis_checkpoint(path,99L,function(){failed_calls <<- failed_calls+1L;NULL})
  check(failed_calls==2L && !file.exists(file.path(path,"tickers","00099.rds")),"failed tickers remain pending instead of poisoning resume checkpoints")
  puga_analysis_write(list(value=NULL),file.path(path,"tickers","00099.rds"))
  check(puga_analysis_checkpoint(path,99L,function() 42L)==42L,"legacy failed-ticker checkpoint is retried and repaired")
  file.create(file.path(path,"cancel"))
  puga_analysis_execute(getwd(),path,request,runner)
  check(puga_analysis_read(file.path(path,"state.rds"))$status=="cancelled","explicit cancellation stops work")
  unlink(file.path(path,"cancel"))
  request$deadline <- Sys.time()-1
  puga_analysis_execute(getwd(),path,request,runner)
  check(puga_analysis_read(file.path(path,"state.rds"))$status=="paused","six-hour deadline pauses without losing checkpoints")
  writeLines("corrupt",file.path(path,"tickers","00001.rds"))
  check(inherits(try(puga_analysis_checkpoint(path,1,function() stop("must not recalculate")),silent=TRUE),"try-error"),"corrupt checkpoint never silently restarts")
  release <- puga_job_release
  puga_job_release <- function(token) invisible(NULL)
  clear <- puga_analysis_clear
  environment(clear) <- environment()
  clear(owner)
  puga_job_release <- release
  check(is.null(puga_analysis_latest(owner)) && !dir.exists(path),"owner can delete saved analysis")
  # Exercise the actual production ticker loop without network providers.
  pipeline_env <- new.env(parent=environment())
  for(e in parse("R/institutional_core.R"))
    if(is.call(e) && identical(e[[1]],as.name("<-")) && identical(e[[2]],as.name("inst_run_pipeline"))) eval(e,pipeline_env)
  pipeline_env$inst_parse_tickers <- function(x) strsplit(x,",",fixed=TRUE)[[1]]
  pipeline_env$.get_spy_history <- function() data.frame(fixture=1)
  calculated <- 0L
  pipeline_env$analyze_one_ticker <- function(...) {calculated <<- calculated+1L;NULL}
  for(attempt in 1:2) try(pipeline_env$inst_run_pipeline(c("ONE","TWO"),
    checkpoint_cb=function(index,compute) puga_analysis_checkpoint(path,index,compute)),silent=TRUE)
  check(calculated==4L,"production pipeline retries failed tickers after recovery")
})
