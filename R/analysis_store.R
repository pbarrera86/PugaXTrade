# Private durable workspaces, shared only by Shiny and the analysis worker.
# Never serve this directory through www/ or accept paths from the browser.
puga_analysis_root <- function() Sys.getenv("PUGA_ANALYSIS_DIR", "/var/lib/pugaxtrade/analysis")
puga_analysis_user_dir <- function(user_id) {
  if (length(user_id)!=1L || is.na(user_id) || !grepl("^[a-fA-F0-9-]{36}$",user_id)) stop("Invalid analysis owner")
  file.path(puga_analysis_root(),tolower(user_id))
}
puga_analysis_write <- function(value,path) {
  dir.create(dirname(path),recursive=TRUE,showWarnings=FALSE,mode="0700")
  tmp <- tempfile(".write-",tmpdir=dirname(path))
  on.exit(unlink(tmp),add=TRUE)
  saveRDS(value,tmp,compress=FALSE)
  Sys.chmod(tmp,"0600")
  if (!file.rename(tmp,path)) stop("Cannot commit analysis checkpoint")
  invisible(value)
}
puga_analysis_read <- function(path) {
  if(!file.exists(path)) return(NULL)
  readRDS(path) # A corrupt checkpoint must fail visibly, never silently restart.
}
puga_analysis_latest <- function(user_id) {
  base <- puga_analysis_user_dir(user_id)
  token <- puga_analysis_read(file.path(base,"latest.rds"))
  if(is.null(token)) return(NULL)
  if(!is.character(token) || length(token)!=1L || !grepl("^[a-zA-Z0-9]{48}$",token)) stop("Invalid analysis reference")
  path <- file.path(base,token)
  request <- puga_analysis_read(file.path(path,"request.rds"))
  if(is.null(request) || !identical(request$user_id,tolower(user_id))) stop("Analysis owner mismatch")
  list(path=path,request=request,state=puga_analysis_read(file.path(path,"state.rds")))
}
puga_analysis_state <- function(path,status,msg,pct=0) {
  puga_analysis_write(list(status=status,msg=msg,pct=as.integer(pct),updated=Sys.time()),file.path(path,"state.rds"))
}
puga_analysis_submit <- function(user_id,tickers,profile,use_pg,resume=FALSE) {
  user_id <- tolower(user_id)
  old <- puga_analysis_latest(user_id)
  if(!is.null(old) && old$state$status %in% c("queued","running")) stop("Ya tienes un análisis en curso.")
  if(resume && (is.null(old) || !old$state$status %in% c("error","paused"))) stop("No hay un análisis pendiente para retomar.")
  token <- puga_job_acquire(user_id)
  if(is.null(token)) stop("Hay análisis en curso. Intenta de nuevo en unos minutos.")
  committed <- FALSE
  on.exit(if(!committed) try(puga_job_release(token),silent=TRUE),add=TRUE)
  if(resume) {
    path <- old$path
    request <- old$request
    unlink(file.path(path,c("cancel","result.rds")))
  } else {
    path <- file.path(puga_analysis_user_dir(user_id),token)
    request <- list(user_id=user_id,tickers=tickers,profile=profile,use_pg=isTRUE(use_pg),created=Sys.time(),version=1L)
  }
  request$ticket <- token
  request$deadline <- Sys.time()+puga_job_timeout_ms/1000
  # Publish queued state only after the complete request has been committed.
  puga_analysis_write(request,file.path(path,"request.rds"))
  puga_analysis_write(basename(path),file.path(dirname(path),"latest.rds"))
  puga_analysis_state(path,"queued","Análisis guardado; esperando al procesador.")
  committed <- TRUE
  if(!resume && !is.null(old)) unlink(old$path,recursive=TRUE)
  puga_analysis_latest(user_id)
}
puga_analysis_cancel <- function(job) {
  if(!is.null(job)) file.create(file.path(job$path,"cancel"))
  invisible(NULL)
}
puga_analysis_clear <- function(user_id) {
  job <- puga_analysis_latest(user_id)
  if(is.null(job)) return(invisible(NULL))
  if(job$state$status %in% c("queued","running")) stop("Espera a que termine o cancela el análisis.")
  puga_job_release(job$request$ticket)
  unlink(job$path,recursive=TRUE)
  unlink(file.path(puga_analysis_user_dir(user_id),"latest.rds"))
  invisible(NULL)
}
puga_analysis_checkpoint <- function(path,index,compute) {
  target <- file.path(path,"tickers",sprintf("%05d.rds",as.integer(index)))
  saved <- puga_analysis_read(target)
  if(!is.null(saved) && (!is.null(saved$value) || index==0L)) return(saved$value)
  value <- compute()
  # NULL from a ticker means execution failed, not that it completed. Retry it
  # on resume. A completed no-data ticker has its normal Sin datos summary.
  if(is.null(value) && index>0L) return(NULL)
  puga_analysis_write(list(value=value),target)
  value
}
puga_analysis_check <- function(path,request,now=Sys.time()) {
  if(file.exists(file.path(path,"cancel"))) stop(structure(list(message="Análisis cancelado por el usuario.",call=NULL),class=c("puga_cancel","error","condition")))
  if(now>=request$deadline) stop(structure(list(message="Se alcanzaron 6 horas. El avance está guardado para retomarlo.",call=NULL),class=c("puga_pause","error","condition")))
  invisible(NULL)
}
