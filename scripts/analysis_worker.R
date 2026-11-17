setwd("/srv/shiny-server/app")
source("R/db.R")
source("R/security.R")
source("R/job_capacity.R")
source("R/analysis_store.R")
source("R/analysis_worker.R")
dir.create(puga_analysis_root(),recursive=TRUE,showWarnings=FALSE,mode="0700")
running <- list()
first <- TRUE
last_cleanup <- as.numeric(Sys.time())
repeat {
  tryCatch({
    owners <- list.dirs(puga_analysis_root(),recursive=FALSE,full.names=TRUE)
    paths <- unlist(lapply(owners,list.dirs,recursive=FALSE,full.names=TRUE),use.names=FALSE)
    requests <- file.path(paths,"request.rds")
    requests <- requests[file.exists(requests)]
    seen_tokens <- character()
    for(file in requests) {
      path <- dirname(file)
      request <- puga_analysis_read(file)
      state <- puga_analysis_read(file.path(path,"state.rds"))
      if(is.null(state)) next
      token <- request$ticket
      seen_tokens <- c(seen_tokens,token)
      # This service has one replica. Container restart kills all its child
      # processes, so abandoned jobs can safely resume from committed tickers.
      if(first && identical(state$status,"running")) {
        puga_analysis_state(path,"queued","Procesador reiniciado; recuperando avance guardado.")
        state$status <- "queued"
      }
      active <- running[[token]]
      if(!is.null(active)) {
        if(file.exists(file.path(path,"cancel")) || Sys.time()>=request$deadline) {
          mirai::stop_mirai(active)
          cancelled <- file.exists(file.path(path,"cancel"))
          puga_analysis_state(path,if(cancelled) "cancelled" else "paused",
            if(cancelled) "Análisis cancelado por el usuario." else "Se alcanzaron 6 horas. El avance está guardado para retomarlo.")
          running[[token]] <- NULL
        } else if(!mirai::unresolved(active)) {
          value <- active$data
          if(mirai::is_error_value(value) || inherits(value,"error"))
            puga_analysis_state(path,"error","El proceso se interrumpió. El avance está guardado; puedes retomarlo.")
          running[[token]] <- NULL
        }
        state <- puga_analysis_read(file.path(path,"state.rds"))
      }
      if(!state$status %in% c("queued","running")) {
        released <- puga_analysis_read(file.path(path,"released.rds"))
        if(!identical(released,token)) {
          puga_job_release(token)
          puga_analysis_write(token,file.path(path,"released.rds"))
        }
        next
      }
      if(!is.null(running[[token]]) || length(running)>=2L) next
      if(file.exists(file.path(path,"cancel")) || Sys.time()>=request$deadline) {
        cancelled <- file.exists(file.path(path,"cancel"))
        puga_analysis_state(path,if(cancelled) "cancelled" else "paused",
          if(cancelled) "Análisis cancelado por el usuario." else "Se alcanzaron 6 horas. El avance está guardado para retomarlo.")
        next
      }
      allowed <- .with_conn(nrow(DBI::dbGetQuery(con,"SELECT 1 FROM analysis_jobs WHERE token=$1 AND user_id=$2 AND expires_at>now()",params=list(token,request$user_id)))==1L)
      if(!allowed || !db_can_analyze(db_get_user_by_id(request$user_id))) {
        puga_analysis_state(path,"error","El trabajo no tiene una reserva o acceso vigente. El avance está guardado.")
        next
      }
      puga_analysis_state(path,"running","Recuperando avance guardado...",1)
      remaining <- max(1,as.numeric(difftime(request$deadline,Sys.time(),units="secs"))*1000)
      running[[token]] <- mirai::mirai({
        setwd(app_dir)
        source("R/analysis_store.R")
        source("R/analysis_worker.R")
        puga_analysis_execute(getwd(),path,request)
      },app_dir=getwd(),path=path,request=request,.timeout=remaining)
    }
    # A viewer may clear a finished workspace before the next coordinator tick.
    for(token in names(running)) if(!token %in% seen_tokens && !mirai::unresolved(running[[token]])) running[[token]] <- NULL
    first <- FALSE
    # Seven-day recovery window; active workspaces are never removed.
    if(as.numeric(Sys.time())-last_cleanup>3600) {
      for(file in requests) {
        path <- dirname(file)
        state <- puga_analysis_read(file.path(path,"state.rds"))
        if(!is.null(state) && !state$status %in% c("queued","running") &&
           as.numeric(difftime(Sys.time(),state$updated,units="days"))>7) {
          pointer <- file.path(dirname(path),"latest.rds")
          if(identical(puga_analysis_read(pointer),basename(path))) unlink(pointer)
          unlink(path,recursive=TRUE)
        }
      }
      last_cleanup <- as.numeric(Sys.time())
    }
    writeLines(format(Sys.time(),tz="UTC"),"/tmp/puga-analysis-worker-heartbeat")
  },error=function(e) message("[AnalysisWorker] Coordinator unavailable; will retry."))
  Sys.sleep(2)
}
