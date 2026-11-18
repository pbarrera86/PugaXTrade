# Compact, lossless data export: a few sheets, not thousands of worksheets.
puga_export_workbook <- function(result,file) {
  wb <- openxlsx::createWorkbook()
  header <- openxlsx::createStyle(textDecoration="bold",fgFill="#17365D",fontColour="#FFFFFF")
  add <- function(name,data) {
    if(is.null(data) || !is.data.frame(data)) return(invisible(NULL))
    chunks <- if(nrow(data)) split(seq_len(nrow(data)),ceiling(seq_len(nrow(data))/950000L)) else list(integer())
    for(i in seq_along(chunks)) {
      sheet <- if(length(chunks)==1L) name else paste0(name,"_",i)
      openxlsx::addWorksheet(wb,sheet)
      openxlsx::writeData(wb,sheet,data[chunks[[i]],,drop=FALSE],headerStyle=header,withFilter=TRUE,keepNA=FALSE)
      openxlsx::freezePane(wb,sheet,firstRow=TRUE)
      if(ncol(data)) openxlsx::setColWidths(wb,sheet,cols=seq_len(ncol(data)),widths=20)
    }
    invisible(NULL)
  }
  add("Resumen",result$summary)
  add("Ranking",result$ranking)
  # Preserve numeric precision and units separately, plus every original field.
  details <- dplyr::bind_rows(result$details,.id="Ticker")
  add("Detalles",details)
  rm(details);gc(FALSE)
  add("Backtesting",result$backtest)
  # Load only the glossary definition, without sources or network providers.
  env <- new.env(parent=globalenv())
  for(e in parse("R/institutional_core.R"))
    if(is.call(e) && identical(e[[1]],as.name("<-")) && identical(e[[2]],as.name("inst_build_glossary_df"))) eval(e,env)
  add("Glosario",env$inst_build_glossary_df())
  openxlsx::saveWorkbook(wb,file,overwrite=TRUE)
  invisible(file)
}
puga_export_state <- function(path,status,msg) puga_analysis_write(
  list(status=status,msg=msg,updated=Sys.time()),file.path(path,"export-state.rds"))
puga_export_request <- function(user_id) {
  job <- puga_analysis_latest(user_id)
  if(is.null(job) || !identical(job$state$status,"ok")) stop("El análisis todavía no está listo para exportar.")
  if(file.exists(file.path(job$path,"analysis.xlsx"))) return(invisible(job))
  if(!puga_analysis_export_busy(job$path)) {
    puga_analysis_write(list(requested=Sys.time()),file.path(job$path,"export-request.rds"))
    puga_export_state(job$path,"queued","Preparando Excel en segundo plano. Puedes seguir consultando la tabla.")
  }
  invisible(job)
}
puga_export_execute <- function(path) {
  tmp <- tempfile(".excel-",tmpdir=path,fileext=".xlsx")
  on.exit(unlink(tmp),add=TRUE)
  tryCatch({
    result <- puga_analysis_read(file.path(path,"result.rds"))
    if(is.null(result) || !nrow(result$summary)) stop("No saved result")
    puga_export_workbook(result,tmp)
    Sys.chmod(tmp,"0600")
    if(!file.rename(tmp,file.path(path,"analysis.xlsx"))) stop("Cannot commit workbook")
    puga_export_state(path,"ready","Excel listo. Descárgalo con el botón Descargar Excel.")
  },error=function(e) {
    puga_export_state(path,"error","No se pudo preparar Excel. Tus resultados siguen guardados; vuelve a intentar la exportación.")
    message("[AnalysisExport] Workbook preparation failed; analysis retained.")
  })
  invisible(NULL)
}
# One export at a time, sharing the same two global capacity reservations.
puga_export_tick <- function(active,requests,first=FALSE,analysis_running=FALSE) {
  if(!is.null(active)) {
    if(Sys.time()>=active$deadline && mirai::unresolved(active$job)) {
      mirai::stop_mirai(active$job)
      puga_export_state(active$path,"error","La exportación excedió su plazo. Los resultados siguen guardados; puedes reintentar.")
    }
    if(!mirai::unresolved(active$job)) {
      if(mirai::is_error_value(active$job$data)) puga_export_state(active$path,"error","La exportación se interrumpió. Los resultados siguen guardados; puedes reintentar.")
      puga_job_release(active$ticket)
      unlink(file.path(active$path,"export-lease.rds"))
      active <- NULL
    }
  }
  for(file in requests) {
    path <- dirname(file)
    state <- puga_analysis_read(file.path(path,"state.rds"))
    requested <- puga_analysis_read(file.path(path,"export-request.rds"))
    export <- puga_analysis_read(file.path(path,"export-state.rds"))
    if(is.null(requested) || is.null(export) || !identical(state$status,"ok")) next
    if(first && (is.null(active) || !identical(active$path,path))) {
      lease <- puga_analysis_read(file.path(path,"export-lease.rds"))
      if(!is.null(lease)) {
        puga_job_release(lease$ticket)
        unlink(file.path(path,"export-lease.rds"))
      }
      if(identical(export$status,"running")) {
        puga_export_state(path,"queued","Procesador reiniciado; recuperando la preparación de Excel.")
        export$status <- "queued"
      }
    }
    if(file.exists(file.path(path,"analysis.xlsx"))) {
      if(!identical(export$status,"ready")) puga_export_state(path,"ready","Excel listo. Descárgalo con el botón Descargar Excel.")
      next
    }
    if(!identical(export$status,"queued")) next
    deadline <- requested$requested+1800
    if(Sys.time()>=deadline) {
      puga_export_state(path,"error","No se pudo preparar Excel en el plazo disponible. Los resultados siguen guardados; puedes reintentar.")
      next
    }
    if(!is.null(active) || analysis_running) next
    request <- puga_analysis_read(file)
    if(!db_can_analyze(db_get_user_by_id(request$user_id))) next
    ticket <- puga_job_acquire(request$user_id)
    if(is.null(ticket)) next
    puga_analysis_write(list(ticket=ticket),file.path(path,"export-lease.rds"))
    puga_export_state(path,"running","Preparando Excel en segundo plano. Puedes seguir consultando la tabla.")
    job <- mirai::mirai({
      setwd(app_dir);source("R/analysis_store.R");source("R/analysis_export.R")
      puga_export_execute(path)
    },app_dir=getwd(),path=path,.timeout=max(1,as.numeric(difftime(deadline,Sys.time(),units="secs"))*1000))
    active <- list(job=job,path=path,ticket=ticket,deadline=deadline)
  }
  active
}
