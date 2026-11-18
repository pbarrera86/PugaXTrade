# No network or deployed database. Exercise real XLSX files at full symbol count.
source("R/analysis_store.R")
source("R/analysis_export.R")
local({
  root <- tempfile("export-fixture-");dir.create(root)
  previous <- Sys.getenv("PUGA_ANALYSIS_DIR",unset=NA_character_)
  Sys.setenv(PUGA_ANALYSIS_DIR=root)
  on.exit({unlink(root,recursive=TRUE);if(is.na(previous)) Sys.unsetenv("PUGA_ANALYSIS_DIR") else Sys.setenv(PUGA_ANALYSIS_DIR=previous)})
  owner <- "00000000-0000-0000-0000-000000000001"
  token <- paste(rep("A",48),collapse="")
  path <- file.path(puga_analysis_user_dir(owner),token)
  fixture <- function(n,metrics=40L) {
    symbols <- sprintf("FIX%05d",seq_len(n))
    detail <- data.frame(Categoria="Valoración",Indicador=paste0("Métrica ",seq_len(metrics)),Valor=rep(c(pi,NA_real_,-1.23456789,100.123456),length.out=metrics),Unidad="%",Semaforo="Verde",Peso=0.123456789,Razon="fixture",stringsAsFactors=FALSE)
    list(summary=data.frame(Ticker=symbols,Precio=123.456789,Puntaje=87.123456),
      ranking=data.frame(Rank=seq_len(n),Ticker=symbols,Puntaje=87.123456),
      details=setNames(rep(list(detail),n),symbols),
      backtest=data.frame(Ticker=rep(symbols,each=2L),Estrategia=rep(c("Buy & Hold","Sistema"),n),Retorno=0.123456789))
  }
  result <- fixture(3L)
  puga_analysis_write(result,file.path(path,"result.rds"))
  puga_analysis_write(list(user_id=owner,ticket=token),file.path(path,"request.rds"))
  puga_analysis_write(token,file.path(dirname(path),"latest.rds"))
  puga_analysis_state(path,"ok","fixture",100)
  checksum <- tools::md5sum(file.path(path,"result.rds"))
  puga_export_request(owner)
  stopifnot(puga_analysis_export_busy(path))
  stopifnot(grepl("exportación",tryCatch({puga_analysis_submit(owner,"OTHER","Balanceado",FALSE);""},error=conditionMessage)))
  stopifnot(grepl("exportación",tryCatch({puga_analysis_clear(owner);""},error=conditionMessage)))
  puga_export_execute(path)
  stopifnot(puga_analysis_read(file.path(path,"export-state.rds"))$status=="ready",!puga_analysis_export_busy(path),
    identical(checksum,tools::md5sum(file.path(path,"result.rds"))))
  file <- file.path(path,"analysis.xlsx")
  stopifnot(identical(openxlsx::getSheetNames(file),c("Resumen","Ranking","Detalles","Backtesting","Glosario")))
  details <- openxlsx::read.xlsx(file,sheet="Detalles",skipEmptyRows=FALSE)
  stopifnot(nrow(details)==120L,all(unique(details$Ticker)==result$summary$Ticker),
    abs(details$Valor[1]-pi)<1e-12,is.na(details$Valor[2]),abs(details$Peso[1]-0.123456789)<1e-12)
  cat("PASS: XLSX preserves owners, all detail fields and numeric precision; export cannot replace/delete analysis\n")
  large <- fixture(7300L)
  largefile <- file.path(root,"7300.xlsx")
  puga_export_workbook(large,largefile)
  stopifnot(length(openxlsx::getSheetNames(largefile))==5L,
    nrow(openxlsx::read.xlsx(largefile,sheet="Resumen"))==7300L,
    nrow(openxlsx::read.xlsx(largefile,sheet="Backtesting"))==14600L)
  xmlpath <- utils::unzip(largefile,files="xl/worksheets/sheet3.xml",exdir=file.path(root,"xml"))
  xml <- paste(readLines(xmlpath,warn=FALSE),collapse="")
  stopifnot(length(gregexpr("<row ",xml,fixed=TRUE)[[1]])==292001L)
  cat("PASS: 7300-symbol XLSX contains 292000 detail rows in five worksheets\n")
})
