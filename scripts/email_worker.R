# Dedicated process: SMTP failures cannot crash an interactive Shiny process.
setwd("/srv/shiny-server/app")
source("R/db.R")
source("R/auth_helpers.R")
repeat {
  tryCatch(.with_conn({
    DBI::dbWithTransaction(con, {
      row <- DBI::dbGetQuery(con,"SELECT * FROM email_outbox WHERE sent_at IS NULL AND attempts<8 AND next_attempt_at<=now() ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED")
      if(nrow(row)) {
        cfg <- .auth_load_cfg()
        if(!nzchar(cfg$host %||% "")) stop("SMTP configuration missing")
        from <- cfg$from %||% "contacto@pugaxtrade.com"
        envelope <- if(grepl("<.*>",from)) sub(".*<(.*)>.*","\\1",from) else from
        msg <- tempfile(fileext=".eml")
        conf <- tempfile(fileext=".curl")
        # Credentials stay out of process arguments; files are private and removed.
        writeLines(.build_mime_message(from,row$recipient[[1]],row$subject[[1]],row$body[[1]]),msg,useBytes=TRUE)
        escape <- function(x) gsub('"','\\"',gsub('\\','\\\\',x,fixed=TRUE),fixed=TRUE)
        writeLines(paste0('user = "',escape(paste0(cfg$user,":",cfg$pass)),'"'),conf)
        Sys.chmod(c(msg,conf),"0600")
        scheme <- if(identical(as.character(cfg$port),"465")) "smtps://" else "smtp://"
        rc <- tryCatch(system2("curl",args=c("--config",shQuote(conf),"--url",shQuote(paste0(scheme,cfg$host,":",cfg$port %||% "587")),
          "--ssl-reqd","--connect-timeout","5","--max-time","15","--mail-from",shQuote(envelope),
          "--mail-rcpt",shQuote(row$recipient[[1]]),"--upload-file",shQuote(msg),"--silent"),stdout=FALSE,stderr=FALSE),error=function(e) 1L)
        unlink(c(msg,conf))
        if(identical(rc,0L)) {
          DBI::dbExecute(con,"UPDATE email_outbox SET sent_at=now(),attempts=attempts+1,body='',last_error=NULL WHERE id=$1",params=list(row$id[[1]]))
        } else {
          DBI::dbExecute(con,"UPDATE email_outbox SET attempts=attempts+1,next_attempt_at=now()+interval '5 minutes',last_error='SMTP delivery failed' WHERE id=$1",params=list(row$id[[1]]))
        }
      }
    })
  }),error=function(e) message("[EmailWorker] Delivery paused: ",conditionMessage(e)))
  Sys.sleep(5)
}
