# Shared server-side policy. No bearer token is sent through Shiny inputs.
auth_token_hash <- function(token) paste0("sha256:", digest::digest(token, algo = "sha256", serialize = FALSE))

auth_valid_password <- function(password) {
  is.character(password) && length(password) == 1L && !is.na(password) && nchar(password) >= 8L && nchar(password, type = "bytes") <= 1024L
}

db_current_session_user <- function(session) {
  tok <- shiny::isolate(session$userData$session_token())
  if (is.null(tok) || !nzchar(tok)) return(NULL)
  tryCatch(db_find_user_by_token(tok), error = function(e) NULL)
}

db_can_analyze <- function(user) {
  if (is.null(user) || nrow(user) != 1L || !isTRUE(user$active[[1]]) || !isTRUE(user$email_verified[[1]])) return(FALSE)
  if (is_super_admin(user) || db_is_admin(user) || db_membership_is_active(user)) return(TRUE)
  days <- db_trial_days_left(user)
  length(days) == 1L && !is.na(days) && days >= 0L
}

db_create_login_handoff <- function(user_id) {
  .with_conn({
    code <- rnd_token(32)
    DBI::dbExecute(con, "INSERT INTO login_handoffs(code_hash,user_id,expires_at) VALUES($1,$2,now()+interval '60 seconds')",
      params = list(auth_token_hash(code), user_id))
    code
  })
}

db_resend_verification <- function(identifier) {
  .with_conn({
    DBI::dbWithTransaction(con, {
      u <- DBI::dbGetQuery(con,"SELECT id,email,name FROM users WHERE (lower(email)=lower($1) OR lower(username)=lower($1)) AND active=true AND email_verified=false FOR UPDATE",params=list(identifier))
      if(nrow(u)==1L) {
        token <- rnd_token(32)
        DBI::dbExecute(con,"UPDATE users SET verification_token=$1,verification_expires_at=now()+interval '48 hours' WHERE id=$2",params=list(auth_token_hash(token),u$id[[1]]))
        body <- paste0("Verifica tu cuenta PugaXTrade en este enlace (válido durante 48 horas):\n",.app_base_url(),"?verify=",token)
        DBI::dbExecute(con,"INSERT INTO email_outbox(recipient,subject,body) VALUES($1,$2,$3)",params=list(u$email[[1]],"Verifica tu cuenta PugaXTrade",body))
      }
      invisible(TRUE)
    })
  })
}

db_redeem_login_handoff <- function(code) {
  if (!is.character(code) || length(code) != 1L || !grepl("^[a-f0-9]{64}$", code)) return(NULL)
  .with_conn({
    DBI::dbWithTransaction(con, {
      h <- DBI::dbGetQuery(con, "SELECT user_id FROM login_handoffs WHERE code_hash=$1 AND expires_at>now()",
        params = list(auth_token_hash(code)))
      if (!nrow(h)) NULL else {
        u <- DBI::dbGetQuery(con, "SELECT id FROM users WHERE id=$1 AND active=true AND email_verified=true FOR UPDATE", params = list(h$user_id[[1]]))
        consumed <- DBI::dbExecute(con,"DELETE FROM login_handoffs WHERE code_hash=$1 AND expires_at>now()",params=list(auth_token_hash(code)))
        if (!nrow(u) || consumed!=1L) NULL else {
          tok <- rnd_token(64)
          DBI::dbExecute(con, "DELETE FROM sessions WHERE user_id=$1", params = list(u$id[[1]]))
          DBI::dbExecute(con, "INSERT INTO sessions(token,user_id,expires_at) VALUES($1,$2,now()+interval '7 days')",
            params = list(auth_token_hash(tok), u$id[[1]]))
          tok
        }
      }
    })
  })
}
