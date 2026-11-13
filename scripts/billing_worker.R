# Runs outside interactive Shiny processes, using the restricted runtime role.
setwd("/srv/shiny-server/app")
source("R/db.R")
source("R/billing.R")
source("R/billing_reconciliation.R")
repeat {
  tryCatch({
    stripe_reconcile_once()
  },error=function(e) {
    # Do not put Stripe response bodies, identifiers or credentials into logs/UI.
    message("[BillingWorker] Reconciliation incomplete; will retry.")
    try(stripe_reconciliation_check(FALSE,"No se completó la reconciliación de Stripe. Revisar configuración o conexión; se reintentará."),silent=TRUE)
  })
  writeLines(format(Sys.time(),tz="UTC"),"/tmp/puga-billing-worker-heartbeat")
  Sys.sleep(300)
}
