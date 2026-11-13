# No network, SMTP or production database access. Run from repository root.
files <- c("app.R","db_init.R",list.files("R",full.names=TRUE,pattern="[.]R$"),list.files("scripts",full.names=TRUE,pattern="[.]R$"))
for(f in files) parse(f)
cat("PASS: all R files parse\n")
source("R/helpers.R")
source("R/security.R")
source("R/billing.R")
check <- function(value,name) {stopifnot(isTRUE(value));cat("PASS:",name,"\n")}
check(!auth_valid_password("short") && auth_valid_password("long enough password"),"password policy")
check(identical(auth_token_hash("abc"),"sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),"token hash is SHA256")
now <- 1800000000
payload <- '{"id":"evt_fixture"}'
sig <- digest::hmac("whsec_fixture",paste0(now,".",payload),algo="sha256",serialize=FALSE)
check(verify_stripe_signature(payload,paste0("t=",now,",v1=",sig),"whsec_fixture",now),"valid signature")
check(verify_stripe_signature(payload,paste0("t=",now,",v1=bad,v1=",sig),"whsec_fixture",now),"rotating signatures")
check(!verify_stripe_signature(paste0(payload,"x"),paste0("t=",now,",v1=",sig),"whsec_fixture",now),"tampered payload rejected")
check(!verify_stripe_signature(payload,paste0("t=",now,",v1=",sig),"whsec_fixture",now+301),"expired signature rejected")
check(!verify_stripe_signature(payload,paste0("t=",now,",t=",now,",v1=",sig),"whsec_fixture",now),"ambiguous timestamp rejected")
sub <- list(status="active",latest_invoice=list(status="paid"),items=list(data=list(list(price=list(id="price_fixture"),current_period_end=now+86400))))
check(stripe_access(sub,"price_fixture",now)$active,"active paid period")
sub$latest_invoice$status <- "open"
check(!stripe_access(sub,"price_fixture",now)$active,"unpaid invoice cannot grant access")
sub$latest_invoice$status <- "paid"
check(!stripe_access(sub,"price_other",now)$matching,"wrong product rejected")
check(!stripe_access(sub,"price_fixture",now+86401)$active,"expired access denied")
sub$status <- "past_due"
check(!stripe_access(sub,"price_fixture",now)$active,"past due denied")
sub$status <- "canceled"
check(!stripe_access(sub,"price_fixture",now)$active,"canceled denied")

suppressPackageStartupMessages(library(dplyr))
# Evaluate just the production backtest definition; avoid loading any data source.
exprs <- parse("R/institutional_core.R")
for(e in exprs) if(is.call(e) && identical(e[[1]],as.name("<-")) && identical(e[[2]],as.name("run_backtest"))) eval(e)
history <- data.frame(Fecha=as.Date("2026-01-01")+0:59,Cierre=rep(100,60),RSI14=rep(50,60),SMA200=rep(90,60))
history$Cierre[60] <- 200
history$RSI14[60] <- 30
b <- run_backtest(history,cost_bps=0)
check(b$`Retorno Total (%)`[2]==0,"new signal cannot earn previous return")
history$RSI14[59] <- 30
b <- run_backtest(history,cost_bps=0)
check(b$`Retorno Total (%)`[2]==100,"prior close signal earns next interval")
check(run_backtest(history,cost_bps=10)$`Retorno Total (%)`[2]<100,"trading costs reduce return")
history$Cierre <- c(100,rep(50,59));history$RSI14 <- rep(50,60)
check(run_backtest(history,cost_bps=0)$`Max Drawdown (%)`[1]==-50,"drawdown includes initial capital")


source("R/http_routes.R")
parser <- function(x) if(identical(x,"action=set-session")) list(action="set-session") else list()
respond <- function(name) function(req) list(status=200,headers=list(),content=name)
router <- puga_http_handler(respond("ui"),parser,respond("auth"),respond("logout"),respond("webhook"))
check(router(list(PATH_INFO="/webhook",REQUEST_METHOD="POST"))$content=="webhook","POST webhook routed before UI")
check(router(list(PATH_INFO="/",QUERY_STRING="action=set-session",REQUEST_METHOD="POST"))$content=="auth","POST login exchange routed")
check(router(list(PATH_INFO="/",REQUEST_METHOD="GET"))$content=="ui","normal UI preserved")
check(router(list(PATH_INFO="/auth/logout",REQUEST_METHOD="POST"))$headers$`Cache-Control`=="no-store","authentication responses never cached")

cat("REGRESSION SUITE OK\n")

source("R/job_capacity.R")
check(puga_ticker_count_allowed(sprintf("X%d",1:7300)),"7300 tickers accepted at request boundary")
check(!puga_ticker_count_allowed(sprintf("X%d",1:7301)),"7301 tickers rejected at request boundary")
