#!/usr/bin/env bash
set -Eeuo pipefail

# entrypoint.sh - PugaX Trade

APP_DIR="/srv/shiny-server/app"
R_ENV_DIR="/etc/R"
R_ENVIRON_SITE="${R_ENV_DIR}/Renviron.site"
SHINY_RENVIRON="/srv/shiny-server/.Renviron"
APP_RENVIRON="${APP_DIR}/.Renviron"
LOG_DIR="/var/log/shiny-server"
APP_CACHE_DIR="${APP_DIR}/app_cache"

# Filtro de variables críticas de entorno
ENV_FILTER='^(PG|STRIPE_|SMTP_|PUBLIC_|NEON_|DATABASE_URL|DATABASE_|PORT|SUPER_ADMIN|DEFAULT_REFERRER|REFERRAL_COMMISSION)='

log() {
  echo "[entrypoint] $*"
}

warn() {
  echo "[entrypoint][WARN] $*" >&2
}

log "Configurando variables de entorno..."

# Crear directorios necesarios
mkdir -p "$APP_DIR" "$LOG_DIR" "$APP_CACHE_DIR" "$R_ENV_DIR" "/srv/shiny-server"

# 1) Variables para R globalmente
if env | grep -E "$ENV_FILTER" > "$R_ENVIRON_SITE"; then
  chmod 0644 "$R_ENVIRON_SITE"
else
  : > "$R_ENVIRON_SITE"
  chmod 0644 "$R_ENVIRON_SITE"
  warn "No se encontraron variables filtradas para $R_ENVIRON_SITE"
fi

# 2) Copia en /srv/shiny-server/.Renviron
if env | grep -E "$ENV_FILTER" > "$SHINY_RENVIRON"; then
  chmod 0600 "$SHINY_RENVIRON"
else
  : > "$SHINY_RENVIRON"
  chmod 0600 "$SHINY_RENVIRON"
  warn "No se encontraron variables filtradas para $SHINY_RENVIRON"
fi

# 3) Copia dentro de la app
cp -f "$SHINY_RENVIRON" "$APP_RENVIRON"
chmod 0600 "$APP_RENVIRON"

# Ownership si existe usuario shiny
chown -R shiny:shiny "$APP_CACHE_DIR" 2>/dev/null || true
chown shiny:shiny "$APP_RENVIRON" 2>/dev/null || true
chown shiny:shiny "$SHINY_RENVIRON" 2>/dev/null || true
chown -R shiny:shiny "$LOG_DIR" 2>/dev/null || true

# Logs de workers a stderr
export SHINY_LOG_STDERR=1

# Inicialización de BD
if [ -f "${APP_DIR}/db_init.R" ]; then
  log "Ejecutando migraciones de Base de Datos..."
  cd "$APP_DIR"
  Rscript db_init.R || warn "db_init.R devolvió error; se continúa con el arranque."
else
  warn "No se encontró ${APP_DIR}/db_init.R; se omite inicialización de base de datos."
fi

log "Iniciando Shiny Server..."

if command -v shiny-server >/dev/null 2>&1; then
  if shiny-server --help 2>&1 | grep -q -- '--foreground'; then
    exec shiny-server --foreground
  else
    shiny-server &
    SHINY_PID=$!

    sleep 2

    if compgen -G "${LOG_DIR}/*.log" > /dev/null; then
      tail -n +1 -qF "${LOG_DIR}"/*.log &
    else
      warn "No se encontraron logs en ${LOG_DIR}"
    fi

    wait "$SHINY_PID"
  fi
else
  warn "No se encontró shiny-server en el contenedor."
  exit 1
fi
