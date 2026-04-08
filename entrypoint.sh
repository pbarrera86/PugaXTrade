#!/usr/bin/env bash
set -Eeuo pipefail

# entrypoint.sh - PugaX Trade

APP_DIR="/srv/shiny-server/app"
SHINY_ROOT="/srv/shiny-server"
R_ENVIRON_SITE="/etc/R/Renviron.site"
SHINY_RENVIRON="${SHINY_ROOT}/.Renviron"
APP_RENVIRON="${APP_DIR}/.Renviron"
SHINY_LOG_DIR="/var/log/shiny-server"
APP_CACHE_DIR="${APP_DIR}/app_cache"

# Filtro de variables críticas de entorno
ENV_FILTER='^(PG|STRIPE_|SMTP_|PUBLIC_|NEON_|DATABASE_URL|DATABASE_|PORT|SUPER_ADMIN|DEFAULT_REFERRER|REFERRAL_COMMISSION)='

log() {
  echo "[entrypoint] $*"
}

warn() {
  echo "[entrypoint][WARN] $*" >&2
}

# Asegurar directorios base
mkdir -p "$SHINY_ROOT" "$APP_DIR" "$SHINY_LOG_DIR" "$APP_CACHE_DIR"

log "Configurando variables de entorno..."

# 1) /etc/R/Renviron.site
#    Cargado automáticamente por procesos R, incluyendo workers de Shiny Server.
#    Se sobreescribe con solo las variables filtradas.
if env | grep -E "$ENV_FILTER" > "$R_ENVIRON_SITE"; then
  chmod 0644 "$R_ENVIRON_SITE"
else
  : > "$R_ENVIRON_SITE"
  chmod 0644 "$R_ENVIRON_SITE"
  warn "No se encontraron variables de entorno que coincidan con el filtro para $R_ENVIRON_SITE."
fi

# 2) Copia de respaldo en /srv/shiny-server/.Renviron
if env | grep -E "$ENV_FILTER" > "$SHINY_RENVIRON"; then
  chmod 0600 "$SHINY_RENVIRON"
  chown shiny:shiny "$SHINY_RENVIRON" 2>/dev/null || true
else
  : > "$SHINY_RENVIRON"
  chmod 0600 "$SHINY_RENVIRON"
  chown shiny:shiny "$SHINY_RENVIRON" 2>/dev/null || true
  warn "No se encontraron variables de entorno que coincidan con el filtro para $SHINY_RENVIRON."
fi

# 3) Copiar al directorio de la app
cp -f "$SHINY_RENVIRON" "$APP_RENVIRON"
chmod 0600 "$APP_RENVIRON"
chown shiny:shiny "$APP_RENVIRON" 2>/dev/null || true

# Validación opcional de libicu70
if [ ! -f "/usr/lib/x86_64-linux-gnu/libicui18n.so.70" ]; then
  warn "libicui18n.so.70 no encontrada. Si tu app la necesita, reconstruye la imagen Docker."
fi

# Exportar para que los workers envíen errores al stderr principal
export SHINY_LOG_STDERR=1

# Permisos de logs y caché
chown -R shiny:shiny "$SHINY_LOG_DIR" 2>/dev/null || true
chown -R shiny:shiny "$APP_CACHE_DIR" 2>/dev/null || true

# Migraciones / inicialización de DB
if [ -f "${APP_DIR}/db_init.R" ]; then
  log "Ejecutando migraciones de Base de Datos..."
  cd "$APP_DIR"
  Rscript db_init.R || warn "db_init.R devolvió error; se continúa con el arranque."
else
  warn "No se encontró ${APP_DIR}/db_init.R; se omite inicialización de base de datos."
fi

log "Iniciando Shiny Server..."

# Ejecutar Shiny Server en primer plano si está disponible esa opción
if /usr/bin/shiny-server --help 2>&1 | grep -q -- '--foreground'; then
  exec /usr/bin/shiny-server --foreground
else
  # Fallback para imágenes donde no existe --foreground
  /usr/bin/shiny-server &
  SHINY_PID=$!

  sleep 2

  if compgen -G "${SHINY_LOG_DIR}/*.log" > /dev/null; then
    tail -n +1 -qF "${SHINY_LOG_DIR}"/*.log &
    TAIL_PID=$!
  else
    warn "No se encontraron logs en ${SHINY_LOG_DIR}. Manteniendo proceso principal de Shiny Server."
    TAIL_PID=""
  fi

  wait "$SHINY_PID"
fi
