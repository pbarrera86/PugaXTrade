#!/bin/bash
set -e

# entrypoint.sh - PugaX Trade

# Filtro de variables críticas de entorno
ENV_FILTER='^(PG|STRIPE_|SMTP_|PUBLIC_|NEON_|DATABASE_URL|DATABASE_|PORT|SUPER_ADMIN|DEFAULT_REFERRER|REFERRAL_COMMISSION)'

echo "Configurando variables de entorno..."

# 1. /etc/R/Renviron.site — cargado automáticamente por TODOS los procesos R,
#    incluyendo los hijos de Shiny Server. Este es el método correcto para Docker.
env | grep -E "$ENV_FILTER" > /etc/R/Renviron.site || true
chmod 644 /etc/R/Renviron.site

# 2. Copia de seguridad en /srv/shiny-server/.Renviron (app.R lo lee con readRenviron)
env | grep -E "$ENV_FILTER" > /srv/shiny-server/.Renviron || true
chown shiny:shiny /srv/shiny-server/.Renviron
chmod 600 /srv/shiny-server/.Renviron

# 3. Copiar también al directorio de la app y DEJARLO AHÍ — app.R lo necesita
#    (getwd() = /srv/shiny-server/app al arrancar Shiny)
cp /srv/shiny-server/.Renviron /srv/shiny-server/app/.Renviron || true
chown shiny:shiny /srv/shiny-server/app/.Renviron 2>/dev/null || true
chmod 600 /srv/shiny-server/app/.Renviron 2>/dev/null || true

# libicu70 se instala en BUILD TIME (Dockerfile) — no es necesario descargarlo aquí.
if [ ! -f "/usr/lib/x86_64-linux-gnu/libicui18n.so.70" ]; then
    echo "ADVERTENCIA: libicui18n.so.70 no encontrada. Reconstruye la imagen Docker."
fi

# Exportar para que los workers devuelvan errores al log principal
export SHINY_LOG_STDERR=1

# Preparar directorio de logs
mkdir -p /var/log/shiny-server
chown shiny:shiny /var/log/shiny-server

# Preparar directorio de caché de la app (evita el error de permisos de bslib/sass)
mkdir -p /srv/shiny-server/app/app_cache
chown -R shiny:shiny /srv/shiny-server/app/app_cache 2>/dev/null || true

echo "Ejecutando migraciones de Base de Datos..."
cd /srv/shiny-server/app && Rscript db_init.R || true

echo "Iniciando Shiny Server..."
/usr/bin/shiny-server &
sleep 2
tail -qF /var/log/shiny-server/*.log
wait
