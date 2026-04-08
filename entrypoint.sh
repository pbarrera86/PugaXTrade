#!/bin/bash
set -e

# entrypoint.sh - PugaX Trade — Gestión de Variables de Entorno y Servidor

# Re-construimos .Renviron con las variables críticas (Dokploy / Env)
# Incluimos DB_URL, STRIPE, SMTP, y cualquier otra necesaria para la app
echo "Configurando .Renviron..."
env | grep -E '^(PG|STRIPE_|SMTP_|PUBLIC_|NEON_|DATABASE_URL|DATABASE_|PORT|SUPER_ADMIN|DEFAULT_REFERRER|REFERRAL_COMMISSION)' > /srv/shiny-server/.Renviron || true

# Aseguramos que el usuario 'shiny' tenga acceso
# En algunas bases de Docker /srv/shiny-server es el home de la app
chown shiny:shiny /srv/shiny-server/.Renviron
chmod 600 /srv/shiny-server/.Renviron

# libicu70 se instala en BUILD TIME (Dockerfile) — no es necesario descargarlo aquí.
# Si no existe, es porque la imagen no fue construida con el Dockerfile correcto.
if [ ! -f "/usr/lib/x86_64-linux-gnu/libicui18n.so.70" ]; then
    echo "ADVERTENCIA: libicui18n.so.70 no encontrada. Reconstruye la imagen Docker."
fi

# Exportamos la variable para que los workers devuelvan errores al log principal
export SHINY_LOG_STDERR=1

# Preparamos el directorio de logs
mkdir -p /var/log/shiny-server
chown shiny:shiny /var/log/shiny-server

echo "Ejecutando migraciones de Base de Datos..."
# Copiamos .Renviron temporalmente para que db_init.R lo use, luego lo eliminamos del dir público
cd /srv/shiny-server/app
if [ -f "/srv/shiny-server/.Renviron" ]; then
  cp /srv/shiny-server/.Renviron ./.Renviron
  Rscript db_init.R || true
  # Eliminar .Renviron del directorio de la app para no exponerlo via Shiny Server
  rm -f ./.Renviron
else
  Rscript db_init.R || true
fi

mkdir -p /srv/shiny-server/app_cache
chown shiny:shiny /srv/shiny-server/app_cache

# Iniciamos el servidor en background y mostramos los logs de workers
echo "Iniciando Shiny Server..."
/usr/bin/shiny-server &
sleep 2
tail -qF /var/log/shiny-server/*.log
wait
