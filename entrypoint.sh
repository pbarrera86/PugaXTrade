#!/bin/bash
set -e

# entrypoint.sh - PugaX Trade — Gestión de Variables de Entorno y Servidor

# Re-construimos .Renviron con las variables críticas (Dokploy / Env)
# Incluimos DB_URL, STRIPE, SMTP, y cualquier otra necesaria para la app
echo "Configurando .Renviron..."
env | grep -E '^(PG|STRIPE_|SMTP_|PUBLIC_|NEON_|DATABASE_URL|DATABASE_|PORT)' > /srv/shiny-server/.Renviron || true

# Aseguramos que el usuario 'shiny' tenga acceso
# En algunas bases de Docker /srv/shiny-server es el home de la app
chown shiny:shiny /srv/shiny-server/.Renviron
chmod 600 /srv/shiny-server/.Renviron

# === FIX PARA VOLUMENES PERSISTENTES ===
# Si Dokploy tiene un volumen montado, los paquetes pre-compilados en el build son ocultados.
# Verificamos si stringi funciona. Si falla, lo reinstalamos en tiempo de ejecución.
echo "Verificando dependencias críticas en tiempo de ejecución..."
R -e "tryCatch(library(stringi), error = function(e) { message('stringi roto, reinstalando...'); install.packages('stringi', repos='https://packagemanager.posit.co/cran/__linux__/noble/latest') })"

# Exportamos la variable para que los workers devuelvan errores al log principal
export SHINY_LOG_STDERR=1

# Preparamos el directorio de logs
mkdir -p /var/log/shiny-server
chown shiny:shiny /var/log/shiny-server

# Iniciamos el servidor en background y mostramos los logs de workers
echo "Iniciando Shiny Server..."
/usr/bin/shiny-server &
sleep 2
tail -qF /var/log/shiny-server/*.log
wait
