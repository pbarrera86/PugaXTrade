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

# === FIX PARA VOLUMENES PERSISTENTES O LIBRERÍAS FALTANTES ===
# Si alguna librería compilada exige exactamente libicui18n.so.70 (versión de Ubuntu 22.04) 
# y estamos en Ubuntu 24.04, lo inyectamos de forma nativa antes de arrancar.
if [ ! -f "/usr/lib/x86_64-linux-gnu/libicui18n.so.70" ]; then
    echo "Instalando librería antigua libicui18n.so.70 para retro-compatibilidad..."
    curl -sO http://security.ubuntu.com/ubuntu/pool/main/i/icu/libicu70_70.1-2_amd64.deb
    dpkg -i libicu70_70.1-2_amd64.deb || true
    rm -f libicu70_70.1-2_amd64.deb
fi
echo "Verificando librerias finalizado."

# Exportamos la variable para que los workers devuelvan errores al log principal
export SHINY_LOG_STDERR=1

# Preparamos el directorio de logs
mkdir -p /var/log/shiny-server
chown shiny:shiny /var/log/shiny-server

echo "Ejecutando migraciones de Base de Datos..."
cd /srv/shiny-server/app && cp ../.Renviron .Renviron && Rscript db_init.R || true

# Iniciamos el servidor en background y mostramos los logs de workers
echo "Iniciando Shiny Server..."
/usr/bin/shiny-server &
sleep 2
tail -qF /var/log/shiny-server/*.log
wait
