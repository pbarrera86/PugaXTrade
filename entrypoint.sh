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

# Iniciamos el proceso del Shiny Server
echo "Iniciando Shiny Server..."
exec /usr/bin/shiny-server
