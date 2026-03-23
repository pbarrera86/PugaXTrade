#!/bin/bash
set -e

# Rebuild .Renviron with only app-related variables
env | grep -E '^(PG|STRIPE_|SMTP_|PUBLIC_|NEON_)' > /home/shiny/.Renviron || true

chown shiny:shiny /home/shiny/.Renviron
chmod 600 /home/shiny/.Renviron

exec /usr/bin/shiny-server
