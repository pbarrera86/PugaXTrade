#!/bin/bash
set -e

# entrypoint.sh - Forward environment variables to Shiny Server safely

# Rebuild .Renviron with only app-related variables
env | grep -E '^(PG|STRIPE_|SMTP_|PUBLIC_|NEON_)' > /home/shiny/.Renviron || true

# Ensure the shiny user owns the file
chown shiny:shiny /home/shiny/.Renviron
chmod 600 /home/shiny/.Renviron

# Start the Shiny Server process
exec /usr/bin/shiny-server
