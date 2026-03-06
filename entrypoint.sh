#!/bin/bash
# entrypoint.sh - Forward environment variables to Shiny Server safely

# Extract relevant application environment variables and write them to .Renviron
# We filter by specific prefixes to avoid internal Railway/OS variables that
# might contain multiline values or invalid identifiers that crash readRenviron()
env | grep -E '^(PG|STRIPE_|SMTP_|PUBLIC_|NEON_)' > /home/shiny/.Renviron

# Ensure the shiny user owns the file
chown shiny:shiny /home/shiny/.Renviron
chmod 600 /home/shiny/.Renviron

# Start the Shiny Server process
exec /usr/bin/shiny-server
