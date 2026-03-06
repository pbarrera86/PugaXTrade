#!/bin/bash
# entrypoint.sh - Forward environment variables to Shiny Server

# Extract all environment variables and write them to the .Renviron file
# specifically for the 'shiny' user, so that R instances spawned by shiny-server
# can access them. We exclude standard bash variables.
env | grep -v -e "^HOME=" -e "^PWD=" -e "^USER=" -e "^_" -e "^SHLVL=" | while read -r line; do
  # Add quotes around the value to handle spaces properly
  key=$(echo "$line" | cut -d= -f1)
  value=$(echo "$line" | cut -d= -f2-)
  echo "${key}=\"${value}\"" >> /home/shiny/.Renviron
done

# Ensure the shiny user owns the file
chown shiny:shiny /home/shiny/.Renviron
chmod 600 /home/shiny/.Renviron

# Start the Shiny Server process
exec /usr/bin/shiny-server
