# ── PugaX Trade — Dockerfile ──────────────────────────────────────────────────
# Base: Rocker Shiny-Verse (Ubuntu 24.04 + R 4.4 + shiny-server + Tidyverse presintalado)
# Esto soluciona de raíz cualquier conflicto con dependencias frágiles como stringi/ICU
FROM rocker/shiny-verse:4.4.2

# ── System dependencies ────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    libsodium-dev \
    libxt-dev \
    pandoc \
    cmake \
    libmbedtls-dev \
    libudunits2-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libcairo2-dev \
    libfontconfig1-dev \
    libicu-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── R packages ─────────────────────────────────────────────────────────────────
# Descargamos del PPM de Noble sólo los paquetes extra que no vienen en shiny-verse.
# Al quitar 'dependencies=TRUE' y dejar los paquetes base en paz, blindamos el sistema.
RUN R -e "options(repos = c(CRAN = 'https://packagemanager.posit.co/cran/__linux__/noble/latest')); \
    install.packages(c( \
    'shinyWidgets', \
    'shinyjs', \
    'DT', \
    'thematic', \
    'furrr', \
    'future', \
    'promises', \
    'DBI', \
    'RPostgres', \
    'pool', \
    'sodium', \
    'yaml', \
    'emayili', \
    'quantmod', \
    'TTR', \
    'openxlsx', \
    'markdown', \
    'plotly' \
  ))"

# ── Shiny Server config ────────────────────────────────────────────────────────
COPY shiny-server.conf /etc/shiny-server/shiny-server.conf

# ── Preparar directorio de la app (el código se monta desde Dokploy) ──────────
RUN mkdir -p /srv/shiny-server \
    && chown -R shiny:shiny /srv/shiny-server

# ── Permisos de configuración ─────────────────────────────────────────────────
RUN chown root:root /etc/shiny-server/shiny-server.conf \
    && chmod 644 /etc/shiny-server/shiny-server.conf

# ── Entrypoint ────────────────────────────────────────────────────────────────
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# ── Puerto ────────────────────────────────────────────────────────────────────
EXPOSE 3838

# ── Health check — Dokploy/Railway sabrán si la app está viva ─────────────────
HEALTHCHECK --interval=30s --timeout=15s --start-period=120s --retries=3 \
  CMD curl -sf http://localhost:3838/ > /dev/null || exit 1

CMD ["/usr/local/bin/entrypoint.sh"]
