# ── PugaX Trade — Dockerfile ──────────────────────────────────────────────────
# Base: Rocker Shiny (Ubuntu 22.04 + R 4.4 + shiny-server)
# Versión pinada para builds reproducibles
FROM rocker/shiny:4.4.2

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
# Usamos el repositorio de binarios de Posit (PPM) para Ubuntu Noble (24.04)
# Esto acelera el build al evitar compilar desde código fuente
RUN R -e "options(repos = c(CRAN = 'https://packagemanager.posit.co/cran/__linux__/noble/latest')); \
    install.packages(c( \
    'shiny', \
    'shinyWidgets', \
    'shinyjs', \
    'DT', \
    'bslib', \
    'thematic', \
    'dplyr', \
    'tidyr', \
    'tibble', \
    'stringr', \
    'lubridate', \
    'purrr', \
    'furrr', \
    'future', \
    'promises', \
    'DBI', \
    'RPostgres', \
    'pool', \
    'sodium', \
    'jsonlite', \
    'httr', \
    'yaml', \
    'emayili', \
    'rvest', \
    'xml2', \
    'quantmod', \
    'TTR', \
    'openxlsx', \
    'markdown', \
    'plotly', \
    'htmltools', \
    'rlang', \
    'openssl' \
  ), dependencies=TRUE)"

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
