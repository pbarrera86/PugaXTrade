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
# PASO 1: Actualizar xfun PRIMERO en su propia capa.
# Rocker shiny-verse trae una versión vieja de xfun que rompía emayili.
# Al actualizarlo primero, garantizamos la versión correcta antes de que emayili lo use.
RUN R -e "options(repos = c(CRAN = 'https://packagemanager.posit.co/cran/__linux__/noble/latest')); \
    install.packages(c('xfun', 'knitr', 'rmarkdown'))"

# PASO 2: Ahora sí instalar el resto de paquetes con xfun ya sano.
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

# ── Copiar el código de la app dentro de la imagen ────────────────────────────
# La app vive en /srv/shiny-server/app — así Shiny Server la sirve como app directa
# y no se mezcla con los sample-apps de Rocker ni con archivos de configuración.
RUN mkdir -p /srv/shiny-server/app
COPY --chown=shiny:shiny . /srv/shiny-server/app

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
