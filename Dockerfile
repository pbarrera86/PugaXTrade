# ── PugaX Trade — Dockerfile ──────────────────────────────────────────────────
# Base: Rocker Shiny (Ubuntu 22.04 + R + shiny-server)
FROM rocker/shiny:latest

# ── System dependencies ────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    libpq-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    libsodium-dev \
    libgit2-dev \
    libxt-dev \
    pandoc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── R packages ─────────────────────────────────────────────────────────────────
# Agregamos 'promises' (CRÍTICO para ExtendedTask) y 'rlang'
RUN R -e "install.packages(c( \
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
    'rlang' \
  ), repos='https://cloud.r-project.org', dependencies=TRUE)"

# ── Shiny Server config ────────────────────────────────────────────────────────
# Intentamos copiar si existe localmente, si no, se usa el default del repo
COPY shiny-server.conf /etc/shiny-server/shiny-server.conf

# ── Copy app into shiny-server's default site folder ──────────────────────────
RUN rm -rf /srv/shiny-server/*
COPY . /srv/shiny-server/

# ── Permissions ───────────────────────────────────────────────────────────────
RUN chown -R shiny:shiny /srv/shiny-server \
    && chown root:root /etc/shiny-server/shiny-server.conf \
    && chmod 644 /etc/shiny-server/shiny-server.conf

# ── Entrypoint ────────────────────────────────────────────────────────────────
# Copiamos el entrypoint para asegurar variables de entorno y ejecución
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# ── Puerto Shiny Server ────────────────────────────────────────────────────────
EXPOSE 3838

CMD ["/usr/local/bin/entrypoint.sh"]
