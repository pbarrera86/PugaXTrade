FROM rocker/shiny:latest

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    libpq-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    libsodium-dev \
    libgit2-dev \
    pandoc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

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
    'markdown' \
  ), repos='https://cloud.r-project.org', dependencies=TRUE)"

COPY shiny-server.conf /etc/shiny-server/shiny-server.conf

RUN rm -rf /srv/shiny-server/*
COPY . /srv/shiny-server/

RUN chown -R shiny:shiny /srv/shiny-server \
    && chown root:root /etc/shiny-server/shiny-server.conf \
    && chmod 644 /etc/shiny-server/shiny-server.conf

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

EXPOSE 3838

CMD ["/usr/local/bin/entrypoint.sh"]
