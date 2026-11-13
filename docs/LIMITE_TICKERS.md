# Límite de tickers

Dashboard Analítico y Extremos Locales admiten hasta 7,300 tickers por solicitud,
después del parseo y eliminación de duplicados. Ambos usan `puga_max_tickers`
y `puga_ticker_count_allowed` en `R/job_capacity.R`; una lista superior se rechaza
antes de adquirir un cupo o iniciar un proceso.

La ampliación del límite de entrada no cambia la concurrencia global (dos
trabajos, uno por usuario), la memoria asignada ni el timeout de 240 minutos (4 horas).
Los proveedores de datos y la caché determinan la duración real; una lista grande
sin caché puede superar ese tiempo. No se acredita aquí una prueba de carga
completa de 7,300 cotizaciones contra servicios externos.

La regresión comprueba aceptación de 7,300 y rechazo de 7,301. La prueba del
navegador comprueba la notificación y que una lista superior al límite no deje
los controles ocupados en ninguno de los dos módulos.
