# Recuperación del Dashboard Analítico

El Dashboard envía el trabajo a `analysis-worker`, un servicio independiente de Shiny. La desconexión, recarga o cierre de una pestaña solo separa al visor: no cancela el trabajo ni libera su reserva.

La solicitud, el progreso, los resultados por ticker y el resultado final se escriben mediante reemplazos atómicos en el volumen privado `analysis_work`, fuera del directorio web. Cada usuario recupera exclusivamente su último trabajo a partir del identificador autenticado. Al volver a entrar, la interfaz restaura parámetros, avance y resultados automáticamente.

Después de reiniciar el procesador, el coordinador retoma trabajos vigentes desde el último ticker confirmado. Puede repetirse el ticker que estaba a medio procesar, pero no los ya guardados. La consolidación final se repite si quedó interrumpida. El coordinador debe tener **una sola réplica**; mantiene hasta dos procesos y respeta las reservas PostgreSQL compartidas con Extremos Locales (un trabajo por usuario y dos globales).

Cada intento conserva el límite de seis horas, incluso tras reconectar o reiniciar. Al agotarlo o fallar el proceso, el botón **Retomar análisis guardado** inicia otro intento sobre los mismos parámetros y checkpoints; no vuelve a descargar tickers completados. Cancelar es una acción explícita y no se reanuda automáticamente.

Se conserva el último análisis por usuario hasta siete días después de terminar o interrumpirse. Un análisis nuevo sustituye al anterior; **Eliminar análisis guardado** elimina solicitud, checkpoints y resultado. Los trabajos activos no se purgan. No se guardan contraseñas ni tokens de sesión en el volumen.

El volumen protege frente a desconexiones y recreación de contenedores en el mismo VPS. No protege contra pérdida total del disco/VPS; los respaldos de base de datos no incluyen este volumen de trabajo. Las sesiones mantienen la política de 30 minutos de inactividad fuera de análisis y los controles de acceso vigentes.

Validación: fixtures sin red interrumpen el procesamiento tras un ticker, reconstruyen el visor/procesador y verifican que solo se ejecutan los tickers sin confirmar. CI también termina un proceso mirai real y recupera el trabajo en uno nuevo. Cancelación, plazo, archivos corruptos e aislamiento de propietarios tienen pruebas específicas. Esto no sustituye una prueba real de 7,300 tickers ni de seis horas.
