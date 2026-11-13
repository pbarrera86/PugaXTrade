# Estado de preparación comercial

Actualizado: 12 de noviembre de 2026. Las correcciones de los PR #17–#20 están
fusionadas. La aceptación operativa descrita abajo corresponde a producción;
los cobros se comprobaron en Stripe de pruebas, sin cargos reales.

## Completado y comprobado

- Autenticación: cookie Secure/HttpOnly, sesiones y enlaces almacenados como
  hashes, intercambio de un solo uso, comprobación de Origin, recuperación
  de sesión por HTTP para Shiny Server OSS y revocación al cambiar contraseña.
- Autorización: comprobación actual de cuenta y permisos antes de análisis,
  exportaciones y acciones administrativas; inactividad máxima de 30 minutos.
- PostgreSQL: migraciones transaccionales y versionadas; propietario separado
  del rol restringido de la aplicación y del worker. Producción usa ese rol;
  se verificó que no puede modificar el esquema ni las migraciones.
- Facturación: firma Stripe, validación del estado actual, producto y periodo;
  eventos y comisiones idempotentes dentro de transacción. Checkout, cancelar
  y reactivar renovación, fallo de renovación y recuperación aprobados en modo
  test. Webhook de producción configurado con los eventos requeridos.
- Correo: cola durable, TLS, reintentos y purga del cuerpo enviado. Aceptación
  SMTP validada; heartbeat del worker y alertas administrativas activos.
- Capacidad: dos análisis simultáneos globales, uno por usuario, timeout de
  60 minutos y recuperación de cupos vencidos. Ocho clientes SQL concurrentes
  admitieron exactamente dos trabajos; dos análisis aislados y sus Excel
  terminaron correctamente y una tercera solicitud fue rechazada.
- Producción: análisis AAPL y AMXB.MX y sus descargas Excel comprobados con el
  rol restringido. El panel administrativo no mostró incidencias al comprobarlo.
- Monitor: salud, worker, HTTPS, disco, respaldo y copia externa cada cinco
  minutos; los seis controles aprobaron la última comprobación.
- Respaldo: mensual, conserva una copia programada; restauración aislada antes
  de sustituirla. Copia cifrada en el alojamiento Hostinger del dominio,
  fuera de public_html, mediante una clave SSH que solo permite subir y
  consultar estado. Hash independiente coincidente; carga corrupta rechazada
  conservando la copia anterior; URL pública devuelve 404. Recuperación en
  [RESPALDO_EXTERNO.md](RESPALDO_EXTERNO.md).
- CI: regresión R/SQL con el driver de producción, build y arranque Docker,
  autenticación completa en Chromium, viewport móvil de 390 px y pruebas de
  cifrado/receptor aprobados para el PR #20.
- Motor: la señal del backtest RSI se retrasa un periodo y descuenta un coste
  supuesto de 10 puntos base por cambio de posición; incluye capital inicial
  en drawdown. No representa un backtest del ranking fundamental completo.

## Pendientes, por prioridad

| Prioridad | Pendiente | Criterio de cierre |
| --- | --- | --- |
| Alta | Derechos comerciales de Yahoo/Finviz | Autorización escrita para extracción, visualización, caché, derivados y Excel; o proveedor sustituto con contrato adecuado. Ver [DATOS_COMERCIALES.md](DATOS_COMERCIALES.md). |
| Alta | Reconciliación periódica Stripe | Detectar y corregir divergencias ante webhooks perdidos con consulta autoritativa e idempotencia, sin conceder acceso a facturas impagadas. |
| Alta | Recuperación completa por pérdida del VPS | Restaurar en un entorno nuevo usando copia externa y clave de recuperación conservada por el titular; reconciliar pagos posteriores. La restauración aislada del dump ya se comprobó. |
| Media | Dependencias y vulnerabilidades | Fijar imagen y dependencias reproducibles y revisar hallazgos del sistema operativo y R. Retirar la descarga HTTP de ICU es una corrección puntual, no cierra este punto. |
| Media | Protección global frente a abuso | Límites por IP/origen en el proxy y pruebas de registro, login y reset; los límites por identificador existentes no bastan. |
| Media | Privacidad y condiciones comerciales | Confirmar responsable y contacto, conservación/purga, cancelaciones, reembolsos y disputas; revisión de textos según el negocio real. |
| Media | Validación de datos y carga representativa | Comparar métricas y ajustes corporativos con fuente independiente; medir listas grandes y concurrencia real antes de aumentar cupos. |
| Media | Accesibilidad y experiencia completas | Recorrido con teclado/lector de pantalla y varios tamaños; la prueba de 390 px no acredita WCAG. Revisar estados de renovación y traducciones de tablas. |
| Media | Auditoría y avisos externos | Registro persistente de acciones administrativas, políticas de retención y alertas fuera de la propia aplicación. |

## Límites de la evidencia

No se declara el sitio completamente listo para comercialización. La prueba
puntual de dos análisis no demuestra capacidad para cientos de clientes. El
respaldo mensual puede perder cambios posteriores a su fecha. La copia externa
cubre la pérdida del VPS, pero ambos alojamientos pertenecen al mismo proveedor.
Las pruebas de Stripe no incluyen un cargo de producción. Los textos legales
no conceden derechos de uso sobre los datos.

## Despliegue y reversión

Usar PR y CI antes de fusionar; main autodespliega. Mantener migraciones en el
servicio migrate y credenciales del propietario fuera de app/worker. Comprobar
salud, análisis, Excel y alertas tras desplegar. Una versión anterior a los hashes
de sesiones no es compatible: no revertir ciegamente a ella ni borrar tablas
nuevas. Al restaurar una copia completa, reconciliar cobros y comisiones
posteriores a la fecha del respaldo.
