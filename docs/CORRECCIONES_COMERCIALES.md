# Correcciones comerciales preparadas

Estado: rama `fix/commercial-readiness` publicada y PR #17 en borrador. **Todavía no desplegado en producción.**
Base: `785781db8c2aacdd6ba009549da548d4de71db67`.

## Cambios por orden de complejidad

1. Experiencia: texto interno de Stripe retirado; botones de renovación admiten ajuste de línea; explicación de los métodos de valoración, coste del backtest y uso de caché.
2. Autorización: limpieza de cliente borra únicamente sus resultados; handlers administrativos validan permisos actuales; exportaciones y nuevos análisis vuelven a comprobar acceso; límite de 1500 símbolos por solicitud.
3. Operación: pool global se cierra al terminar la aplicación; migración fallida detiene el arranque; archivo de entorno global con permiso 0640; healthcheck comprueba DB y versión de migración en `/healthz`.
4. Motor: señal retrasada un periodo y coste supuesto de 10 puntos base por cambio de posición; drawdown incluye capital inicial. Se presenta como simulación RSI, no como backtest del ranking fundamental completo.
5. Cuenta: política de contraseña uniforme en DB, tokens de verificación con caducidad, aceptación versionada de documentos, referidos aleatorios y límites básicos de solicitudes de reset/registro. Se corrigió `shiny::hidden`, que no es el helper correcto para el formulario de reset.
6. Sesiones: token persistente solo en cookie Secure/HttpOnly; intercambio POST con comprobación de Origin y código de un solo uso de 60 segundos; hash de tokens en DB; lectura de cookie desde la solicitud del servidor; revocación ante cambio/reset de contraseña; revalidación cada 15 segundos y antes de operaciones; expiración tras 30 minutos sin actividad humana.
7. HTTP: login/logout/webhooks se interceptan antes del handler de UI de Shiny, que por defecto sirve GET. Se añaden no-store, strict-origin, nosniff, SAMEORIGIN y una política básica contra frames/objetos/base externos. No se presenta como una CSP completa contra XSS.
8. Facturación: HMAC con múltiples firmas v1 y ventana de 5 minutos; modo, precio, factura pagada, estado y periodo validados con consulta actual a Stripe; eventos, acceso y comisiones se confirman atómicamente; recuperación de pagos; protección frente a eventos de suscripciones antiguas; comisión única por usuario; idempotencia temporal de Checkout. Los eventos `invoice.paid`, `invoice.payment_failed`, `customer.subscription.updated/deleted`, `checkout.session.completed/async_payment_succeeded` deben estar habilitados en el endpoint.
9. Correo: outbox persistente y proceso dedicado con TLS, timeout, hasta 8 intentos y bloqueo SKIP LOCKED; las credenciales SMTP no se pasan como argumentos de proceso. Los mensajes enviados eliminan su cuerpo de la cola. Debe validarse entrega real y monitorizar intentos agotados.

## Validación local

48 comprobaciones de regresión completadas, incluidas:

- Parseo de todos los archivos R, sintaxis de shell y JavaScript; YAML de compose.
- Firmas válidas, alteradas, vencidas y rotadas; producto/periodo/estado de suscripción.
- Ausencia del rendimiento anterior a la señal; costes y drawdown inicial.
- Migración de tokens de sesiones/reset sin romper sus enlaces originales.
- Uso único de códigos de login y reset, incluso con dos procesos concurrentes.
- Cuenta desactivada rechazada; sesiones revocadas por cambio/reset.
- Reentrega de evento, recuperación de pago, comisión única y rollback para reintento.
- Evento de suscripción antigua sin revocación de la actual.

Las pruebas SQL utilizaron PostgreSQL 17 desechable, fixtures ficticios y el driver RPostgreSQL disponible localmente. CI repite las pruebas con RPostgres, que es el driver de producción, y añade un build Docker con arranque y healthcheck contra PostgreSQL desechable. CI aprobó el driver RPostgres, el build completo y el arranque de la imagen Docker (run 43112613822). Chromium comprobó que el formulario conserva Origin, rechaza un código de acceso inválido con 401 y vuelve al inicio tras logout. En staging se comprobó aceptación SMTP y vaciado del cuerpo enviado en la cola; Stripe test respondió a la consulta de precio y el webhook aceptó firma válida y rechazó firma inválida. Estas comprobaciones no equivalen a un pago Checkout completo ni a una aceptación funcional completa. La prueba ampliada detectó que Shiny Server OSS no entrega la cookie a la conexión SockJS y que un manejador JavaScript sin argumento interrumpía su registro. Se añadió recuperación HTTP vinculada a cada sesión, con Origin verificado y sin devolver tokens, y se corrigió el manejador. El 12 de noviembre se desplegó en staging y se observó la cuenta de prueba del operador autenticada al recargar por HTTPS. El CI completo de contraseña, cookie, recarga y logout aprobó junto con la regresión R/SQL en el run 43250844349.

## Validación funcional adicional

- Respaldo lógico y restauración aislada comprobados. La comparación de tablas no detectó cambios en la cantidad de filas.
- Ensayo de migración sobre la copia restaurada completado, conservando los registros y convirtiendo las sesiones al formato protegido.
- Checkout en modo test completado: membresía activa. Cancelar renovación conserva el acceso del periodo pagado; reactivarla vuelve a habilitar la renovación.
- La reentrega firmada de un evento de prueba ya procesado respondió correctamente sin duplicar efectos.
- Revisar la configuración de correo antes del despliegue.
- El puerto directo del host se limita a loopback; el proxy conserva el acceso por la red de servicio.
- Las evidencias operativas detalladas se conservan fuera del repositorio. La comprobación puntual no sustituye una política periódica de respaldo, retención y recuperación.

## Despliegue y reversión

1. Publicar la rama y abrir PR; no mezclar directamente en `main`, que autodespliega.
2. Ejecutar CI, build Docker y revisión del diff. Configurar staging con DB independiente y claves Stripe test.
3. Respaldar Neon por separado del VPS y probar restauración aislada.
4. En staging, ejecutar `Rscript db_init.R`. El archivo `migrations/001_commercial_security.sql` se registra en `schema_migrations` y se aplica dentro de una transacción con lock. Mantener habilitada la extensión pgcrypto.
5. La migración convierte tokens existentes en hashes y crea tablas de códigos de acceso, eventos, comisiones y correo. La versión anterior de la app no entiende esos hashes: coordinar parada y reemplazo, evitando versiones mixtas. Verificar datos históricos antes de migrar.
6. Probar registro, verificación, reenvío, login, reinicio de navegador, reset, logout, cuenta vencida/desactivada, análisis, Excel, pago, fallo/recuperación, cancelación y usuario común contra administración.
7. Confirmar que `email-worker` está funcionando y que el endpoint de Stripe recibe los eventos enumerados. Probar firma rechazada y duplicados en modo test.
8. Tras aprobar staging, desplegar de forma coordinada y comprobar `/healthz`, cookies, WebSocket, correo y facturación.
9. Para revertir la aplicación antigua: detener las dos versiones y el worker, conservar evidencia y tablas nuevas, invalidar las sesiones y resets hash, y volver a iniciar sesión. Si se restaura un backup completo, reconciliar posteriormente pagos y comisiones que Stripe haya recibido después del backup. Los hashes no pueden convertirse de vuelta a tokens originales. No hacer DROP de tablas nuevas para una reversión de código.

## Pendientes antes de aprobar comercialización

- Rama publicada y PR #17 creado; driver de producción y build aprobados. Respaldo, restauración, ensayo de migración y Checkout aprobado comprobados. Pendientes revisión final de configuración y aceptación funcional restante antes del despliegue de producción.
- Rol de aplicación con mínimos privilegios, separado del propietario/migrador. No se cambió el rol real ni se generaron credenciales.
- Backups/PITR y retención de Neon confirmados, con restauración ensayada; destino S3 de Dokploy si se usará ese mecanismo.
- Validación de entrega del reenvío de verificación, limitación por IP y observabilidad de intentos/colas. Los límites por identificador no reemplazan protección global contra abuso.
- Cola global de trabajos, cuotas por plan, cancelación/timeout y pruebas de carga. El límite de 1500 símbolos no demuestra capacidad multiusuario.
- Reconciliación periódica de suscripciones ante webhooks perdidos y revisión de la política de reembolsos/disputas.
- Fijación reproducible de todas las dependencias y revisión de vulnerabilidades. No se cambiaron versiones sin construir y probar la imagen completa.
- Validación de la pantalla de renovación condicionada al estado, precio obtenido de Stripe, traducción de tablas, pruebas reales de móvil/accesibilidad y validación independiente de datos/modelos.
- Configuración/entregabilidad SMTP, alertas de cola fallida, retención/purga de emails y auditoría administrativa persistente.
- Documentos actualizados describen prueba sin cobro automático y proveedores actuales; todavía requieren revisión comercial/jurídica y de derechos de uso de datos.

No se ha declarado el sitio listo para venta. El código preparado cierra fallos importantes, pero no sustituye la validación del despliegue ni los pendientes anteriores.
