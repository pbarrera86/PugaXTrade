# Reconciliación periódica de Stripe

El servicio `billing-worker` revisa las suscripciones al arrancar y espera cinco
minutos entre revisiones. No sustituye al webhook: recupera divergencias cuando
se pierde una entrega. Solo realiza GET a Stripe; no cobra, reembolsa, cambia
planes, cancela ni reactiva suscripciones en Stripe.

## Alcance y seguridad

- Comprueba todos los IDs de suscripción vinculados en PostgreSQL. Además recorre
  la lista paginada de Stripe con `status=all` para recuperar compras todavía no
  vinculadas que incluyan un `metadata.user_id` existente en la aplicación.
- No vincula por coincidencia de email ni modifica suscripciones sin vínculo o
  metadatos del usuario. Una compra antigua sin ambos requiere revisión manual.
- Para cada suscripción consulta de nuevo el estado actual bajo el mismo bloqueo
  transaccional que los webhooks. La respuesta paginada sirve para descubrir IDs,
  no para conceder acceso. Rechaza conflictos de identidad o de modo live/test.
- Solo concede acceso del plan configurado cuando la suscripción está activa,
  la última factura está pagada y el periodo no ha terminado. Un cambio a otro
  precio en una suscripción vinculada retira ese acceso de Stripe. Conserva la
  vigencia de las activaciones manuales.
- Recupera la comisión del primer pago válido si faltaba; el UNIQUE por usuario
  y la transacción impiden acreditarla otra vez. No crea eventos ficticios en
  `stripe_events` ni cambia la deduplicación de los webhooks reales.
- Un fallo de red o respuesta inválida no se interpreta como cancelación ni
  pago: conserva el estado anterior de esa suscripción y registra el error.
- Un bloqueo global de PostgreSQL impide dos barridos simultáneos. Cada barrido
  admite hasta 100 páginas y un presupuesto de 240 segundos, comprobado entre
  peticiones; cada GET tiene timeout de 15 segundos. Una flota que supere ese
  presupuesto requiere revisar la estrategia antes de aumentar límites.

## Operación

Usa la imagen de producción y el rol restringido existente. No requiere nuevas
credenciales ni migraciones. Se comprueba el latido del proceso en Docker; el
resultado del último barrido se guarda en `operational_checks` con el nombre
`stripe_reconciliation`. El panel administrativo muestra fallos y datos sin
actualizar, sin exponer identificadores ni respuestas de Stripe.

Si falla, revisar conectividad, precio configurado, modo y metadatos en los
paneles privados. Resolver primero el origen de la divergencia y permitir el
siguiente barrido. No editar membresías o billeteras masivamente para ocultar
el aviso. Conservar los webhooks configurados y sus reintentos.

La recuperación sincroniza el estado actual; no reconstruye todo el historial
de facturas, disputas o reembolsos. Su tratamiento comercial permanece pendiente.
La revisión completa de pérdida del VPS sigue requiriendo restaurar y contrastar
los cobros posteriores al respaldo.

## Evidencia

Pruebas con PostgreSQL desechable y respuestas Stripe controladas cubren pérdida
del primer webhook, recuperación/fallo de pago, paginación, precio diferente,
factura impagada, modo incorrecto, conflicto de usuario, permisos manuales,
fallo de red, comisión única y exclusión de dos procesos concurrentes. CI debe
repetir la suite con RPostgres y construir/arrancar la imagen antes de desplegar.

Referencias oficiales:
- https://docs.stripe.com/api/subscriptions/list
- https://docs.stripe.com/events/manage-webhook-endpoints
