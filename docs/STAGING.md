# Staging aislado

Usar `compose.staging.yml` por separado, sin combinarlo con el compose de producción.

1. Crear `.env.staging` a partir de `.env.staging.example`. Definir contraseña exclusiva de base de datos y URL HTTPS de staging. Introducir las credenciales desde la configuración privada del servidor, nunca en Git ni en comentarios del PR.
2. Usar exclusivamente claves y precio Stripe test/sandbox. El arranque rechaza claves live. Configurar un buzón SMTP de pruebas con TLS y destinatarios ficticios bajo control del operador.
3. Ejecutar `docker compose --env-file .env.staging -f compose.staging.yml config --quiet`, y después `docker compose --env-file .env.staging -f compose.staging.yml up -d --build`.
4. La aplicación escucha únicamente en loopback del servidor, puerto 3839. La base de datos no publica puerto y reside en una red interna. Configurar el proxy HTTPS y restringir el acceso de staging antes de habilitar entrada externa. No apuntar el dominio de producción a este servicio.
5. Comprobar `curl -f http://127.0.0.1:3839/healthz` y el estado de los tres servicios. La base empieza vacía: no importar clientes ni copiar la base Neon.

## Criterios de aceptación

- Registro, verificación y reenvío; entrega real al buzón de prueba y cola sin mensajes agotados.
- Login, recarga/reinicio de navegador y logout. Cookie Secure/HttpOnly/SameSite y ausencia de token en HTML/URL.
- Recuperación y cambio de contraseña revocan sesiones previas. Cuenta desactivada o vencida no puede analizar/exportar.
- Dos navegadores con usuarios distintos conservan datos y permisos separados; usuario común no ejecuta acciones administrativas.
- Análisis pequeño y Excel; limpieza de resultados no borra los de otro usuario.
- Checkout test: pago aprobado, pago fallido/recuperado, cancelación, firma inválida y reentrega del mismo evento. Verificar membresía y una sola comisión.
- Habilitar en el webhook test los eventos descritos en CORRECCIONES_COMERCIALES.md. Ninguna de estas comprobaciones autoriza cargos reales.

Conservar evidencia de cada criterio. Un build verde no equivale a una aceptación funcional completa. Después de validar staging, ensayar backup/restauración de Neon y coordinar el reemplazo de producción: la app antigua no entiende los tokens migrados a hash.
