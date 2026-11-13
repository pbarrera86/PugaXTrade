# Operación comercial

## Capacidad y tareas

La admisión de análisis se comparte en PostgreSQL entre procesos y módulos:
dos trabajos simultáneos en total y uno por usuario. Si no hay cupo se muestra
una advertencia; no se inicia otro proceso ni se mantiene una cola ilimitada.
Los procesos mirai tienen un máximo de 240 minutos (4 horas); los cupos abandonados
caducan a los 241 minutos. Al terminar, cancelar o cerrar la sesión se libera el
cupo. Los datos permanecen sujetos a la vigencia de la caché.

La prueba aislada de ocho clientes concurrentes verifica la admisión, el rechazo
por usuario, la liberación y la recuperación de cupos vencidos. No representa
una medición de capacidad para cientos de usuarios ni valida la velocidad del
proveedor de datos. La carga real y el tamaño de cada lista deben medirse antes
de aumentar los límites.

## Separación de permisos

El servicio `migrate` ejecuta el esquema y las migraciones antes del inicio de
la aplicación. Recibe `PG_MIGRATOR_USER` y `PG_MIGRATOR_PASSWORD`; por compatibilidad
puede usar las variables PG existentes mientras se provisiona el rol restringido.
La aplicación y el worker solo reciben sus propias variables PG. La aplicación
verifica las versiones de migración sin ejecutar DDL.

Producción ya utiliza el rol restringido; se comprobaron ausencia de membresías
privilegiadas, de CREATE en el esquema y de propiedad de tablas. El propietario
se conserva en el servicio de migración. Las instalaciones nuevas deben aplicar
`scripts/runtime_role.sql`, provisionar la contraseña en privado y verificar
permisos antes de habilitar tráfico. No imprimir ni publicar las credenciales.

## Alertas

El panel de Usuarios, disponible solo para administradores, muestra correos que
agotaron reintentos, envíos demorados y comprobaciones operativas fallidas o sin
actualizar. La consulta se refresca cada minuto. El monitor del servidor debe
actualizar `operational_checks` cada cinco minutos con resultados de salud,
worker, disco y respaldo. Sin ese monitor, el panel advierte que faltan datos.
El worker de correo publica un latido local comprobado por Docker. Estas alertas
internas no equivalen a un servicio externo de avisos por correo o SMS.

## Respaldos y aceptación

La política solicitada es mensual y conserva una copia programada: primero se
genera la nueva, se comprueba su restauración aislada y solo después se retira
la anterior. Un fallo no debe reemplazar la última copia válida. La programación
y las evidencias privadas se administran fuera del repositorio. Un respaldo
mensual admite perder los cambios posteriores a su fecha; la copia alojada en
el mismo servidor no cubre la pérdida completa de ese servidor.

La copia externa cifrada ya está activa en el alojamiento del dominio, fuera de
public_html. El receptor restringido sustituye únicamente la copia actual tras
comprobar tamaño y hash. No permite shell ni navegación. Se comprobó el rechazo
de una carga corrupta sin alterar la copia válida. Véase
[RESPALDO_EXTERNO.md](RESPALDO_EXTERNO.md) para la recuperación. Conservar la clave
de recuperación fuera del VPS; perderla impide descifrar la copia.

El ciclo de cobro fallido y recuperación de Stripe en modo test, los análisis y
Excel en producción con rol restringido y una comprobación móvil básica ya
aprobaron. La lista vigente de pendientes y sus criterios de cierre está en
[CORRECCIONES_COMERCIALES.md](CORRECCIONES_COMERCIALES.md).
