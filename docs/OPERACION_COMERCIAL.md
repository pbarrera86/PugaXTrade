# Operación comercial

## Capacidad y tareas

La admisión de análisis se comparte en PostgreSQL entre procesos y módulos:
dos trabajos simultáneos en total y uno por usuario. Si no hay cupo se muestra
una advertencia; no se inicia otro proceso ni se mantiene una cola ilimitada.
Los procesos mirai tienen un máximo de 20 minutos; los cupos abandonados
caducan a los 21 minutos. Al terminar, cancelar o cerrar la sesión se libera el
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

Para completar la separación, ejecutar `scripts/runtime_role.sql` con el
propietario, provisionar en privado la credencial del rol y configurar PGUSER y
PGPASSWORD para ese rol. Conservar el propietario exclusivamente en las dos
variables del migrador. Verificar que el rol no herede membresías privilegiadas,
que PUBLIC no le conceda CREATE sobre el esquema y que no sea propietario de
tablas. La preparación del código no confirma que producción ya use ese rol.

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

Antes de declarar lista la comercialización, comprobar además el ciclo de cobro
fallido y recuperación en Stripe de pruebas, análisis y exportación con el rol
restringido, navegación móvil, derechos comerciales de los datos y revisión
legal del aviso de privacidad, condiciones y política de cancelación.
