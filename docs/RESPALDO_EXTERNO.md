# Copia mensual cifrada

El volcado se restaura primero en PostgreSQL aislado. Después se cifra con GnuPG
y una contraseña de recuperación, se verifica descifrándolo en el origen y se
envía por SSH con la identidad del destino fijada en `known_hosts`.

El receptor se instala en una carpeta privada fuera de `public_html`. La clave
de transferencia debe limitarse por IP, sin terminal ni reenvíos y con un
comando forzado que ejecute `hostinger_backup_receiver.py`. Solo admite subir
el archivo y consultar su tamaño y checksum. No concede navegación ni shell.

El archivo `latest.dump.gpg` anterior se conserva si la transferencia queda
incompleta o su checksum es incorrecto. Una copia íntegra se publica con una
operación de renombrado atómica. El origen comprueba nuevamente el archivo
almacenado antes de registrar éxito. Se conserva una copia programada en cada
destino; no se modifica la política de respaldos propios del proveedor.

La configuración y las claves se guardan privadamente en el servidor, con
permisos 0600 y directorios 0700. No se incluyen en Git. La contraseña de
recuperación debe conservarse también en el gestor de contraseñas del dueño:
sin ella no puede descifrarse la copia si se pierde el servidor original.

## Recuperación

1. Descargar `latest.dump.gpg` desde el administrador de archivos autenticado.
2. Descifrar con GnuPG o una aplicación compatible, introduciendo la contraseña
   de recuperación de forma interactiva:

       gpg --output restored.dump --decrypt latest.dump.gpg

3. Restaurar primero en PostgreSQL aislado con `pg_restore --exit-on-error`,
   comprobar las tablas y aplicar las migraciones con el rol del migrador.
4. Restablecer los permisos del rol de aplicación y reconciliar los pagos de
   Stripe posteriores a la fecha del respaldo antes de reabrir producción.

El respaldo mensual puede perder los cambios posteriores a su fecha. Guardar
una copia propia en el hosting no implica que el proveedor la incluya a su vez
en sus respaldos automáticos.
