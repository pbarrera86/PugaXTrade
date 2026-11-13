# Inactividad de sesiones

La sesión abierta expira por inactividad después de **más de 30 minutos**, con
comprobación cada 15 segundos. Clic, movimiento del puntero, teclado, toque,
foco y scroll (incluido el de tablas interiores) renuevan la actividad. El
heartbeat técnico no cuenta como interacción humana.

Un trabajo admitido de análisis institucional o extremos mantiene activa la
sesión aunque el usuario espere sin tocar la página. Al terminar, fallar o
cancelarse empieza un nuevo plazo de 30 minutos. Un trabajo bloqueado no puede
mantenerla indefinidamente: el reloj respeta el límite de su lease de 61 minutos,
posterior al timeout de cálculo de 60 minutos. El análisis tiene un máximo de una hora; no cambia la cantidad de trabajos
simultáneos.

Se conserva el aviso a los 25 minutos, con botón para continuar, y el cierre
manual de sesión. La revocación real, desactivación de cuenta y caducidad del
token continúan siendo controles independientes de seguridad. Un error temporal
al consultar la base de datos no borra la cookie ni se considera una revocación:
se reintenta en la siguiente comprobación. Las operaciones protegidas siguen
requiriendo autorización actual y fallan de forma cerrada si no se puede validar.

`R/session_activity.R` mantiene el reloj en el servidor y recibe explícitamente
los inicios/finales de trabajo de ambos módulos. `tests/session_activity.R`
comprueba los límites con un reloj controlado, incluyendo fin/cancelación,
trabajo abandonado, fallo de DB y revocación. No requiere esperar 30 minutos
reales ni introduce atajos de autenticación en producción.
