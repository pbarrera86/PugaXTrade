# Exportación del Dashboard para miles de tickers

El resultado original sigue en el volumen privado de análisis. Exportar no consulta proveedores, no reinicia el cálculo y no modifica ese resultado.

**Exportar Excel** solicita la preparación en segundo plano. Mientras tanto se pueden consultar, filtrar y paginar las tablas. Al terminar aparece **Descargar Excel**, que entrega el archivo ya preparado. La solicitud y el archivo sobreviven a una recarga o recreación del contenedor. Un error de exportación permite reintentar y conserva el análisis.

El archivo tiene cinco hojas: **Resumen**, **Ranking**, **Detalles**, **Backtesting** y **Glosario**. Detalles reúne los indicadores de todas las empresas con una columna Ticker, conserva campos, unidades, semáforos y valores numéricos sin redondeo adicional. Evita miles de hojas y estilos por celda. Si una tabla supera el límite de filas, se divide en varias hojas. El historial de precios y la matriz completa de correlación permanecen en el resultado guardado; no se incluyen en este libro.

El procesador prepara una exportación por vez y comparte las dos reservas globales con los análisis. La preparación tiene un plazo total de 30 minutos y no se inicia mientras hay análisis activos en ese procesador. Reiniciar el procesador recupera la solicitud y libera su reserva anterior. Una exportación pendiente bloquea sustituir o eliminar su análisis para evitar perder el archivo fuente a mitad del trabajo.

Las tablas de Dashboard usan paginación en el navegador para el límite de 7,300 símbolos. Cambiar el número de filas, buscar u ordenar no requiere peticiones Ajax a endpoints de una sesión ocupada. La autorización para recuperar el resultado y descargar el archivo se valida en el servidor.

CI construye y revisa un libro sintético con 7,300 empresas y 292,000 filas de detalle bajo un límite de memoria de 3 GB. Esa comprobación verifica la exportación, no equivale a ejecutar un análisis real de 7,300 tickers.
