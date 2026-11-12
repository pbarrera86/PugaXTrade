# Derechos de datos para PugaXTrade

Revisión: 12 de noviembre de 2026. No consta en esta revisión una autorización
contractual que cubra la redistribución comercial. Esto no afirma que el titular
carezca de contrato: debe verificarse su alcance antes de cerrar el pendiente.

## Uso observado en código

| Fuente | Acceso actual | Uso en la aplicación |
| --- | --- | --- |
| Finviz | HTML quote.ashx, R/utils_core.R y R/institutional_core.R | Fundamentales de acciones estadounidenses, caché, métricas y exportación Excel. |
| Yahoo Finance | quantmod::getSymbols y quoteSummary | Históricos OHLCV, indicadores/backtest y fundamentales, incluidos símbolos .MX; resultados y Excel. |

## Condiciones que deben confirmarse

Yahoo prohíbe por defecto el acceso automatizado sin permiso previo y la
explotación comercial sin autorización expresa. Su permiso para capturas
ilustrativas no cubre automáticamente cotizaciones de terceros ni este servicio.
Finviz anuncia API/exportaciones para Elite, pero esa funcionalidad no acredita
por sí sola derechos de redistribución en un SaaS; su FAQ indica que no vende
históricos crudos a terceros. No se ha podido verificar aquí el contrato completo
de Finviz, por lo que no se atribuyen prohibiciones adicionales no comprobadas.

Fuentes oficiales consultadas:
- https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html
- https://legal.yahoo.com/us/en/yahoo/permissions/requests/index.html
- https://finviz.com/help/faq
- https://finviz.com/blog/our-upgraded-api-higher-limits-clearer-data-and-more-ways-to-connect/

## Solicitud preparada para el proveedor (no enviada)

Solicitamos confirmar por escrito la licencia adecuada para PugaXTrade, una
aplicación de análisis de acciones estadounidenses y mexicanas con suscripción
anual. El servidor consulta precios históricos y fundamentales, conserva caché,
calcula indicadores, puntuaciones y simulaciones, muestra resultados a usuarios
autenticados y permite descargar Excel con métricas y datos derivados.

¿Su licencia permite expresamente cada uso descrito? Indiquen cobertura por
mercado, acceso automatizado permitido, límites de usuarios y consultas,
retención/caché, atribución, retrasos, exportación por usuarios, redistribución
de datos originales y derivados, costes de bolsas adicionales, precio y efecto
de terminar el contrato sobre los datos guardados. Necesitamos distinguir una
suscripción de uso individual de la licencia de nuestra aplicación comercial.

Antes de enviar, el titular debe añadir razón social, usuarios previstos y
volumen estimado. No comprar un plan personal suponiendo que cubre estos usos.

## Criterio de decisión

Conservar autorización o contrato y mapear cada uso a su permiso. Si no cubre
históricos, mercado mexicano, visualización o Excel, elegir un proveedor con
cobertura adecuada y adaptar el acceso. Los indicadores calculados y un aviso
educativo no convierten automáticamente el uso de la fuente en autorizado.
