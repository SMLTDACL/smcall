Cloud Scheduler / Apps Script cron
----------------------------------
- El cron de retrasos se dispara ahora por HTTP: `https://script.google.com/macros/s/<DEPLOY_ID>/exec?mode=cron`.
- Método: GET, sin cuerpo. Si el deploy está público no requiere headers; si está restringido, usar el service account del Scheduler.
- Programar dos jobs en Cloud Scheduler: `0 * * * *` y `30 * * * *` (minutos cerrados :00 y :30).
- Eliminamos el disparador time-driven interno; otros `mode` del Web App siguen funcionando y no llaman al cron.
- El endpoint `markDelay` quedó deshabilitado (solo el `mode=cron` dispara el webhook); el front solo pinta colores.

No show / asistencia (calendar/code.gs + call/code.gs)
-----------------------------------------------------
- Endpoint cron asistencia: `https://script.google.com/macros/s/<DEPLOY_ID>/exec?mode=cron_asistencia`
- Programar dos jobs en Cloud Scheduler: `1 * * * *` y `31 * * * *` (1 minuto después del cierre de cada slot).
- Ventana evaluada por cron:
  - Slot fijo de 30 minutos (CFG.SLOT_MINUTES) + 10 minutos antes del inicio.
  - El cron siempre mira **solo la ventana que acaba de terminar** (ej: al minuto :01 mira el slot que terminó en :00).
- Qué busca:
  - En `Agendas` (calendar/code.gs) toma bookings de la ventana recién finalizada, ignora cancelados y solo los que no tengan marcada asistencia.
  - Consulta presencia por `idpipe` contra `call/code.gs` (`mode=presence_status`), que lee la columna CQ (PRESENCE_LOG) y devuelve `lastIn`/`lastOut`.
  - Se considera asistencia si hay `lastIn` o `lastOut` dentro de la ventana (inicio-10m hasta fin del slot).
- Qué marca:
  - Si **no** hubo presencia en esa ventana, escribe `no_show` en la columna O (`asistencia`) de `Agendas`.
  - No reabre ni limpia marcas: una vez puesto `no_show`, queda fijo.
- Front:
  - `admin.html` y `consultor.html` muestran la badge “No se llamó” cuando `asistencia` es `no_show`.
