Admin (Agenda) – Guía rápida
---------------------------

### Qué es
Panel para coordinar las agendas de consultores. Lee y escribe contra el web app de Apps Script (`admin.html` usa `https://script.google.com/macros/s/AKfycbxluSUVEHa-jeX-QSJknVE5ZfdiBFMCm7qw3oc-9wNZ_4kT2CnW_2OZNUFZzZZtWr5e/exec`) y consulta presencia en el web app de “call”.

### Acceso
- Credenciales por defecto: `admin` / `consultores#5`.
- Se abre con dos filtros: fecha y consultor; el listado arranca vacío hasta cargar.

### Auto-actualización
- Refresh automático alineado a `:55, :10, :25, :40` (cortes cada 15 minutos desplazados -5m).
- Si la pestaña vuelve del background y se saltó un corte, recarga inmediatamente; si no, solo re-alinea el reloj.
- Botón “Actualizar” recarga al instante.

### Listado de agendamientos
- Cada fila muestra hora, consultor, idPipe, teléfono, nombre, tipo, estado de cancelación y, si existe, badge “Retraso enviado”.
- Estados visuales:
  - Gris tenue: pasado.
  - Verde: en curso con presencia detectada.
  - Amarillo (pendiente) / naranjo (ausente): ventana de presencia sin detectar.
  - Tachado: cancelado.
- Ventana de presencia: comienza 5 min antes de la hora agendada y termina 25 min después.

### Acciones por booking
- Reasignar: busca consultores disponibles para mover la reunión.
- Reprogramar: abre calendario/slots y luego cancela la original con motivo “Reprogramado…”.
- Cancelar: pide motivo y marca cancelado en backend.

### Configuración
- Bloqueos de fecha por consultor o global.
- Edición de horarios por consultor (días/slots) y límite de días futuros.
- Gestión de consultores (crear/eliminar) y listado de activos.

### Presencia y retrasos
- El front solo marca colores; no envía el webhook de retraso.
- El webhook de retraso lo dispara el cron de Apps Script (modo `cron` desde Cloud Scheduler).

### Endpoints principales
- GET `?mode=listBookings&date=YYYY-MM-DD&consultor=todos|ID`
- POST `mode=cancel | blockDate | unblockDate | addConsultor | deleteConsultor | saveSchedule`
- Presencia: `ENDPOINT_CALL?mode=presence_status&idpipe=...`

### Notas operativas
- Si un consultor no tiene horarios configurados, no aparecerá en slots de reprogramación.
- El cron de retrasos se programa en Cloud Scheduler (ver `docs/notes_cloud_scheduler.md`).
