Endpoints y ubicaciones
=======================

Resumen rápido de las URLs de Apps Script usadas por el front (constantes en los HTML), con la ruta en el repo donde viven y el archivo local que corresponde al backend.

| Constante           | URL (`/exec`)                                                                                                                                         | Ruta(s) front en repo                              | Backend local en repo                      |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- | ------------------------------------------ |
| `AUTH_ENDPOINT`     | https://script.google.com/macros/s/AKfycbxJi8qeHg_n9Sp0tAfBoksvyTUSc8l6Qex3bCNmTiD_tHmsC5QF6_fXdObaqKbtIkw2ww/exec                                    | `docs/index.html`                                 | `apps-script/auth/code.gs`                 |
| `ENDPOINT_CALL`     | https://script.google.com/macros/s/AKfycbwnZkcBrj3UJkxAMC3dgxxsWfdsUpni6SYuW2f2DANDHJZGZPod_A_hBd6Q3mumtiPn/exec                                      | `docs/call.html`, `docs/calendar/admin.html` (presencia) | `apps-script/call/code.gs`                 |
| `ENDPOINT_PUNTOS`   | https://script.google.com/macros/s/AKfycbwu1EIBD8PPVTCKIe39zO_sjMzUIAnjGqQxehzdEG2sXppjJEJnta8ZTegTv19T7W2y/exec                                      | `docs/call.html`, `docs/stats.html`, `docs/adminstats.html`, `docs/files/adminstats.html` | `apps-script/puntos/code.gs`               |
| `ENDPOINT_CALENDAR` | https://script.google.com/macros/s/AKfycbxluSUVEHa-jeX-QSJknVE5ZfdiBFMCm7qw3oc-9wNZ_4kT2CnW_2OZNUFZzZZtWr5e/exec                                      | `docs/calendar/admin.html`, `docs/calendar/consultor.html`, `docs/calendar/agendar.html` | `docs/calendar/apps-script/calendar/code.gs` |

Notas:
- Las URLs apuntan a despliegues de Google Apps Script; los archivos indicados son la copia local del código.
- Si se re-publica un web app de Apps Script y cambia la URL `/exec`, actualiza la constante correspondiente en los HTML de front.
