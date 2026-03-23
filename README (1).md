# 🤖 Telegram Bot API Proxy — `main.py`

Servidor HTTP construido con **Flask** que actúa como proxy entre tus aplicaciones y bots de Telegram. Envía comandos automáticamente a bots especializados y devuelve los datos estructurados en formato JSON, con soporte para múltiples fuentes y fallback automático.

---

## 📋 Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Requisitos Previos](#requisitos-previos)
- [Instalación](#instalación)
- [Configuración de Variables de Entorno](#configuración-de-variables-de-entorno)
- [Ejecución](#ejecución)
- [Arquitectura del Sistema](#arquitectura-del-sistema)
- [Endpoints Disponibles](#endpoints-disponibles)
  - [Utilidades](#utilidades)
  - [LederData — Consultas PE](#lederdata--consultas-pe)
  - [Azura Search](#azura-search)
- [Formato de Respuesta](#formato-de-respuesta)
- [Lógica de Fallback y Bloqueo de Bots](#lógica-de-fallback-y-bloqueo-de-bots)
- [Parser Universal](#parser-universal)
- [Descarga de Archivos Adjuntos](#descarga-de-archivos-adjuntos)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Ejemplos de Uso](#ejemplos-de-uso)
- [Errores Comunes](#errores-comunes)

---

## 📖 Descripción General

Este servicio expone una API REST que:

1. Recibe una petición HTTP GET con parámetros.
2. Se conecta a Telegram usando una sesión de usuario (no bot).
3. Envía el comando correspondiente al bot de Telegram configurado.
4. Espera y recolecta la(s) respuesta(s) del bot.
5. Parsea automáticamente los campos detectados (`Clave: Valor`).
6. Devuelve un JSON estructurado con los datos extraídos.

### Bots integrados

| Bot | Identificador | Rol |
|-----|--------------|-----|
| LederData Principal | `@LEDERDATA_OFC_BOT` | Consultas PE (primario) |
| LederData Backup | `@lederdata_publico_bot` | Fallback automático |
| Azura Search | `@AzuraSearchServices_bot` | Consultas independientes |

---

## ✅ Requisitos Previos

- Python **3.9+**
- Cuenta de Telegram con sesión activa (ver [Obtener SESSION_STRING](#configuración-de-variables-de-entorno))
- API ID y API Hash de Telegram (obtenidos en [my.telegram.org](https://my.telegram.org))

### Dependencias Python

```bash
pip install flask flask-cors telethon
```

O usando un archivo `requirements.txt`:

```
flask
flask-cors
telethon
```

```bash
pip install -r requirements.txt
```

---

## ⚙️ Instalación

```bash
# 1. Clonar o descargar el repositorio
git clone <url-del-repo>
cd <carpeta-del-proyecto>

# 2. Instalar dependencias
pip install flask flask-cors telethon

# 3. Configurar variables de entorno (ver sección siguiente)

# 4. Ejecutar
python main.py
```

---

## 🔑 Configuración de Variables de Entorno

Crea un archivo `.env` o configura estas variables en tu entorno antes de ejecutar:

| Variable | Requerida | Descripción | Ejemplo |
|----------|-----------|-------------|---------|
| `API_ID` | ✅ Sí | ID de API de Telegram (my.telegram.org) | `12345678` |
| `API_HASH` | ✅ Sí | Hash de API de Telegram | `abc123def456...` |
| `SESSION_STRING` | ✅ Sí | Cadena de sesión Telethon activa | `1BVts...` |
| `PUBLIC_URL` | ✅ Sí | URL pública del servidor (para links de archivos) | `https://tudominio.com` |
| `PORT` | ❌ No | Puerto del servidor (por defecto: `8080`) | `8080` |

### ¿Cómo obtener el `SESSION_STRING`?

Ejecuta el siguiente script **una sola vez** en tu máquina local para generar la cadena de sesión:

```python
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

API_ID = 12345678       # Tu API ID
API_HASH = "tu_api_hash"

with TelegramClient(StringSession(), API_ID, API_HASH) as client:
    print(client.session.save())
```

Guarda el string resultante como la variable `SESSION_STRING`. **No lo compartas ni lo subas a repositorios públicos.**

### Ejemplo de archivo `.env`

```env
API_ID=12345678
API_HASH=abc123def456789abc123def456789ab
SESSION_STRING=1BVtsOHABu7wJ3...
PUBLIC_URL=https://mi-servidor.com
PORT=8080
```

Para cargar el `.env` automáticamente, instala `python-dotenv`:

```bash
pip install python-dotenv
```

Y agrega al inicio de `main.py`:

```python
from dotenv import load_dotenv
load_dotenv()
```

---

## 🚀 Ejecución

### Modo desarrollo

```bash
python main.py
```

El servidor iniciará en `http://0.0.0.0:8080` (o el puerto configurado).

### Modo producción (con Gunicorn)

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:8080 main:app
```

### Con Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY main.py .
CMD ["python", "main.py"]
```

```bash
docker build -t telegram-proxy .
docker run -p 8080:8080 --env-file .env telegram-proxy
```

---

## 🏗️ Arquitectura del Sistema

```
Cliente HTTP
    │
    ▼
┌─────────────────────────────┐
│       Flask REST API         │
│  (universal_handler / CORS) │
└────────────┬────────────────┘
             │
    ┌────────┴─────────┐
    │                  │
    ▼                  ▼
LederData Bot      Azura Bot
  (primary)       (@AzuraSearch...)
    │
    ▼ (si falla)
LederData Backup
    │
    ▼
Telethon Client
(sesión de usuario)
    │
    ▼
Bot de Telegram
    │
    ▼
Parser Universal
(extrae campos Clave: Valor)
    │
    ▼
JSON estructurado → Cliente
```

---

## 🌐 Endpoints Disponibles

Todos los endpoints usan método `GET`.

### Utilidades

#### `GET /health`
Comprueba si el servidor está activo.

**Respuesta:**
```json
{ "status": "healthy" }
```

---

#### `GET /status`
Estado actual de los bots LederData.

**Respuesta:**
```json
{
  "status": "online",
  "bots": ["@LEDERDATA_OFC_BOT", "@lederdata_publico_bot"],
  "primary_blocked": false,
  "backup_blocked": false,
  "primary_blocked_until": null,
  "backup_blocked_until": null
}
```

---

#### `GET /files/<nombre_archivo>`
Sirve archivos descargados (PDFs, imágenes) adjuntos a las respuestas de los bots.

**Ejemplo:**
```
GET /files/1700000000_123.pdf
```

---

### LederData — Consultas PE

Todos los endpoints LederData aceptan parámetros como query string.

#### `GET /dni?dni=<número>`
Consulta por número de DNI peruano (8 dígitos).

```
GET /dni?dni=12345678
```

---

#### `GET /ruc?query=<número>`
Consulta por RUC (11 dígitos).

```
GET /ruc?query=20123456789
```

---

#### `GET /cla?dni=<número>`
Consulta de clave AFP por DNI.

```
GET /cla?dni=12345678
```

---

#### `GET /afp?dni=<número>`
Consulta AFP por DNI.

```
GET /afp?dni=12345678
```

---

#### `GET /bdir?query=<dirección>`
Búsqueda por dirección domiciliaria.

```
GET /bdir?query=AV+LOS+PINOS+123
```

---

#### `GET /pasaporte?pasaporte=<número>`
Consulta por número de pasaporte.

```
GET /pasaporte?pasaporte=AB123456
```

---

#### `GET /cedula?cedula=<número>`
Consulta por cédula (venezolanos).

```
GET /cedula?cedula=V12345678
```

---

#### `GET /sunr?razon_social=<texto>`
Consulta SUNAT por razón social (mínimo 3 caracteres, sin números ni `|`).

```
GET /sunr?razon_social=EMPRESA+EJEMPLO+SAC
```

---

#### `GET /sbs?query=<parámetro>`
Consulta SBS.

```
GET /sbs?query=12345678
```

---

#### `GET /dend?dni=<número>`
Denuncias por DNI.

```
GET /dend?dni=12345678
```

---

#### `GET /dence?cedula=<número>`
Denuncias por cédula.

```
GET /dence?cedula=V12345678
```

---

#### `GET /denpas?pasaporte=<número>`
Denuncias por pasaporte.

```
GET /denpas?pasaporte=AB123456
```

---

#### `GET /denci?carnet_extranjeria=<número>`
Denuncias por carnet de extranjería.

```
GET /denci?carnet_extranjeria=001234567
```

---

#### `GET /denp?placa=<placa>`
Denuncias por placa vehicular.

```
GET /denp?placa=ABC123
```

---

#### `GET /denar?serie_armamento=<serie>`
Denuncias por serie de armamento.

```
GET /denar?serie_armamento=SN123456
```

---

#### `GET /dencl?clave_denuncia=<clave>`
Consulta por clave de denuncia.

```
GET /dencl?clave_denuncia=2024-XXXX
```

---

#### `GET /cafp?dni=<número>`
Consulta AFP complementaria por DNI.

```
GET /cafp?dni=12345678
```

---

#### `GET /dni_nombres?apepaterno=<apellido>&apematerno=<apellido>&nombres=<nombres>`
Búsqueda de DNI por nombres y apellidos (RENIEC NOMBRES).

**Parámetros:**

| Parámetro | Requerido | Descripción |
|-----------|-----------|-------------|
| `apepaterno` | ✅ | Apellido paterno |
| `apematerno` | ✅ | Apellido materno |
| `nombres` | ❌ | Nombres (opcional) |

```
GET /dni_nombres?apepaterno=GARCIA&apematerno=LOPEZ&nombres=JUAN
```

---

#### `GET /venezolanos_nombres?query=<texto>`
Búsqueda de venezolanos por nombre.

```
GET /venezolanos_nombres?query=JUAN+GARCIA
```

---

### Azura Search

Todos los endpoints Azura siguen el patrón `/azura_<comando>`.

**Parámetros aceptados:** `dni`, `query`, o `param`.

#### `GET /azura_<comando>?dni=<valor>`

Ejemplo de endpoints Azura:

```
GET /azura_dni?dni=12345678
GET /azura_placa?query=ABC123
GET /azura_ruc?param=20123456789
```

El sistema reemplaza `azura_` por `/` y envía el comando resultante a `@AzuraSearchServices_bot`.

---

## 📦 Formato de Respuesta

### Respuesta exitosa con datos estructurados

```json
{
  "status": "success",
  "data": {
    "dni": "12345678",
    "apellido_paterno": "GARCIA",
    "apellido_materno": "LOPEZ",
    "nombres": "JUAN CARLOS",
    "fecha_nacimiento": "01/01/1990",
    "genero": "MASCULINO",
    "direccion": "AV. LOS PINOS 123",
    "distrito": "MIRAFLORES",
    "provincia": "LIMA",
    "departamento": "LIMA"
  },
  "raw_message": "DNI: 12345678\nAPELLIDO PATERNO: GARCIA\n..."
}
```

### Respuesta exitosa con mensaje libre

```json
{
  "status": "success",
  "message": "Texto de respuesta del bot sin campos detectados."
}
```

### Respuesta con archivos adjuntos

```json
{
  "status": "success",
  "data": {
    "dni": "12345678",
    "urls": [
      { "url": "https://tudominio.com/files/1700000000_123.jpg", "type": "document" }
    ]
  },
  "raw_message": "..."
}
```

### Respuesta de error

```json
{
  "status": "error",
  "message": "No se encontraron resultados."
}
```

---

## 🔄 Lógica de Fallback y Bloqueo de Bots

El sistema implementa un mecanismo inteligente de gestión de fallos para los bots LederData:

```
Petición entrante
      │
      ▼
¿Bot principal bloqueado?
   │           │
  NO           SÍ
   │           │
   ▼           ▼
Intenta     Usa bot
principal   backup
   │
   ▼
¿Respuesta en 35s?
   │           │
  SÍ           NO
   │           │
   ▼           ▼
Procesa    Registra fallo
           (bloqueado 3h)
               │
               ▼
           Reintenta con
           bot backup (50s)
               │
               ▼
           ¿Respuesta?
           │        │
          SÍ        NO
           │        │
           ▼        ▼
        Procesa   Error 
```

### Timeouts configurados

| Bot | Timeout |
|-----|---------|
| LederData Principal | 35 segundos |
| LederData Backup | 50 segundos |
| Azura | 35 segundos |

### Tiempo de bloqueo

Si el bot principal no responde, queda marcado como **bloqueado durante 3 horas**. Durante ese período, todas las peticiones se redirigen automáticamente al bot backup.

---

## 🧩 Parser Universal

El **Parser Universal** analiza automáticamente el texto de respuesta de los bots y detecta campos con el patrón:

```
Nombre de Campo: Valor del campo
```

### Reglas de normalización

- Las claves se convierten a **minúsculas**.
- Los espacios en claves se reemplazan por **guiones bajos** (`_`).
- Se eliminan caracteres especiales de las claves.
- Los valores se limpian de saltos de línea extra.

### Ejemplo

**Texto recibido del bot:**
```
DNI: 12345678
APELLIDO PATERNO: GARCIA RODRIGUEZ
FECHA DE NACIMIENTO: 01/01/1990
ESTADO: VIGENTE
```

**Resultado del parser:**
```json
{
  "dni": "12345678",
  "apellido_paterno": "GARCIA RODRIGUEZ",
  "fecha_de_nacimiento": "01/01/1990",
  "estado": "VIGENTE"
}
```

El parser se aplica a **todas** las respuestas de LederData y Azura, complementando los campos extraídos manualmente.

---

## 📁 Descarga de Archivos Adjuntos

Cuando el bot responde con un archivo adjunto (PDF, imagen), el sistema:

1. Descarga el archivo automáticamente al directorio `downloads/`.
2. Genera una URL pública: `{PUBLIC_URL}/files/{nombre_archivo}`.
3. Incluye la URL en el campo `urls` del JSON de respuesta.

```
downloads/
├── 1700000000_123.jpg
├── 1700000001_456.pdf
└── ...
```

> ⚠️ Los archivos se acumulan en el directorio `downloads/`. Implementa una tarea de limpieza periódica si es necesario.

---

## 📂 Estructura del Proyecto

```
proyecto/
│
├── main.py              # Código principal del servidor
├── requirements.txt     # Dependencias Python
├── .env                 # Variables de entorno (NO subir a Git)
├── .gitignore           # Ignorar .env y downloads/
├── downloads/           # Archivos descargados de los bots (auto-creado)
└── README.md            # Esta documentación
```

### `.gitignore` recomendado

```
.env
downloads/
__pycache__/
*.pyc
*.session
```

---

## 💡 Ejemplos de Uso

### Con `curl`

```bash
# Consulta DNI
curl "http://localhost:8080/dni?dni=12345678"

# Consulta por nombres
curl "http://localhost:8080/dni_nombres?apepaterno=GARCIA&apematerno=LOPEZ"

# Estado del servidor
curl "http://localhost:8080/status"

# Consulta Azura
curl "http://localhost:8080/azura_dni?dni=12345678"

# SUNAT razón social
curl "http://localhost:8080/sunr?razon_social=EMPRESA+EJEMPLO"
```

### Con JavaScript / Fetch

```javascript
const response = await fetch('http://localhost:8080/dni?dni=12345678');
const data = await response.json();

if (data.status === 'success') {
  console.log('Nombres:', data.data?.nombres);
  console.log('Dirección:', data.data?.direccion);
} else {
  console.error('Error:', data.message);
}
```

### Con Python / requests

```python
import requests

res = requests.get('http://localhost:8080/dni', params={'dni': '12345678'})
data = res.json()

if data['status'] == 'success':
    print('DNI:', data['data'].get('dni'))
    print('Nombres:', data['data'].get('nombres'))
else:
    print('Error:', data['message'])
```

---

## ⚠️ Errores Comunes

| Error | Causa | Solución |
|-------|-------|----------|
| `Credenciales de Telegram no configuradas.` | Faltan `API_ID`, `API_HASH` o `SESSION_STRING` | Configurar las variables de entorno correctamente |
| `Cliente no autorizado.` | `SESSION_STRING` inválida o expirada | Regenerar el `SESSION_STRING` |
| `No se obtuvo respuesta de ningún bot.` | Ambos bots no respondieron en el tiempo límite | Verificar que los bots estén activos; revisar `/status` |
| `No se encontraron resultados.` | El bot confirmó que no hay datos para el parámetro | El parámetro consultado no existe en la base de datos |
| `Parámetro faltante` | No se envió el query param requerido | Agregar el parámetro correcto a la URL |
| `Por favor, usa el formato correcto.` | El valor enviado no cumple las validaciones | Revisar formato del parámetro (p.ej. `sunr` requiere texto, no números) |

---

## 🔒 Consideraciones de Seguridad

- **No expongas este servicio públicamente sin autenticación.** Agrega un middleware de API Key o JWT.
- **Protege tu `SESSION_STRING`** — equivale a las credenciales completas de tu cuenta de Telegram.
- **Usa HTTPS** en producción (con Nginx o un proxy inverso).
- Este proyecto usa una **sesión de usuario** de Telegram, no una sesión de bot. Úsalo responsablemente y respetando los Términos de Servicio de Telegram.

---

## 📄 Licencia

Este proyecto es de uso privado. No redistribuir sin autorización.
