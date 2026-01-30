import os
import re
import asyncio
import traceback
import time
import json
import mimetypes
from datetime import datetime, timedelta
from urllib.parse import unquote
from concurrent.futures import TimeoutError as FutureTimeoutError
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
from telethon.errors.rpcerrorlist import UserBlockedError

# --- Configuración y Variables de Entorno ---
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PUBLIC_URL = os.getenv("PUBLIC_URL", "").rstrip("/")
SESSION_STRING = os.getenv("SESSION_STRING", None)
PORT = int(os.getenv("PORT", 8080))

# --- Configuración Interna ---
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# --- Bots LederData (NO MODIFICAR LÓGICA) ---
LEDERDATA_BOT_ID = "@LEDERDATA_OFC_BOT"
LEDERDATA_BACKUP_BOT_ID = "@lederdata_publico_bot"
ALL_BOT_IDS = [LEDERDATA_BOT_ID, LEDERDATA_BACKUP_BOT_ID]

TIMEOUT_PRIMARY = 35
TIMEOUT_BACKUP = 50
BOT_BLOCK_HOURS = 3  # 3 horas

# --- Nuevo bot Azura (SIN BACKUP) ---
AZURA_BOT_ID = "@AzuraSearchServices_bot"
AZURA_TIMEOUT = 35  # Esperar ~35s sin reenvíos

# --- Trackeo de Fallos de Bots (solo LederData principal) ---
bot_fail_tracker = {}

def is_bot_blocked(bot_id: str) -> bool:
    """Verifica si un bot está bloqueado (no responde)"""
    last_fail_time = bot_fail_tracker.get(bot_id)
    if not last_fail_time:
        return False
    now = datetime.now()
    block_time_ago = now - timedelta(hours=BOT_BLOCK_HOURS)
    if last_fail_time > block_time_ago:
        return True
    bot_fail_tracker.pop(bot_id, None)
    return False

def record_bot_failure(bot_id: str):
    """Registra un fallo de bot (no respuesta)"""
    bot_fail_tracker[bot_id] = datetime.now()

# --- Lógica de Limpieza y Extracción de Datos (LederData) ---
def clean_and_extract(raw_text: str):
    if not raw_text:
        return {"text": "", "fields": {}}
    text = raw_text
    text = re.sub(r"\[#?LEDER_BOT\]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\[CONSULTA PE\]", "", text, flags=re.IGNORECASE)
    header_pattern = r"^\[.*?\]\s*→\s*.*?\[.*?\](\r?\n){1,2}"
    text = re.sub(header_pattern, "", text, flags=re.IGNORECASE | re.DOTALL)
    footer_pattern = r"((\r?\n){1,2}\[|Página\s*\d+\/\d+.*|(\r?\n){1,2}Por favor, usa el formato correcto.*|↞ Anterior|Siguiente ↠.*|Credits\s*:.+|Wanted for\s*:.+|\s*@lederdata.*|(\r?\n){1,2}\s*Marca\s*@lederdata.*|(\r?\n){1,2}\s*Créditos\s*:\s*\d+)"
    text = re.sub(footer_pattern, "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"\-{3,}", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"\s+", " ", text)
    text = text.strip()

    fields = {}
    patterns = {
        "dni": r"DNI\s*:\s*(\d{8})",
        "ruc": r"RUC\s*:\s*(\d{11})",
        "apellido_paterno": r"APELLIDO\s+PATERNO\s*:\s*(.*?)(?:\n|$)",
        "apellido_materno": r"APELLIDO\s+MATERNO\s*:\s*(.*?)(?:\n|$)",
        "nombres": r"NOMBRES\s*:\s*(.*?)(?:\n|$)",
        "estado": r"ESTADO\s*:\s*(.*?)(?:\n|$)",
        "fecha_nacimiento": r"(?:FECHA\s+DE\s+NACIMIENTO|F\.?NAC\.?)\s*:\s*(.*?)(?:\n|$)",
        "genero": r"(?:GÉNERO|SEXO)\s*:\s*(.*?)(?:\n|$)",
        "direccion": r"(?:DIRECCIÓN|DOMICILIO)\s*:\s*(.*?)(?:\n|$)",
        "ubigeo": r"UBIGEO\s*:\s*(.*?)(?:\n|$)",
        "departamento": r"DEPARTAMENTO\s*:\s*(.*?)(?:\n|$)",
        "provincia": r"PROVINCIA\s*:\s*(.*?)(?:\n|$)",
        "distrito": r"DISTRITO\s*:\s*(TODO\s+EL\s+DISTRITO|.*?)(?:\n|$)",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            fields[key] = match.group(1).strip()
            text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.DOTALL)

    photo_type_match = re.search(r"Foto\s*:\s*(rostro|huella|firma|adverso|reverso).*", text, re.IGNORECASE)
    if photo_type_match:
        fields["photo_type"] = photo_type_match.group(1).lower()

    not_found_pattern = r"\[⚠️\]\s*(no se encontro información|no se han encontrado resultados|no se encontró una|no hay resultados|no tenemos datos|no se encontraron registros)"
    if re.search(not_found_pattern, text, re.IGNORECASE | re.DOTALL):
        fields["not_found"] = True

    text = re.sub(r"\n\s*\n", "\n", text).strip()
    return {"text": text, "fields": fields}

def format_nm_response(all_received_messages):
    """Formatea respuesta completa de /nm y /nmv sin recortes"""
    combined_text = ""
    for msg in all_received_messages:
        if msg.get("message"):
            combined_text += msg.get("message", "") + "\n"
    combined_text = combined_text.strip()
    
    if not combined_text:
        return json.dumps({"status": "success", "message": ""}, ensure_ascii=False)

    # Limpiar solo encabezados no deseados, manteniendo todo el contenido
    lines = combined_text.split('\n')
    cleaned_lines = []
    
    for line in lines:
        line_stripped = line.strip()
        
        # Saltar líneas de encabezado del bot
        if "RENIEC NOMBRES [PREMIUM]" in line or ("RENIEC NOMBRES" in line and "PREMIUM" in line):
            # Mantener solo el contador de resultados si existe
            count_part = re.search(r"Se encontr[oó]\s+\d+\s+resultados?", line, re.IGNORECASE)
            if count_part:
                cleaned_lines.append(f"→ {count_part.group(0)}.")
            continue
        
        # Saltar marcas de bot
        if re.match(r'^\[.*?LEDER.*?\]', line_stripped, re.IGNORECASE):
            continue
            
        # Mantener todo lo demás
        if line_stripped:
            cleaned_lines.append(line_stripped)
    
    formatted_text = '\n'.join(cleaned_lines).strip()
    
    return json.dumps({
        "status": "success",
        "message": formatted_text
    }, ensure_ascii=False)

# --- NUEVO: Consolidación de respuesta Azura (multi-mensajes -> uno solo) ---
def format_azura_response(all_received_messages):
    """Consolida todos los mensajes de Azura en una respuesta completa"""
    parts = []
    for msg in all_received_messages:
        t = (msg.get("message") or "").strip()
        if t:
            parts.append(t)
    final_text = "\n".join(parts).strip()
    return {"status": "success", "message": final_text}

# --- Función principal LederData ---
async def send_telegram_command(command: str, consulta_id: str = None, endpoint_path: str = None):
    client = None
    try:
        if API_ID == 0 or not API_HASH or not SESSION_STRING:
            raise Exception("Credenciales de Telegram no configuradas.")

        session = StringSession(SESSION_STRING)
        client = TelegramClient(session, API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            raise Exception("Cliente no autorizado.")

        primary_blocked = is_bot_blocked(LEDERDATA_BOT_ID)

        bot_to_use = None
        use_backup = False

        if not primary_blocked:
            bot_to_use = LEDERDATA_BOT_ID
            use_backup = False
        else:
            bot_to_use = LEDERDATA_BACKUP_BOT_ID
            use_backup = True

        all_received_messages = []
        stop_collecting = asyncio.Event()
        last_message_time = [time.time()]

        @client.on(events.NewMessage(incoming=True))
        async def temp_handler(event):
            if stop_collecting.is_set():
                return

            try:
                entity = await client.get_entity(bot_to_use)
                if event.sender_id != entity.id:
                    return

                last_message_time[0] = time.time()
                raw_text = event.raw_text or ""

                if endpoint_path in ["/dni_nombres", "/venezolanos_nombres"] or command.startswith(("/nm", "/nmv")):
                    cleaned = {"text": raw_text, "fields": {}}
                else:
                    cleaned = clean_and_extract(raw_text)

                msg_obj = {
                    "message": cleaned["text"],
                    "fields": cleaned["fields"],
                    "urls": [],
                    "event_message": event.message
                }
                all_received_messages.append(msg_obj)

                if "ANTI-SPAM" in raw_text and "INTENTA DESPUÉS DE 10 SEGUNDOS" in raw_text:
                    if not use_backup:
                        stop_collecting.set()
                    return

                if re.search(r"\[⚠️\]\s*no se encontro información", raw_text, re.IGNORECASE):
                    stop_collecting.set()
                    return

            except Exception as e:
                print(f"Error en handler: {e}")

        # Enviar comando UNA SOLA VEZ
        await client.send_message(bot_to_use, command)

        start_time = time.time()
        timeout_val = TIMEOUT_PRIMARY if bot_to_use == LEDERDATA_BOT_ID else TIMEOUT_BACKUP

        while (time.time() - start_time) < timeout_val:
            if stop_collecting.is_set():
                break

            if all_received_messages and (time.time() - last_message_time[0]) > 4.5:
                break

            await asyncio.sleep(0.5)

        client.remove_event_handler(temp_handler)

        if not all_received_messages:
            if bot_to_use == LEDERDATA_BOT_ID:
                record_bot_failure(bot_to_use)

            if not use_backup:
                await asyncio.sleep(5)

                bot_to_use = LEDERDATA_BACKUP_BOT_ID
                use_backup = True

                @client.on(events.NewMessage(incoming=True))
                async def backup_handler(event):
                    if stop_collecting.is_set():
                        return

                    try:
                        entity = await client.get_entity(bot_to_use)
                        if event.sender_id != entity.id:
                            return

                        last_message_time[0] = time.time()
                        raw_text = event.raw_text or ""

                        if endpoint_path in ["/dni_nombres", "/venezolanos_nombres"] or command.startswith(("/nm", "/nmv")):
                            cleaned = {"text": raw_text, "fields": {}}
                        else:
                            cleaned = clean_and_extract(raw_text)

                        msg_obj = {
                            "message": cleaned["text"],
                            "fields": cleaned["fields"],
                            "urls": [],
                            "event_message": event.message
                        }
                        all_received_messages.append(msg_obj)

                        if re.search(r"\[⚠️\]\s*no se encontro información", raw_text, re.IGNORECASE):
                            stop_collecting.set()
                            return

                    except Exception as e:
                        print(f"Error en backup handler: {e}")

                # Enviar comando UNA SOLA VEZ al backup
                await client.send_message(bot_to_use, command)

                start_time = time.time()
                last_message_time[0] = time.time()

                while (time.time() - start_time) < TIMEOUT_BACKUP:
                    if stop_collecting.is_set():
                        break

                    if all_received_messages and (time.time() - last_message_time[0]) > 4.5:
                        break

                    await asyncio.sleep(0.5)

                client.remove_event_handler(backup_handler)

                if not all_received_messages:
                    raise Exception("No se obtuvo respuesta de ningún bot.")
            else:
                raise Exception("No se obtuvo respuesta del bot.")

        return await process_bot_response(client, all_received_messages, command, endpoint_path)

    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        if client:
            await client.disconnect()

async def process_bot_response(client, all_received_messages, command, endpoint_path):
    """Procesa la respuesta del bot y retorna datos completos sin recortes"""
    if any("formato correcto" in (m["message"] or "").lower() for m in all_received_messages):
        return {"status": "error", "message": "Formato incorrecto."}

    if any(m.get("fields", {}).get("not_found") for m in all_received_messages):
        return {"status": "error", "message": "No se encontraron resultados."}

    # Descargar archivos adjuntos (LederData)
    for msg in all_received_messages:
        event_msg = msg.get("event_message")
        if event_msg and getattr(event_msg, "media", None):
            try:
                ext = ".pdf" if "pdf" in str(event_msg.media).lower() else ".jpg"
                fname = f"{int(time.time())}_{event_msg.id}{ext}"
                path = await client.download_media(event_msg, file=os.path.join(DOWNLOAD_DIR, fname))
                if path:
                    msg["urls"].append({"url": f"{PUBLIC_URL}/files/{fname}", "type": "document"})
            except Exception as e:
                print(f"Error descargando archivo: {e}")

    # Para endpoints de nombres, retornar respuesta completa formateada
    if endpoint_path in ["/dni_nombres", "/venezolanos_nombres"] or command.startswith(("/nm", "/nmv")):
        return json.loads(format_nm_response(all_received_messages))

    # Para otros endpoints, consolidar todos los campos
    final_fields = {}
    full_text_parts = []
    urls = []
    
    for msg in all_received_messages:
        # Consolidar texto completo
        if msg.get("message"):
            full_text_parts.append(msg.get("message"))
        
        # Consolidar campos extraídos
        for k, v in msg.get("fields", {}).items():
            if v and not final_fields.get(k):
                final_fields[k] = v
        
        # Consolidar URLs
        urls.extend(msg.get("urls", []))

    # Agregar texto completo consolidado
    if full_text_parts:
        final_fields["mensaje_completo"] = "\n".join(full_text_parts).strip()
    
    final_fields["urls"] = urls
    
    return {"status": "success", "data": final_fields}

def run_telegram_command(command: str, consulta_id: str = None, endpoint_path: str = None):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(send_telegram_command(command, consulta_id, endpoint_path))
    finally:
        loop.close()

# --- NUEVO: Envío a AZURA (API) ---
async def send_azura_command(command: str, endpoint_path: str = None):
    """Envía comando a Azura y retorna respuesta completa consolidada"""
    client = None
    try:
        if API_ID == 0 or not API_HASH or not SESSION_STRING:
            raise Exception("Credenciales de Telegram no configuradas.")

        session = StringSession(SESSION_STRING)
        client = TelegramClient(session, API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            raise Exception("Cliente no autorizado.")

        all_received_messages = []
        last_message_time = [time.time()]

        @client.on(events.NewMessage(incoming=True))
        async def azura_handler(event):
            try:
                entity = await client.get_entity(AZURA_BOT_ID)
                if event.sender_id != entity.id:
                    return

                last_message_time[0] = time.time()
                raw_text = event.raw_text or ""

                # Guardar mensaje completo sin modificaciones
                all_received_messages.append({
                    "message": raw_text,
                    "event_message": event.message
                })
            except Exception as e:
                print(f"Error en azura handler: {e}")

        # Enviar comando UNA SOLA VEZ
        await client.send_message(AZURA_BOT_ID, command)

        start_time = time.time()

        # Esperar hasta AZURA_TIMEOUT segundos
        while (time.time() - start_time) < AZURA_TIMEOUT:
            if all_received_messages and (time.time() - last_message_time[0]) > 4.5:
                break
            await asyncio.sleep(0.5)

        client.remove_event_handler(azura_handler)

        if not all_received_messages:
            return {"status": "error", "message": "No se obtuvo respuesta de @AzuraSearchServices_bot."}

        # Consolidar múltiples mensajes en uno solo
        return format_azura_response(all_received_messages)

    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        if client:
            await client.disconnect()

def run_azura_command(command: str, endpoint_path: str = None):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(send_azura_command(command, endpoint_path=endpoint_path))
    finally:
        loop.close()

# --- HELPER DE COMANDOS (LederData) ---
def get_command_and_param(path, request_args):
    cmd = path.lstrip('/')
    p = (
        request_args.get("dni")
        or request_args.get("query")
        or request_args.get("pasaporte")
        or request_args.get("cedula")
        or request_args.get("direccion")
        or request_args.get("carnet_extranjeria")
        or request_args.get("cedula_identidad")
        or request_args.get("placa")
        or request_args.get("serie_armamento")
        or request_args.get("clave_denuncia")
    )

    mapping = {
        "cla": f"/cla {p}",
        "afp": f"/afp {p}",
        "bdir": f"/bdir {p}",
        "pasaporte": f"/pasaporte {p}",
        "cedula": f"/cedula {p}",
        "dend": f"/dend {p}",
        "dence": f"/dence {p}",
        "denpas": f"/denpas {p}",
        "denci": f"/denci {p}",
        "denp": f"/denp {p}",
        "denar": f"/denar {p}",
        "dencl": f"/dencl {p}",
        "cafp": f"/cafp {p}",
        "sbs": f"/sbs {p}"
    }

    final_cmd = mapping.get(cmd)
    if not final_cmd and not p:
        return None, "Parámetro faltante"
    return final_cmd or f"/{cmd} {p}", None

# --- APP FLASK ---
app = Flask(__name__)
CORS(app)

@app.route("/files/<path:filename>")
def files(filename):
    return send_from_directory(DOWNLOAD_DIR, filename)

@app.route("/<path:endpoint>", methods=["GET"])
def universal_handler(endpoint):
    # Especiales existentes
    if endpoint in ["files", "health", "status", "dni_nombres", "venezolanos_nombres"]:
        return handle_special(endpoint)

    # --- Rutas Azura ---
    if endpoint.startswith("azura_"):
        az_cmd_name = endpoint.replace("azura_", "", 1).strip()
        if not az_cmd_name:
            return jsonify({"status": "error", "message": "Comando Azura inválido."}), 400

        p = request.args.get("dni") or request.args.get("query") or request.args.get("param")
        if not p:
            return jsonify({"status": "error", "message": "Parámetro faltante"}), 400

        # Comando final a Azura
        command = f"/{az_cmd_name} {p}"

        result = run_azura_command(command, endpoint_path=f"/{endpoint}")
        return jsonify(result)

    # --- Rutas LederData ---
    command, error = get_command_and_param(endpoint, request.args)
    if error:
        return jsonify({"status": "error", "message": error}), 400

    result = run_telegram_command(command, endpoint_path=f"/{endpoint}")
    return jsonify(result)

def handle_special(endpoint):
    if endpoint == "status":
        primary_blocked = is_bot_blocked(LEDERDATA_BOT_ID)
        backup_blocked = False
        return jsonify({
            "status": "online",
            "bots": ALL_BOT_IDS,
            "primary_blocked": primary_blocked,
            "backup_blocked": backup_blocked,
            "primary_blocked_until": bot_fail_tracker.get(LEDERDATA_BOT_ID) if primary_blocked else None,
            "backup_blocked_until": None
        })

    if endpoint == "health":
        return jsonify({"status": "healthy"})

    if endpoint == "dni_nombres":
        nom = unquote(request.args.get("nombres", "")).replace(" ", ",")
        pat = unquote(request.args.get("apepaterno", "")).replace(" ", "+")
        mat = unquote(request.args.get("apematerno", "")).replace(" ", "+")
        if not pat or not mat:
            return jsonify({"error": "Faltan apellidos"}), 400
        res = run_telegram_command(f"/nm {nom}|{pat}|{mat}", endpoint_path="/dni_nombres")
        return jsonify(res)

    if endpoint == "venezolanos_nombres":
        q = unquote(request.args.get("query", ""))
        if not q:
            return jsonify({"error": "Query faltante"}), 400
        res = run_telegram_command(f"/nmv {q}", endpoint_path="/venezolanos_nombres")
        return jsonify(res)

    return jsonify({"error": "Not found"}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
