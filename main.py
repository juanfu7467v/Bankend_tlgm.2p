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

# --- 🆕 PARSER UNIVERSAL ---
def universal_parser(raw_text: str) -> dict:
    """
    Parser Universal: Detecta automáticamente campos con formato 'Clave: Valor'
    y los convierte en un diccionario estructurado.
    
    Reglas:
    - Detecta cualquier línea con formato: "Palabra(s): Valor"
    - Normaliza claves (minúsculas, guiones bajos, sin espacios extras)
    - Mantiene valores originales completos sin recortar
    """
    if not raw_text:
        return {}
    
    parsed_data = {}
    
    # Patrón que captura: "Cualquier texto seguido de dos puntos: Valor"
    # Captura multilínea para valores largos
    pattern = r'^([^:\n]+):\s*(.+?)(?=\n[^:\n]+:|$)'
    
    matches = re.finditer(pattern, raw_text, re.MULTILINE | re.DOTALL)
    
    for match in matches:
        key_raw = match.group(1).strip()
        value_raw = match.group(2).strip()
        
        if not key_raw or not value_raw:
            continue
        
        # Normalizar clave: minúsculas, reemplazar espacios por guiones bajos
        key_normalized = re.sub(r'\s+', '_', key_raw.lower())
        key_normalized = re.sub(r'[^\w_]', '', key_normalized)  # Eliminar caracteres especiales
        
        # Limpiar valor de saltos de línea extra pero mantener contenido completo
        value_clean = re.sub(r'\s+', ' ', value_raw).strip()
        
        parsed_data[key_normalized] = value_clean
    
    return parsed_data

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
    combined_text = ""
    for msg in all_received_messages:
        if msg.get("message"):
            combined_text += msg.get("message", "") + "\n"
    combined_text = combined_text.strip()
    if not combined_text:
        return {"status": "success", "message": ""}

    multi_match = re.search(r"Se encontro\s+(\d+)\s+resultados?\.?", combined_text, re.IGNORECASE)
    if multi_match:
        lines = combined_text.split('\n')
        cleaned_lines = []
        for line in lines:
            line = line.strip()
            if "RENIEC NOMBRES [PREMIUM]" in line or ("RENIEC NOMBRES" in line and "PREMIUM" in line):
                if "Se encontro" in line:
                    count_part = re.search(r"Se encontro\s+\d+\s+resultados?", line, re.IGNORECASE)
                    if count_part:
                        cleaned_lines.append(f"→ {count_part.group(0)}.")
                continue
            if line:
                cleaned_lines.append(line)
        formatted_text = '\n'.join(cleaned_lines).strip()
        
        # 🆕 Aplicar Parser Universal
        parsed_data = universal_parser(formatted_text)
        if parsed_data:
            return {
                "status": "success",
                "data": parsed_data,
                "raw_message": formatted_text
            }
        return {"status": "success", "message": formatted_text}
    else:
        lines = combined_text.split('\n')
        formatted_lines = [line.strip() for line in lines if line.strip() and not line.strip().startswith('[') and 'LEDER' not in line.upper()]
        formatted_text = '\n'.join(formatted_lines)
        
        # 🆕 Aplicar Parser Universal
        parsed_data = universal_parser(formatted_text)
        if parsed_data:
            return {
                "status": "success",
                "data": parsed_data,
                "raw_message": formatted_text
            }
        return {"status": "success", "message": formatted_text}

# --- 🆕 Consolidación de respuesta Azura con Parser Universal ---
def format_azura_response(all_received_messages):
    """
    Consolida múltiples mensajes de Azura y aplica el Parser Universal
    para estructurar automáticamente la respuesta.
    """
    parts = []
    for msg in all_received_messages:
        t = (msg.get("message") or "").strip()
        if t:
            parts.append(t)
    
    final_text = "\n".join(parts).strip()
    
    # 🆕 Aplicar Parser Universal
    parsed_data = universal_parser(final_text)
    
    # Si el parser detectó campos estructurados, devolver formato estructurado
    if parsed_data:
        return {
            "status": "success",
            "data": parsed_data,
            "raw_message": final_text  # Mantener texto original por si acaso
        }
    
    # Si no se detectaron campos, devolver solo el mensaje
    return {
        "status": "success",
        "message": final_text
    }

# --- Función principal LederData con Parser Universal integrado ---
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

    # Manejo especial para endpoints de nombres
    if endpoint_path in ["/dni_nombres", "/venezolanos_nombres"] or command.startswith(("/nm", "/nmv")):
        return format_nm_response(all_received_messages)

    # 🆕 Aplicar Parser Universal a la respuesta consolidada de LederData
    combined_text = ""
    for msg in all_received_messages:
        if msg.get("message"):
            combined_text += msg.get("message", "") + "\n"
    
    combined_text = combined_text.strip()
    
    # Intentar parsear automáticamente
    parsed_data = universal_parser(combined_text)
    
    # Recopilar URLs de archivos
    urls = []
    for msg in all_received_messages:
        urls.extend(msg.get("urls", []))
    
    # Construir respuesta final con Parser Universal
    final_fields = {}
    for msg in all_received_messages:
        for k, v in msg.get("fields", {}).items():
            if v and not final_fields.get(k):
                final_fields[k] = v
    
    # Combinar campos extraídos manualmente + parser universal
    if parsed_data:
        final_fields.update(parsed_data)
    
    if urls:
        final_fields["urls"] = urls
    
    # 🆕 Retornar estructura limpia y completa
    return {
        "status": "success",
        "data": final_fields,
        "raw_message": combined_text  # Mantener mensaje original completo
    }

def run_telegram_command(command: str, consulta_id: str = None, endpoint_path: str = None):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(send_telegram_command(command, consulta_id, endpoint_path))
    finally:
        loop.close()

# --- 🆕 Envío a AZURA con Parser Universal - CORREGIDO PARA ENVÍO ÚNICO ---
async def send_azura_command(command: str, endpoint_path: str = None):
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
        stop_collecting = asyncio.Event()
        last_message_time = [time.time()]

        @client.on(events.NewMessage(incoming=True))
        async def azura_handler(event):
            try:
                entity = await client.get_entity(AZURA_BOT_ID)
                if event.sender_id != entity.id:
                    return

                last_message_time[0] = time.time()
                raw_text = event.raw_text or ""

                all_received_messages.append({
                    "message": raw_text,
                    "event_message": event.message
                })
                
                # Marcar que ya recibimos respuesta para detener la espera
                if raw_text:
                    stop_collecting.set()
                    
            except Exception as e:
                print(f"Error en azura handler: {e}")

        # 🔹 ENVÍO ÚNICO del comando (solo una vez)
        print(f"Enviando comando a Azura: {command}")
        await client.send_message(AZURA_BOT_ID, command)

        start_time = time.time()

        # 🔹 ESPERA EXACTA de 35 segundos o hasta que llegue respuesta
        while (time.time() - start_time) < AZURA_TIMEOUT:
            if stop_collecting.is_set():
                print("Respuesta recibida de Azura, deteniendo espera...")
                break
            
            # Si no hay respuesta después de 35 segundos, salir
            if (time.time() - start_time) >= AZURA_TIMEOUT:
                print("Timeout de 35 segundos alcanzado sin respuesta")
                break
                
            await asyncio.sleep(0.5)

        client.remove_event_handler(azura_handler)

        # 🔹 Si no hay respuesta después de 35 segundos
        if not all_received_messages:
            return {"status": "error", "message": "No se encontró resultado en la API base"}

        # 🆕 Aplicar Parser Universal en la consolidación
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

# --- HELPER DE COMANDOS (LederData actual) ---
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
        or request_args.get("razon_social")  # 🆕 Agregado para SUNAT RAZÓN SOCIAL
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
        "sbs": f"/sbs {p}",
        "sunr": f"/sunr {p}"  # 🆕 Agregado para SUNAT RAZÓN SOCIAL
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

    # --- 🆕 Rutas Azura con Parser Universal ---
    if endpoint.startswith("azura_"):
        az_cmd_name = endpoint.replace("azura_", "", 1).strip()
        if not az_cmd_name:
            return jsonify({"status": "error", "message": "Comando Azura inválido."}), 400

        p = request.args.get("dni") or request.args.get("query") or request.args.get("param")
        if not p:
            return jsonify({"status": "error", "message": "Parámetro faltante"}), 400

        command = f"/{az_cmd_name} {p}"

        result = run_azura_command(command, endpoint_path=f"/{endpoint}")
        return jsonify(result)

    # --- 🆕 Lógica especial para SUNAT RAZÓN SOCIAL ---
    if endpoint == "sunr":
        razon_social = request.args.get("razon_social") or request.args.get("query")
        if not razon_social:
            return jsonify({"status": "error", "message": "Parámetro faltante"}), 400
        
        # Validar mínimo 3 caracteres
        if len(razon_social.strip()) < 3:
            return jsonify({"status": "error", "message": "Por favor, usa el formato correcto. [‼️]"}), 400
        
        # Validar que no sea numérico
        if razon_social.strip().isdigit():
            return jsonify({"status": "error", "message": "Por favor, usa el formato correcto. [‼️]"}), 400
        
        # Validar que no contenga caracteres especiales como |
        if "|" in razon_social:
            return jsonify({"status": "error", "message": "Por favor, usa el formato correcto. [‼️]"}), 400
        
        # Formatear comando para el bot
        command = f"/sunr {razon_social.strip()}"
        
        # Ejecutar comando con LederData
        result = run_telegram_command(command, endpoint_path=f"/{endpoint}")
        return jsonify(result)

    # --- Rutas LederData con Parser Universal ---
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
            "primary_blocked_until": bot_fail_tracker.get(LEDERDATA_BOT_ID).isoformat() if primary_blocked and bot_fail_tracker.get(LEDERDATA_BOT_ID) else None,
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
