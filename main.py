import os
import re
import asyncio
import traceback
import time
import json
import mimetypes
from datetime import datetime, timezone, timedelta
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

LEDERDATA_BOT_ID = "@LEDERDATA_OFC_BOT" 
LEDERDATA_BACKUP_BOT_ID = "@lederdata_publico_bot"
ALL_BOT_IDS = [LEDERDATA_BOT_ID, LEDERDATA_BACKUP_BOT_ID]

TIMEOUT_PRIMARY = 35  
TIMEOUT_BACKUP = 50   
BOT_BLOCK_HOURS = 3   # Cambiado a 3 horas según especificación

# --- Trackeo de Fallos de Bots ---
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
    # Si ya pasó el tiempo de bloqueo, limpiar el registro
    bot_fail_tracker.pop(bot_id, None)
    return False

def record_bot_failure(bot_id: str):
    """Registra un fallo de bot (no respuesta)"""
    bot_fail_tracker[bot_id] = datetime.now()

# --- Lógica de Limpieza y Extracción de Datos ---
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
    
    # Detectar mensaje de "no se encontró información"
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
        return json.dumps({"status": "success", "message": ""}, ensure_ascii=False)
    
    multi_match = re.search(r"Se encontro\s+(\d+)\s+resultados?\.?", combined_text, re.IGNORECASE)
    if multi_match:
        lines = combined_text.split('\n')
        cleaned_lines = []
        for line in lines:
            line = line.strip()
            if "RENIEC NOMBRES [PREMIUM]" in line or "RENIEC NOMBRES" in line and "PREMIUM" in line:
                if "Se encontro" in line:
                    count_part = re.search(r"Se encontro\s+\d+\s+resultados?", line, re.IGNORECASE)
                    if count_part:
                        cleaned_lines.append(f"→ {count_part.group(0)}.")
                continue
            if line:
                cleaned_lines.append(line)
        formatted_text = '\n'.join(cleaned_lines).strip()
        return json.dumps({"status": "success", "message": formatted_text}, ensure_ascii=False)
    else:
        lines = combined_text.split('\n')
        formatted_lines = [line.strip() for line in lines if line.strip() and not line.strip().startswith('[') and not 'LEDER' in line.upper()]
        return json.dumps({"status": "success", "message": '\n'.join(formatted_lines)}, ensure_ascii=False)

# --- Función Principal para Envío de Comandos CORREGIDA ---
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
        
        # Verificar estado de bots
        primary_blocked = is_bot_blocked(LEDERDATA_BOT_ID)
        
        # Decidir qué bot usar primero
        bot_to_use = None
        use_backup = False
        
        if not primary_blocked:
            bot_to_use = LEDERDATA_BOT_ID
            use_backup = False
        else:
            # Si el bot principal está bloqueado, usar directamente el backup
            bot_to_use = LEDERDATA_BACKUP_BOT_ID
            use_backup = True
        
        all_received_messages = []
        all_files_data = []
        stop_collecting = asyncio.Event()
        last_message_time = [time.time()]
        
        # Handler temporal para capturar respuestas
        @client.on(events.NewMessage(incoming=True))
        async def temp_handler(event):
            if stop_collecting.is_set():
                return
            
            try:
                # Verificar que el mensaje viene del bot que estamos usando
                entity = await client.get_entity(bot_to_use)
                if event.sender_id != entity.id:
                    return
                
                last_message_time[0] = time.time()
                raw_text = event.raw_text or ""
                
                # Limpiar y extraer datos según el tipo de consulta
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
                
                # Detectar anti-spam inmediatamente
                if "ANTI-SPAM" in raw_text and "INTENTA DESPUÉS DE 10 SEGUNDOS" in raw_text:
                    if not use_backup:
                        # Anti-spam en bot principal, marcar para usar backup
                        stop_collecting.set()
                    return
                
                # Detectar "no se encontró información"
                if re.search(r"\[⚠️\]\s*no se encontro información", raw_text, re.IGNORECASE):
                    stop_collecting.set()
                    return
                    
            except Exception as e:
                print(f"Error en handler: {e}")

        # Enviar comando UNA SOLA VEZ al bot seleccionado
        await client.send_message(bot_to_use, command)
        
        # Esperar respuestas
        start_time = time.time()
        timeout_val = TIMEOUT_PRIMARY if bot_to_use == LEDERDATA_BOT_ID else TIMEOUT_BACKUP
        
        while (time.time() - start_time) < timeout_val:
            if stop_collecting.is_set():
                break
            
            # Si no hay respuestas después de un tiempo, esperar
            if all_received_messages and (time.time() - last_message_time[0]) > 4.5:
                break
                
            await asyncio.sleep(0.5)
        
        # Remover handler
        client.remove_event_handler(temp_handler)
        
        # Analizar respuestas recibidas
        if not all_received_messages:
            # No hubo respuesta - registrar fallo solo si es el bot principal
            if bot_to_use == LEDERDATA_BOT_ID:
                record_bot_failure(bot_to_use)
            
            # Si era el bot principal y no recibimos respuesta, intentar con backup
            if not use_backup:
                # Esperar 5 segundos antes de intentar con backup
                await asyncio.sleep(5)
                
                # Configurar para usar backup
                bot_to_use = LEDERDATA_BACKUP_BOT_ID
                use_backup = True
                
                # Crear nuevo handler para backup
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
                        
                        # Detectar "no se encontró información" en backup
                        if re.search(r"\[⚠️\]\s*no se encontro información", raw_text, re.IGNORECASE):
                            stop_collecting.set()
                            return
                            
                    except Exception as e:
                        print(f"Error en backup handler: {e}")
                
                # Enviar comando UNA SOLA VEZ al backup
                await client.send_message(bot_to_use, command)
                
                # Reiniciar temporizador
                start_time = time.time()
                last_message_time[0] = time.time()
                
                # Esperar respuesta del backup
                while (time.time() - start_time) < TIMEOUT_BACKUP:
                    if stop_collecting.is_set():
                        break
                    
                    if all_received_messages and (time.time() - last_message_time[0]) > 4.5:
                        break
                        
                    await asyncio.sleep(0.5)
                
                client.remove_event_handler(backup_handler)
                
                if not all_received_messages:
                    # NOTA: No registramos fallo para el bot de respaldo
                    raise Exception("No se obtuvo respuesta de ningún bot.")
            else:
                raise Exception("No se obtuvo respuesta del bot.")
        
        # Procesar mensajes recibidos
        return await process_bot_response(client, all_received_messages, command, endpoint_path)
                
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        if client:
            await client.disconnect()

async def process_bot_response(client, all_received_messages, command, endpoint_path):
    # Verificar formato incorrecto
    if any("formato correcto" in m["message"].lower() for m in all_received_messages):
        return {"status": "error", "message": "Formato incorrecto."}
    
    # Verificar "no se encontró información"
    if any(m["fields"].get("not_found") for m in all_received_messages):
        return {"status": "error", "message": "No se encontraron resultados."}
    
    # Verificar anti-spam en bot principal (ya manejado en la lógica principal)
    
    # Descargar archivos adjuntos
    for msg in all_received_messages:
        event_msg = msg.get("event_message")
        if event_msg and getattr(event_msg, "media", None):
            try:
                # Determinar extensión basada en tipo de archivo
                ext = ".pdf" if "pdf" in str(event_msg.media).lower() else ".jpg"
                fname = f"{int(time.time())}_{event_msg.id}{ext}"
                path = await client.download_media(event_msg, file=os.path.join(DOWNLOAD_DIR, fname))
                if path:
                    msg["urls"].append({"url": f"{PUBLIC_URL}/files/{fname}", "type": "document"})
            except Exception as e:
                print(f"Error descargando archivo: {e}")

    # Formatear respuesta según tipo de consulta
    if endpoint_path in ["/dni_nombres", "/venezolanos_nombres"] or command.startswith(("/nm", "/nmv")):
        return json.loads(format_nm_response(all_received_messages))

    # Combinar campos de todos los mensajes
    final_fields = {}
    urls = []
    for msg in all_received_messages:
        for k, v in msg["fields"].items():
            if v and not final_fields.get(k):
                final_fields[k] = v
        urls.extend(msg["urls"])
    
    final_fields["urls"] = urls
    return final_fields

def run_telegram_command(command: str, consulta_id: str = None, endpoint_path: str = None):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try: 
        return loop.run_until_complete(send_telegram_command(command, consulta_id, endpoint_path))
    finally: 
        loop.close()

# --- HELPER DE COMANDOS ACTUALIZADO ---
def get_command_and_param(path, request_args):
    cmd = path.lstrip('/')
    p = request_args.get("dni") or request_args.get("query") or request_args.get("pasaporte") or request_args.get("cedula") or request_args.get("direccion") or request_args.get("carnet_extranjeria") or request_args.get("cedula_identidad") or request_args.get("placa") or request_args.get("serie_armamento") or request_args.get("clave_denuncia")
    
    # Mapeo según comandos
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
    if endpoint in ["files", "health", "status", "dni_nombres", "venezolanos_nombres"]: 
        return handle_special(endpoint)
    
    command, error = get_command_and_param(endpoint, request.args)
    if error: 
        return jsonify({"status": "error", "message": error}), 400
    
    result = run_telegram_command(command, endpoint_path=f"/{endpoint}")
    return jsonify(result)

def handle_special(endpoint):
    if endpoint == "status":
        primary_blocked = is_bot_blocked(LEDERDATA_BOT_ID)
        # El bot de respaldo nunca se bloquea, siempre está disponible
        backup_blocked = False
        return jsonify({
            "status": "online", 
            "bots": ALL_BOT_IDS,
            "primary_blocked": primary_blocked,
            "backup_blocked": backup_blocked,
            "primary_blocked_until": bot_fail_tracker.get(LEDERDATA_BOT_ID) if primary_blocked else None,
            "backup_blocked_until": None  # Siempre None para el bot de respaldo
        })
    
    if endpoint == "health":
        return jsonify({"status": "healthy"})
    
    # Lógica para NM / NMV (DNI NOMBRES)
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
