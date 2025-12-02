import csv
import random
import os

# Configuración
NUM_RECORDS = 150  # Generaremos 150 registros para asegurar el mínimo de 100
DATA_DIR = 'data'
NUM_INCIDENTS = 200 
NUM_AGENTS = 20     

# Listas de base
STATUS_LIST = ["abierto", "en_progreso", "resuelto", "cerrado", "en_espera"]
CHAT_MESSAGES = [
    "Hola, ¿en qué puedo ayudarle?",
    "Estoy revisando los logs del servidor.",
    "El problema persiste después del reinicio.",
    "Necesito acceso remoto a su equipo.",
    "Confirmado, es un error de capa 8.",
    "La actualización se ha completado.",
    "Por favor envíe una captura de pantalla.",
    "Escalando el ticket al departamento de redes.",
    "El usuario reporta que ya funciona.",
    "Cerrando ticket por inactividad.",
    "¿Podría intentar limpiar la caché?",
    "Recibido, asignando un técnico.",
    "El servicio se restablecerá en 10 minutos.",
    "Gracias por su paciencia.",
    "Error 500 detectado en el balanceador."
]

STATUS_DETAILS = [
    "Revisión inicial del caso",
    "Cliente contactado por teléfono",
    "Esperando respuesta del proveedor",
    "Solución aplicada exitosamente",
    "Falsa alarma, sistema operativo",
    "Mantenimiento programado",
    "Ticket creado automáticamente",
    "Validación con el usuario final",
    "Reasignado por carga de trabajo",
    "Cierre administrativo"
]

def generate_ids(prefix, count, padding):
    return [f"{prefix}_{i+1:0{padding}d}" for i in range(count)]

def generate_cassandra_csvs():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    inc_ids = generate_ids("inc", NUM_INCIDENTS, 4) # inc_0001 ...
    agent_ids = generate_ids("agent", NUM_AGENTS, 3) # agent_001 ...

    print(f"Generando {NUM_RECORDS} registros para Cassandra...")

    # --- 1. Generar Chat History ---
    chat_file = os.path.join(DATA_DIR, 'chat_history.csv')
    with open(chat_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['ticket_id', 'agent_id', 'message'])
        
        for _ in range(NUM_RECORDS):
            tid = random.choice(inc_ids)
            aid = random.choice(agent_ids)
            msg = random.choice(CHAT_MESSAGES)
            writer.writerow([tid, aid, msg])
            
    print(f" -> {chat_file} creado.")

    # --- 2. Generar Status History ---
    status_file = os.path.join(DATA_DIR, 'status_history.csv')
    with open(status_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['ticket_id', 'status', 'agent_id', 'details'])
        
        for _ in range(NUM_RECORDS):
            tid = random.choice(inc_ids)
            st = random.choice(STATUS_LIST)
            aid = random.choice(agent_ids)
            det = random.choice(STATUS_DETAILS)
            writer.writerow([tid, st, aid, det])

    print(f" -> {status_file} creado.")
    print("\n¡Datos generados! Ahora ejecuta 'python populate.py' para cargarlos.")

if __name__ == '__main__':
    generate_cassandra_csvs()