# Cassandra/manager.py
import uuid
from datetime import datetime

# --- Funciones de Ayuda ---
def to_uuid(id_str):
    # Genera siempre el mismo UUID para el mismo string (ej. 'inc_0001')
    return uuid.uuid5(uuid.NAMESPACE_DNS, id_str)

# --- Requerimientos ---
def register_message(session, ticket_id_str, autor_id_str, contenido):
    query = """
        INSERT INTO mensajes_ticket (ticket_id, fecha_evento, autor, contenido)
        VALUES (%s, now(), %s, %s)
    """
    session.execute(query, (to_uuid(ticket_id_str), to_uuid(autor_id_str), contenido))

def get_chat_history(session, ticket_id_str):
    query = "SELECT * FROM mensajes_ticket WHERE ticket_id = %s"
    rows = session.execute(query, [to_uuid(ticket_id_str)])
    return [{"fecha": str(r.fecha_evento), "autor": str(r.autor), "mensaje": r.contenido} for r in rows]

def update_ticket_status(session, ticket_id_str, new_status, operator_id_str, detalles):
    query = """
        INSERT INTO historial_estados (ticket_id, fecha_cambio, estado, operador_que_actualizo, detalles_actualizacion)
        VALUES (%s, now(), %s, %s, %s)
    """
    session.execute(query, (to_uuid(ticket_id_str), new_status, to_uuid(operator_id_str), detalles))

def get_status_history(session, ticket_id_str):
    query = "SELECT * FROM historial_estados WHERE ticket_id = %s"
    rows = session.execute(query, [to_uuid(ticket_id_str)])
    return [{"fecha": str(r.fecha_cambio), "estado": r.estado, "operador": str(r.operador_que_actualizo)} for r in rows]

def get_current_status(session, ticket_id_str):
    query = "SELECT estado FROM historial_estados WHERE ticket_id = %s LIMIT 1"
    row = session.execute(query, [to_uuid(ticket_id_str)]).one()
    return row.estado if row else "Desconocido"

def register_participation(session, ticket_id_str, agent_id_str, action_detail):
    query = """
        INSERT INTO participacion_agentes (ticket_id, agente_id, nombre_operador, fecha_ultima_accion, detalle_accion)
        VALUES (%s, %s, %s, toTimestamp(now()), %s)
    """
    session.execute(query, (to_uuid(ticket_id_str), to_uuid(agent_id_str), "Agente " + agent_id_str, action_detail))

def get_participants(session, ticket_id_str):
    query = "SELECT agente_id, nombre_operador, detalle_accion FROM participacion_agentes WHERE ticket_id = %s"
    rows = session.execute(query, [to_uuid(ticket_id_str)])
    return [{"agente": str(r.agente_id), "accion": r.detalle_accion} for r in rows]

def get_daily_audit(session, date_str=None):
    if not date_str: date_str = datetime.now().date()
    else: date_str = datetime.strptime(date_str, "%Y-%m-%d").date()
    query = "SELECT * FROM bitacora_actividades WHERE dia = %s"
    rows = session.execute(query, [date_str])
    return [r for r in rows]