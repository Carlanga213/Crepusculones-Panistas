import uuid
import csv
import os
from datetime import datetime

AGENT_NAMES = {}

#convierte id de string a UUID para cassandra
def to_uuid(id_str):
    return uuid.uuid5(uuid.NAMESPACE_DNS, id_str)

#carga nombres a memoria
def load_agent_names():
    global AGENT_NAMES
    if AGENT_NAMES: return
    path = 'data/agents.csv'
    if not os.path.exists(path): return
    try:
        with open(path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                AGENT_NAMES[str(to_uuid(row['agent_id']))] = row['name']
    except: pass


#Devuelve nombre de agente 
def get_agent_name(uuid_val):
    load_agent_names()
    return AGENT_NAMES.get(str(uuid_val), str(uuid_val))



#1 registra nuevo mensaje 
def register_message(session, ticket_id_str, autor_id_str, contenido):

    query = "INSERT INTO mensajes_ticket (ticket_id, fecha_evento, autor, contenido) VALUES (%s, now(), %s, %s)"
    session.execute(query, (to_uuid(ticket_id_str), to_uuid(autor_id_str), contenido))
    # registra actividad de operador
    log_activity(session, autor_id_str, ticket_id_str, "Mensaje Chat", "Enviado")



#2 devuelve chat completo
def get_chat_history(session, ticket_id_str):

    query = "SELECT * FROM mensajes_ticket WHERE ticket_id = %s"
    rows = session.execute(query, [to_uuid(ticket_id_str)])

    return [{"fecha": r.fecha_evento, "autor": get_agent_name(r.autor), "mensaje": r.contenido} for r in rows]



#3 actualiza estado ticket
def update_ticket_status(session, ticket_id_str, new_status, operator_id_str, detalles):

    query = "INSERT INTO historial_estados (ticket_id, fecha_cambio, estado, operador_que_actualizo, detalles_actualizacion) VALUES (%s, now(), %s, %s, %s)"
    session.execute(query, (to_uuid(ticket_id_str), new_status, to_uuid(operator_id_str), detalles))

    log_activity(session, operator_id_str, ticket_id_str, "Cambio Estado", f"A {new_status}")
    #actualiza metrica de desempeno
    update_performance(session, operator_id_str, atendido=True, cerrado=(new_status=='cerrado'))




#4 muestra todos los cambios de estado de un ticket
def get_status_history(session, ticket_id_str):

    query = "SELECT * FROM historial_estados WHERE ticket_id = %s"
    rows = session.execute(query, [to_uuid(ticket_id_str)])
    return [{"fecha": r.fecha_cambio, "estado": r.estado, "operador": get_agent_name(r.operador_que_actualizo)} for r in rows]




#5 estado actual de ticket
def get_current_status(session, ticket_id_str):

    row = session.execute("SELECT estado FROM historial_estados WHERE ticket_id = %s LIMIT 1", [to_uuid(ticket_id_str)]).one()
    return row.estado if row else "Desconocido"




#6 registra participacion de operador en ticket
def register_participation(session, ticket_id_str, agent_id_str, action_detail):
    
    load_agent_names()
    real_name = AGENT_NAMES.get(str(to_uuid(agent_id_str)), f"Agente {agent_id_str}")
    query = "INSERT INTO participacion_agentes (ticket_id, agente_id, nombre_operador, fecha_ultima_accion, detalle_accion) VALUES (%s, %s, %s, toTimestamp(now()), %s)"
    session.execute(query, (to_uuid(ticket_id_str), to_uuid(agent_id_str), real_name, action_detail))




#7 muestra los operadores que participaron en el ticket
def get_participants(session, ticket_id_str):

    rows = session.execute("SELECT agente_id, nombre_operador, detalle_accion FROM participacion_agentes WHERE ticket_id = %s", [to_uuid(ticket_id_str)])
    return [{"agente": r.nombre_operador if r.nombre_operador else get_agent_name(r.agente_id), "accion": r.detalle_accion} for r in rows]




#8 registra actividad de operador
def log_activity(session, operator_id_str, ticket_id_str, actividad, detalle):

    if not operator_id_str: return
    op_uuid = to_uuid(operator_id_str)
    tic_uuid = to_uuid(ticket_id_str) if ticket_id_str else None
    today = datetime.now().date()
    query = "INSERT INTO bitacora_actividades (dia, hora, operador, ticket_id_afectado, actividad, detalle_accion) VALUES (%s, now(), %s, %s, %s, %s)"
    session.execute(query, (today, op_uuid, tic_uuid, actividad, detalle))





#9 muestra el historial de actividad de todos los operadores
def get_daily_audit(session, date_str=None):

    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else datetime.now().date()
    rows = session.execute("SELECT * FROM bitacora_actividades WHERE dia = %s", [date_obj])
    return [{"hora": r.hora, "operador": get_agent_name(r.operador), "actividad": r.actividad, "detalle": r.detalle_accion} for r in rows]



#funcion para actualizar metricas dependiendo lo que se le de a el query
def update_performance(session, operator_id_str, atendido=False, cerrado=False):
    op_uuid = to_uuid(operator_id_str)
    today = datetime.now().date()
    if atendido: session.execute("UPDATE rendimiento_operador SET tickets_atendidos = tickets_atendidos + 1 WHERE fecha = %s AND operador = %s", (today, op_uuid))
    if cerrado: session.execute("UPDATE rendimiento_operador SET tickets_cerrados = tickets_cerrados + 1 WHERE fecha = %s AND operador = %s", (today, op_uuid))


#10 obtiene metricas de operadores dependiendo la fecha
def get_daily_performance(session, date_str=None):

    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else datetime.now().date()
    rows = session.execute("SELECT * FROM rendimiento_operador WHERE fecha = %s", [date_obj])
    return [{"operador": get_agent_name(r.operador), "atendidos": r.tickets_atendidos, "cerrados": r.tickets_cerrados} for r in rows]