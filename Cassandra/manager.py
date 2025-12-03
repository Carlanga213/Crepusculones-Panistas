from datetime import datetime


# Requerimientos

# 1 nuevo mensaje
def register_message(session, ticket_id, autor_id, contenido):
    query = """
        INSERT INTO mensajes_ticket (ticket_id, fecha_evento, autor, contenido) 
        VALUES (%s, now(), %s, %s)
    """
    session.execute(query, (ticket_id, autor_id, contenido))
    # registra en historial
    log_activity(session, autor_id, ticket_id, "Mensaje Chat", "Enviado")



# 2 obtener chat
def get_chat_history(session, ticket_id):
    query = "SELECT * FROM mensajes_ticket WHERE ticket_id = %s"
    rows = session.execute(query, [ticket_id])
    return [
        {
            "fecha": r.fecha_evento,
            "autor": r.autor,
            "mensaje": r.contenido
        }
        for r in rows
    ]



#3 cambia estado de ticket
def update_ticket_status(session, ticket_id, new_status, operator_id, detalles):
    query = """
        INSERT INTO historial_estados (ticket_id, fecha_cambio, estado, operador_que_actualizo, detalles_actualizacion) 
        VALUES (%s, now(), %s, %s, %s)
    """
    session.execute(query, (ticket_id, new_status, operator_id, detalles))
    
    # Actualiza bitácora y contadores de rendimiento
    log_activity(session, operator_id, ticket_id, "Cambio Estado", f"A {new_status}")
    update_performance(session, operator_id, atendido=True, cerrado=(new_status == 'cerrado'))





# 4 muestra hisotiral de cambios de estado
def get_status_history(session, ticket_id):
    query = "SELECT * FROM historial_estados WHERE ticket_id = %s"
    rows = session.execute(query, [ticket_id])
    return [
        {
            "fecha": r.fecha_cambio,
            "estado": r.estado,
            "operador": r.operador_que_actualizo,
            "detalles": r.detalles_actualizacion
        }
        for r in rows
    ]





# 5 Estado actual de ticket
def get_current_status(session, ticket_id):
    query = "SELECT estado FROM historial_estados WHERE ticket_id = %s LIMIT 1"
    row = session.execute(query, [ticket_id]).one()

    return row.estado if row else "Desconocido"





# 6 registra que agente trabajo en un ticket
def register_participation(session, ticket_id, agent_id, action_detail):
    query = """
        INSERT INTO participacion_agentes (ticket_id, agente_id, nombre_operador, fecha_ultima_accion, detalle_accion) 
        VALUES (%s, %s, %s, toTimestamp(now()), %s)
    """
    session.execute(query, (ticket_id, agent_id, str(agent_id), action_detail))



# 7 Muestra todos los agentes que han participado en el ticket
def get_participants(session, ticket_id):
    query = "SELECT agente_id, nombre_operador, detalle_accion FROM participacion_agentes WHERE ticket_id = %s"
    rows = session.execute(query, [ticket_id])

    return [
        {
            "agente": r.nombre_operador,
            "accion": r.detalle_accion
        }
        for r in rows
    ]



# 8 Registra actividad de operador 
def log_activity(session, operator_id, ticket_id, actividad, detalle):
    
    if not operator_id: 
        return
        
    query = """
        INSERT INTO bitacora_actividades (dia, hora, operador, ticket_id_afectado, actividad, detalle_accion) 
        VALUES (%s, now(), %s, %s, %s, %s)
    """
    today = datetime.now().date()
    session.execute(query, (today, operator_id, ticket_id, actividad, detalle))





# 9 Muestra todas las actividades del dia
def get_daily_audit(session, date_str=None):
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else datetime.now().date()

    query = "SELECT * FROM bitacora_actividades WHERE dia = %s"
    rows = session.execute(query, [target_date])

    return [
        {
            "hora": r.hora,
            "operador": r.operador,
            "actividad": r.actividad,
            "detalle": r.detalle_accion
        }
        for r in rows
    ]





# Actualiza estadistica de operador
def update_performance(session, operator_id, atendido=False, cerrado=False):
    today = datetime.now().date()
    
    if atendido:
        session.execute(
            "UPDATE rendimiento_operador SET tickets_atendidos = tickets_atendidos + 1 WHERE fecha = %s AND operador = %s", 
            (today, operator_id)
        )
        
    if cerrado:
        session.execute(
            "UPDATE rendimiento_operador SET tickets_cerrados = tickets_cerrados + 1 WHERE fecha = %s AND operador = %s", 
            (today, operator_id)
        )




# 10 Muestra rendimiento de operadores ( tickets cerrados por operador )
def get_daily_performance(session, date_str=None):
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else datetime.now().date()

    query = "SELECT * FROM rendimiento_operador WHERE fecha = %s"
    rows = session.execute(query, [target_date])

    return [
        {
            "operador": r.operador,
            "atendidos": r.tickets_atendidos,
            "cerrados": r.tickets_cerrados
        }
        for r in rows
    ]