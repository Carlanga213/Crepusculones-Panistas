# Cassandra/schema.py

def create_schema(session):
    """
    Crea el Keyspace y las Tablas usando TEXT para los IDs
    para que sean legibles (ej. 'inc_0001', 'agent_005').
    """
    # 1. Crear keyspace
    session.execute("DROP KEYSPACE IF EXISTS helpdesk_system;")
    
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS helpdesk_system 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('helpdesk_system')

    # Mensajes de chat 
    session.execute("""
        CREATE TABLE IF NOT EXISTS mensajes_ticket (
            ticket_id TEXT,
            fecha_evento TIMEUUID,
            autor TEXT,
            contenido TEXT,
            PRIMARY KEY (ticket_id, fecha_evento)
        ) WITH CLUSTERING ORDER BY (fecha_evento ASC);
    """)

    # Historial de Estados
    session.execute("""
        CREATE TABLE IF NOT EXISTS historial_estados (
            ticket_id TEXT,
            fecha_cambio TIMEUUID,
            estado TEXT,
            operador_que_actualizo TEXT,
            detalles_actualizacion TEXT,
            PRIMARY KEY (ticket_id, fecha_cambio)
        ) WITH CLUSTERING ORDER BY (fecha_cambio DESC);
    """)

    # Rendimiento de Agente
    session.execute("""
        CREATE TABLE IF NOT EXISTS rendimiento_operador (
            fecha DATE,
            operador TEXT,
            tickets_atendidos COUNTER,
            tickets_cerrados COUNTER,
            PRIMARY KEY (fecha, operador)
        );
    """)

    # Actividades Diarias de agente
    session.execute("""
        CREATE TABLE IF NOT EXISTS bitacora_actividades (
            dia DATE,
            hora TIMEUUID,
            operador TEXT,
            ticket_id_afectado TEXT,
            actividad TEXT,
            detalle_accion TEXT,
            PRIMARY KEY (dia, hora)
        ) WITH CLUSTERING ORDER BY (hora DESC);
    """)

    # Participación de agentes en ticket
    session.execute("""
        CREATE TABLE IF NOT EXISTS participacion_agentes (
            ticket_id TEXT,
            agente_id TEXT,
            nombre_operador TEXT,
            fecha_ultima_accion TIMESTAMP,
            detalle_accion TEXT,
            PRIMARY KEY (ticket_id, agente_id)
        );
    """)

    print("✅ Esquema de Cassandra actualizado a TEXT (IDs legibles).")

