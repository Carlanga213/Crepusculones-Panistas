# Cassandra/schema.py

def create_schema(session):
    """
    Crea el Keyspace y las Tablas requeridas.
    """
    # 1. Crear Keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS helpdesk_system 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('helpdesk_system')

    # 2. Tabla: Mensajes de Chat por Ticket 
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS mensajes_ticket (
            ticket_id UUID,
            fecha_evento TIMEUUID,
            autor UUID,
            contenido TEXT,
            PRIMARY KEY (ticket_id, fecha_evento)
        ) WITH CLUSTERING ORDER BY (fecha_evento ASC);
    """)

    # 3. Tabla: Historial de Estados
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS historial_estados (
            ticket_id UUID,
            fecha_cambio TIMEUUID,
            estado TEXT, -- El PDF dice UUID, pero TEXT es más práctico para estados como 'abierto'
            operador_que_actualizo UUID,
            detalles_actualizacion TEXT,
            PRIMARY KEY (ticket_id, fecha_cambio)
        ) WITH CLUSTERING ORDER BY (fecha_cambio DESC);
    """)

    # 4. Tabla: Rendimiento de Operador 

    session.execute("""
        CREATE TABLE IF NOT EXISTS rendimiento_operador (
            fecha DATE,
            operador UUID,
            tickets_atendidos COUNTER,
            tickets_cerrados COUNTER,
            PRIMARY KEY (fecha, operador)
        );
    """)

    # 5. Tabla: Actividades Diarias de Operador
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS bitacora_actividades (
            dia DATE,
            hora TIMEUUID,
            operador UUID,
            ticket_id_afectado UUID,
            actividad TEXT,
            detalle_accion TEXT,
            PRIMARY KEY (dia, hora)
        ) WITH CLUSTERING ORDER BY (hora DESC);
    """)

    # 6. Tabla: Agentes que participaron en el ticket
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS participacion_agentes (
            ticket_id UUID,
            agente_id UUID,
            nombre_operador TEXT,
            fecha_ultima_accion TIMESTAMP,
            detalle_accion TEXT,
            PRIMARY KEY (ticket_id, agente_id)
        );
    """)

    print("Esquema de Cassandra creado correctamente.")