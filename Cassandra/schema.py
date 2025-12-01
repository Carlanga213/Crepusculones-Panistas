# Cassandra/schema.py

def create_schema(session):
    """
    Crea el Keyspace y las Tablas requeridas según el PDF.
    """
    # 1. Crear Keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS helpdesk_system 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('helpdesk_system')

    # 2. Tabla: Mensajes de Chat por Ticket (Req 1, 2)
    # PDF Pag 13: ticket_id (PK), fecha_evento (CK), autor, contenido
    session.execute("""
        CREATE TABLE IF NOT EXISTS mensajes_ticket (
            ticket_id UUID,
            fecha_evento TIMEUUID,
            autor UUID,
            contenido TEXT,
            PRIMARY KEY (ticket_id, fecha_evento)
        ) WITH CLUSTERING ORDER BY (fecha_evento ASC);
    """)

    # 3. Tabla: Historial de Estados (Req 3, 4, 5)
    # PDF Pag 14: ticket_id (PK), fecha_cambio (CK), estado, operador..., detalles...
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

    # 4. Tabla: Rendimiento de Operador (Req 10)
    # PDF Pag 15: date (PK), operador (PK), contadores. 
    # NOTA: Las tablas con COUNTER deben ser dedicadas exclusivamente a contadores + PKs.
    session.execute("""
        CREATE TABLE IF NOT EXISTS rendimiento_operador (
            fecha DATE,
            operador UUID,
            tickets_atendidos COUNTER,
            tickets_cerrados COUNTER,
            PRIMARY KEY (fecha, operador)
        );
    """)

    # 5. Tabla: Actividades Diarias de Operador (Req 8, 9)
    # PDF Pag 16: dia (PK), hora (CK), operador, ticket_id_afectado, actividad, detalle
    # Ajuste: Para filtrar por día Y operador eficientemente, 'operador' debería ser parte de la PK o usar un índice.
    # Siguiendo el PDF estricto (dia PK, hora CK), filtraremos por día.
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

    # 6. Tabla: Agentes que participaron en el ticket (Req 6, 7)
    # PDF Pag 17: ticket_id (PK), agente_id (CK), nombre..., fecha..., detalle...
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

    print("✅ Esquema de Cassandra creado correctamente.")