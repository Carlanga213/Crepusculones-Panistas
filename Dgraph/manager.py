import pydgraph
import json
import datetime

# --- Definición del Esquema ---
def set_schema(client):
    """
    Define el esquema y los índices según el diseño del PDF (Páginas 10-13).
    """
    schema = """
    # [cite_start]Índices [cite: 194-204]
    customer_id: string @index(hash) .
    agent_id: string @index(hash) .
    incident_id: string @index(hash) .
    org_id: string @index(hash) .
    status: string @index(hash) .
    name: string @index(trigram) .

    # [cite_start]Bordes y Direcciones [cite: 205-214]
    assigned_to: uid @reverse @count .
    reported_by: uid @reverse .
    belongs_to_org: uid @reverse .
    reports_to: uid @reverse .
    related_to: uid @reverse .
    escalated_from: uid @reverse . 

    # [cite_start]Tipos [cite: 170-191]
    type Customer {
        customer_id
        name
        belongs_to_org
    }
    type Agent {
        agent_id
        name
        reports_to
    }
    type Incident {
        incident_id
        status
        reported_by
        assigned_to
        related_to
        escalated_from
    }
    type Organization {
        org_id
        name
    }
    """
    op = pydgraph.Operation(schema=schema)
    client.alter(op)
    print(">> Esquema Dgraph actualizado correctamente.")

def get_uid(client, field, value):
    """Busca el UID de un nodo dado su identificador único."""
    query = f"""
    {{
        get(func: eq({field}, "{value}")) {{
            uid
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    data = json.loads(res.json)
    if data['get']:
        return data['get'][0]['uid']
    return None


def assign_incident(client, incident_id, agent_id):
    inc_uid = get_uid(client, "incident_id", incident_id)
    ag_uid = get_uid(client, "agent_id", agent_id)
    
    if not inc_uid or not ag_uid: return False

    timestamp = datetime.datetime.now().isoformat()
    nquads = f'<{inc_uid}> <assigned_to> <{ag_uid}> (timestamp="{timestamp}", action="initial_assignment") .'
    
    txn = client.txn()
    try:
        txn.mutate(pydgraph.Mutation(set_nquads=nquads.encode('utf-8')))
        txn.commit()
        return True
    finally:
        txn.discard()

def link_incident_customer(client, incident_id, customer_id):
    inc_uid = get_uid(client, "incident_id", incident_id)
    cust_uid = get_uid(client, "customer_id", customer_id)

    if not inc_uid or not cust_uid: return False

    txn = client.txn()
    try:
        nquads = f'<{inc_uid}> <reported_by> <{cust_uid}> .'
        txn.mutate(pydgraph.Mutation(set_nquads=nquads.encode('utf-8')))
        txn.commit()
        return True
    finally:
        txn.discard()

def set_agent_hierarchy(client, sub_id, sup_id):
    sub_uid = get_uid(client, "agent_id", sub_id)
    sup_uid = get_uid(client, "agent_id", sup_id)

    if not sub_uid or not sup_uid: return False

    txn = client.txn()
    try:
        nquads = f'<{sub_uid}> <reports_to> <{sup_uid}> .'
        txn.mutate(pydgraph.Mutation(set_nquads=nquads.encode('utf-8')))
        txn.commit()
        return True
    finally:
        txn.discard()

def escalate_incident(client, incident_id, old_agent_id, new_agent_id):
    inc_uid = get_uid(client, "incident_id", incident_id)
    old_uid = get_uid(client, "agent_id", old_agent_id)
    new_uid = get_uid(client, "agent_id", new_agent_id)

    if not (inc_uid and old_uid and new_uid): return False

    timestamp = datetime.datetime.now().isoformat()
    nquads = f"""
    <{inc_uid}> <escalated_from> <{old_uid}> (timestamp="{timestamp}") .
    <{inc_uid}> <assigned_to> <{new_uid}> (timestamp="{timestamp}", action="escalation") .
    """
    txn = client.txn()
    try:
        txn.mutate(pydgraph.Mutation(set_nquads=nquads.encode('utf-8')))
        txn.commit()
        return True
    finally:
        txn.discard()

def link_related_incidents(client, id_a, id_b):
    uid_a = get_uid(client, "incident_id", id_a)
    uid_b = get_uid(client, "incident_id", id_b)

    if not (uid_a and uid_b): return False

    txn = client.txn()
    try:
        nquads = f"""
        <{uid_a}> <related_to> <{uid_b}> .
        <{uid_b}> <related_to> <{uid_a}> .
        """
        txn.mutate(pydgraph.Mutation(set_nquads=nquads.encode('utf-8')))
        txn.commit()
        return True
    finally:
        txn.discard()

def get_agent_workload(client, agent_id):
    query = f"""
    {{
        workload(func: eq(agent_id, "{agent_id}")) {{
            name
            assigned_tickets: ~assigned_to @filter(eq(status, "abierto") OR eq(status, "en_progreso")) {{
                incident_id
                status
                title
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return json.loads(res.json)

def get_customer_history(client, customer_id):
    query = f"""
    {{
        history(func: eq(customer_id, "{customer_id}")) {{
            name
            tickets: ~reported_by {{
                incident_id
                status
                fecha_creacion
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return json.loads(res.json)

def set_customer_org(client, customer_id, org_id):
    cust_uid = get_uid(client, "customer_id", customer_id)
    org_uid = get_uid(client, "org_id", org_id)

    if not (cust_uid and org_uid): return False

    txn = client.txn()
    try:
        nquads = f'<{cust_uid}> <belongs_to_org> <{org_uid}> .'
        txn.mutate(pydgraph.Mutation(set_nquads=nquads.encode('utf-8')))
        txn.commit()
        return True
    finally:
        txn.discard()

def get_org_incidents(client, org_id):
    query = f"""
    {{
        org_incidents(func: eq(org_id, "{org_id}")) {{
            name
            members: ~belongs_to_org {{
                name
                tickets: ~reported_by {{
                    incident_id
                    status
                }}
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return json.loads(res.json)

def get_traceability(client, incident_id):
    query = f"""
    {{
        traceability(func: eq(incident_id, "{incident_id}")) {{
            incident_id
            assignments: assigned_to @facets(timestamp, action) {{
                name
                agent_id
            }}
            escalations: escalated_from @facets(timestamp) {{
                name
                agent_id
            }}
        }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    return json.loads(res.json)