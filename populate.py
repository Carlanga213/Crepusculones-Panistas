import csv
import os
import json
import uuid
import pydgraph
import Mongo.connect as connect
from Dgraph import manager as dgraph_manager
from Cassandra import schema 
from Cassandra import manager as cassandra_manager

DATA_DIR = 'data'

def get_dgraph_client():
    stub = connect.create_client_stub()
    return connect.create_client(stub)

def get_cassandra_session():
    return connect.create_cassandra_session()

def load_csv(filename):
    file_path = os.path.join(DATA_DIR, filename)
    data = []
    if not os.path.exists(file_path):
        print(f"[Aviso] No se encontró {file_path}. Saltando carga de este archivo.")
        return []
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data

def populate():
    # --- 1. CONFIGURACIÓN DGRAPH ---
    print(">> Conectando a Dgraph...")
    dgraph_client = get_dgraph_client()
    op = pydgraph.Operation(drop_op="DATA")
    dgraph_client.alter(op)
    dgraph_manager.set_schema(dgraph_client)

    # --- 2. CONFIGURACIÓN CASSANDRA ---
    print(">> Conectando a Cassandra...")
    cass_session = get_cassandra_session()
    if cass_session:
        print(">> Recreando esquema Cassandra...")
        # Nota: schema.create_schema usualmente usa 'IF NOT EXISTS', 
        # para limpiar datos viejos en desarrollo, a veces es útil hacer un drop keyspace manual
        # o simplemente confiar en que los inserts nuevos se agregarán.
        schema.create_schema(cass_session)
        # Opcional: Limpiar tablas para no duplicar si corres el script varias veces
        try:
            tablas = ['mensajes_ticket', 'historial_estados', 'rendimiento_operador', 'bitacora_actividades', 'participacion_agentes']
            for t in tablas:
                cass_session.execute(f"TRUNCATE helpdesk_system.{t}")
            print(">> Tablas de Cassandra limpiadas.")
        except Exception as e:
            print(f">> Nota: No se pudieron truncar tablas (quizás es la primera ejecución): {e}")

    else:
        print("!! No se pudo conectar a Cassandra. Saltando...")

    # --- 3. CARGA DE DATOS ---
    
    # Mapas para Dgraph (UIDs temporales)
    org_uid_map = {}
    cust_uid_map = {}
    agent_uid_map = {}

    txn = dgraph_client.txn()
    try:
        print("\n--- Procesando Organizaciones ---")
        orgs = load_csv('organizations.csv')
        for row in orgs:
            obj = {'uid': '_:' + row['org_id'], 'dgraph.type': 'Organization', 'org_id': row['org_id'], 'name': row['name']}
            txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))
        txn.commit()
        
        # Re-mapeo Dgraph
        txn = dgraph_client.txn()
        res = txn.query("""{ orgs(func: type(Organization)) { uid org_id } }""")
        for o in json.loads(res.json).get('orgs', []):
            org_uid_map[o['org_id']] = o['uid']

        print("\n--- Procesando Clientes ---")
        customers = load_csv('customers.csv')
        for row in customers:
            org_real_uid = org_uid_map.get(row['belongs_to_org'])
            if org_real_uid:
                obj = {
                    'uid': '_:' + row['customer_id'], 'dgraph.type': 'Customer',
                    'customer_id': row['customer_id'], 'name': row['name'],
                    'belongs_to_org': [{'uid': org_real_uid}]
                }
                txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))
        txn.commit()

        # Re-mapeo Clientes
        txn = dgraph_client.txn()
        res = txn.query("""{ cust(func: type(Customer)) { uid customer_id } }""")
        for c in json.loads(res.json).get('cust', []):
            cust_uid_map[c['customer_id']] = c['uid']

        print("\n--- Procesando Agentes ---")
        agents = load_csv('agents.csv')
        for row in agents:
            obj = {'uid': '_:' + row['agent_id'], 'dgraph.type': 'Agent', 'agent_id': row['agent_id'], 'name': row['name']}
            txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))
        txn.commit()

        # Re-mapeo Agentes
        txn = dgraph_client.txn()
        res = txn.query("""{ ag(func: type(Agent)) { uid agent_id } }""")
        for a in json.loads(res.json).get('ag', []):
            agent_uid_map[a['agent_id']] = a['uid']

        # Jerarquías Dgraph
        hierarchy_mutations = []
        for row in agents:
            if row['reports_to'] in agent_uid_map:
                sub = agent_uid_map[row['agent_id']]
                sup = agent_uid_map[row['reports_to']]
                hierarchy_mutations.append(f'<{sub}> <reports_to> <{sup}> .')
        if hierarchy_mutations:
            txn = dgraph_client.txn()
            txn.mutate(pydgraph.Mutation(set_nquads='\n'.join(hierarchy_mutations).encode('utf-8')))
            txn.commit()

        print("\n--- Procesando Incidentes (Base) ---")
        incidents = load_csv('incidents.csv')
        txn = dgraph_client.txn()
        
        count_cass_init = 0
        for row in incidents:
            # 1. DGRAPH
            cust_uid = cust_uid_map.get(row['reported_by'])
            agent_uid = agent_uid_map.get(row['assigned_to'])
            if cust_uid:
                obj = {
                    'dgraph.type': 'Incident', 'incident_id': row['incident_id'],
                    'status': row['status'], 'reported_by': [{'uid': cust_uid}]
                }
                if agent_uid:
                    obj['assigned_to'] = [{'uid': agent_uid}]
                txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))

            # 2. CASSANDRA (Estado Inicial Base)
            # Solo insertamos si NO vamos a cargar historial detallado para este ticket,
            # pero como estamos haciendo una carga masiva, es seguro insertar el estado base primero.
            if cass_session:
                cassandra_manager.update_ticket_status(
                    cass_session, 
                    row['incident_id'], 
                    row['status'], 
                    row['assigned_to'] if row['assigned_to'] else "system", 
                    "Estado inicial (Importación)"
                )
                if row['assigned_to']:
                    cassandra_manager.register_participation(
                        cass_session, row['incident_id'], row['assigned_to'], "Asignación Inicial"
                    )
                count_cass_init += 1

        txn.commit()
        print(f"[Dgraph] Incidentes cargados.")
        print(f"[Cassandra] Estados base sincronizados: {count_cass_init}")

        # --- CARGA DE HISTORIAL CASSANDRA ---
        if cass_session:
            print("\n--- Cargando Historial de Chat (Cassandra) ---")
            chats = load_csv('chat_history.csv')
            for row in chats:
                cassandra_manager.register_message(
                    cass_session,
                    row['ticket_id'],
                    row['agent_id'],
                    row['message']
                )
            print(f">> {len(chats)} mensajes de chat insertados.")

            print("\n--- Cargando Historial de Estados (Cassandra) ---")
            # Esto llenará historial, bitácora y rendimiento
            history = load_csv('status_history.csv')
            for row in history:
                cassandra_manager.update_ticket_status(
                    cass_session,
                    row['ticket_id'],
                    row['status'],
                    row['agent_id'],
                    row['details']
                )
                # También registramos participación explícita
                cassandra_manager.register_participation(
                    cass_session,
                    row['ticket_id'],
                    row['agent_id'],
                    f"Cambio de estado a {row['status']}"
                )
            print(f">> {len(history)} eventos de historial insertados.")

    except Exception as e:
        print(f"\n[ERROR] Falló la carga: {e}")
        # import traceback; traceback.print_exc()

if __name__ == '__main__':
    populate()