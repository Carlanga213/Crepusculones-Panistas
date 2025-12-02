import csv
import os
import json
import connect
from datetime import datetime
from bson import ObjectId
import pydgraph

# Importamos los managers y esquemas
from Dgraph import manager as dgraph_manager
from Cassandra import manager as cass_manager
from Cassandra import schema as cass_schema  # Necesario para recrear las tablas

DATA_DIR = 'data'

# --- UTILIDADES ---
def load_json(filename):
    """Lee un JSON y devuelve la lista de datos"""
    file_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(file_path):
        print(f"[Aviso] No se encontró {file_path}")
        return []
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_csv(filename):
    """Lee un CSV y devuelve una lista de diccionarios"""
    file_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(file_path):
        print(f"[Aviso] No se encontró {file_path}")
        return []
    with open(file_path, mode='r', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def parse_mongo_document(doc):
    """Convierte formato JSON extendido a tipos nativos"""
    for key, value in doc.items():
        if isinstance(value, dict):
            if "$oid" in value:
                doc[key] = ObjectId(value["$oid"])
            elif "$date" in value:
                doc[key] = datetime.fromisoformat(value["$date"].replace("Z", "+00:00"))
            else:
                doc[key] = parse_mongo_document(value)
    return doc

def populate_mongo(db):
    print("\n>>> 1. MONGODB: Reiniciando colecciones...")
    
    # Borrar colecciones existentes
    db.usuarios.drop()
    db.operadores.drop()
    db.tickets.drop()
    print(" -> Colecciones antiguas eliminadas.")

    # 1. Usuarios
    users_data = load_json('usuarios.json')
    if users_data:
        clean_users = [parse_mongo_document(doc) for doc in users_data]
        db.usuarios.insert_many(clean_users)
        print(f" -> {len(clean_users)} usuarios insertados.")

    # 2. Operadores
    ops_data = load_json('operadores.json')
    if ops_data:
        clean_ops = [parse_mongo_document(doc) for doc in ops_data]
        db.operadores.insert_many(clean_ops)
        print(f" -> {len(clean_ops)} operadores insertados.")

    # 3. Tickets
    tickets_data = load_json('tickets.json')
    if tickets_data:
        clean_tickets = [parse_mongo_document(doc) for doc in tickets_data]
        db.tickets.insert_many(clean_tickets)
        print(f" -> {len(clean_tickets)} tickets insertados.")

# --- DGRAPH ---
def populate_dgraph(client):
    print("\n>>> 2. DGRAPH: Reiniciando grafo...")
    
    # 1. Limpiar TODOS los datos y esquema antiguos
    op = pydgraph.Operation(drop_op="DATA")
    client.alter(op)
    print(" -> Datos antiguos eliminados.")

    # 2. Configurar el Esquema de nuevo
    dgraph_manager.set_schema(client)

    # Mapas para guardar los UIDs reales
    org_uid_map = {}
    cust_uid_map = {}
    agent_uid_map = {}

    txn = client.txn()
    try:
        # A. Organizaciones
        orgs = load_csv('organizations.csv')
        for row in orgs:
            obj = {
                'uid': '_:' + row['org_id'],
                'dgraph.type': 'Organization',
                'org_id': row['org_id'],
                'name': row['name']
            }
            res = txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))
            org_uid_map[row['org_id']] = res.uids[row['org_id']]
        print(f" -> {len(orgs)} organizaciones creadas.")

        # B. Clientes
        customers = load_csv('customers.csv')
        for row in customers:
            org_real_uid = org_uid_map.get(row['belongs_to_org'])
            if org_real_uid:
                obj = {
                    'uid': '_:' + row['customer_id'],
                    'dgraph.type': 'Customer',
                    'customer_id': row['customer_id'],
                    'name': row['name'],
                    'belongs_to_org': [{'uid': org_real_uid}]
                }
                res = txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))
                cust_uid_map[row['customer_id']] = res.uids[row['customer_id']]
        print(f" -> {len(customers)} clientes creados.")

        # C. Agentes
        agents = load_csv('agents.csv')
        for row in agents:
            obj = {
                'uid': '_:' + row['agent_id'],
                'dgraph.type': 'Agent',
                'agent_id': row['agent_id'],
                'name': row['name']
            }
            res = txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))
            agent_uid_map[row['agent_id']] = res.uids[row['agent_id']]
        
        # D. Jerarquías
        hierarchy_mutations = []
        for row in agents:
            supervisor_id = row['reports_to']
            if supervisor_id and supervisor_id in agent_uid_map:
                sub_uid = agent_uid_map[row['agent_id']]
                sup_uid = agent_uid_map[supervisor_id]
                rdf = f'<{sub_uid}> <reports_to> <{sup_uid}> .'
                hierarchy_mutations.append(rdf)
        
        if hierarchy_mutations:
            nquads = '\n'.join(hierarchy_mutations)
            txn.mutate(pydgraph.Mutation(set_nquads=nquads.encode('utf-8')))
        print(f" -> {len(agents)} agentes y jerarquías configuradas.")

        # E. Incidentes
        incidents = load_csv('incidents.csv')
        for row in incidents:
            cust_uid = cust_uid_map.get(row['reported_by'])
            agent_uid = agent_uid_map.get(row['assigned_to'])
            
            if cust_uid:
                obj = {
                    'uid': '_:inc',
                    'dgraph.type': 'Incident',
                    'incident_id': row['incident_id'],
                    'status': row['status'],
                    'reported_by': [{'uid': cust_uid}]
                }
                if agent_uid:
                    obj['assigned_to'] = [{'uid': agent_uid}]
                
                txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))
        print(f" -> {len(incidents)} incidentes creados.")

        txn.commit()
    finally:
        txn.discard()

# --- CASSANDRA  ---
def populate_cassandra(session):
    print("\n>>> 3. CASSANDRA: Reiniciando Keyspace...")
    
    # 1. Borrar Keyspace existente para limpiar todo
    try:
        session.execute("DROP KEYSPACE IF EXISTS helpdesk_system")
        print(" -> Keyspace antiguo eliminado.")
    except Exception as e:
        print(f" !! Advertencia borrando keyspace: {e}")

    # 2. Recrear Esquema (Keyspace y Tablas)
    # Esto llama a la función que ya tienes en Cassandra/schema.py
    cass_schema.create_schema(session)
    
    # 3. Llenar datos
    # Historial de Chat
    chats = load_csv('chat_history.csv')
    if chats:
        print(f" -> Insertando {len(chats)} mensajes de chat...")
        for row in chats:
            cass_manager.register_message(
                session, 
                row['ticket_id'], 
                row['agent_id'], 
                row['message']
            )

    # Historial de Estados
    statuses = load_csv('status_history.csv')
    if statuses:
        print(f" -> Insertando {len(statuses)} cambios de estado...")
        for row in statuses:
            cass_manager.update_ticket_status(
                session, 
                row['ticket_id'], 
                row['status'], 
                row['agent_id'], 
                row['details']
            )

def main():
    print("--- INICIANDO CARGA MASIVA DE DATOS (RESET TOTAL) ---")
    
    # 1. MongoDB
    try:
        mongo_db = connect.create_mongo_db()
        populate_mongo(mongo_db)
    except Exception as e:
        print(f"[ERROR MongoDB] {e}")

    # 2. Dgraph
    try:
        stub = connect.create_client_stub()
        dgraph_client = connect.create_client(stub)
        populate_dgraph(dgraph_client)
    except Exception as e:
        print(f"[ERROR Dgraph] {e}")

    # 3. Cassandra
    try:
        cass_session = connect.create_cassandra_session()
        populate_cassandra(cass_session)
    except Exception as e:
        print(f"[ERROR Cassandra] {e}")

    print("\n[FIN] Proceso completado. Bases de datos listas.")

if __name__ == '__main__':
    main()