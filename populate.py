import csv
import os
import json
import pydgraph
import connect
from Dgraph import manager

# Configuración de ruta de datos
DATA_DIR = 'data'

def get_client():
    """Crea una conexión usando el archivo connect.py"""
    stub = connect.create_client_stub()
    return connect.create_client(stub)

def drop_data(client):
    """Limpia los datos antiguos manteniendo el esquema"""
    print(">> Limpiando datos antiguos...")
    op = pydgraph.Operation(drop_op="DATA")
    client.alter(op)

def load_csv(filename):
    """Lee un CSV y devuelve una lista de diccionarios"""
    file_path = os.path.join(DATA_DIR, filename)
    data = []
    if not os.path.exists(file_path):
        print(f"[Error] No se encontró {file_path}. Ejecuta data_generator.py primero.")
        return []
    
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data

def populate():
    client = get_client()
    
    # 1. Limpieza inicial
    drop_data(client)
    
    # 2. Asegurar esquema
    print(">> Verificando esquema...")
    manager.set_schema(client)

    org_uid_map = {}
    cust_uid_map = {}
    agent_uid_map = {}

    txn = client.txn()
    try:
        print("\n--- Cargando Organizaciones ---")
        orgs = load_csv('organizations.csv')
        for row in orgs:
            obj = {
                'uid': '_:' + row['org_id'],
                'dgraph.type': 'Organization',
                'org_id': row['org_id'],
                'name': row['name']
            }
            # CORRECCIÓN: Siempre enviar bytes
            txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))
            
        # Para obtener los UIDs reales, necesitamos hacer un commit parcial o usar 
        # asignación de variables. Dgraph asigna UIDs al commitear o al consultar.
        # Para este script simple, haremos commits parciales para asegurar que obtenemos los UIDs 
        # y evitar errores de memoria en transacciones gigantes.
        txn.commit() 
        
        # Recuperar UIDs de Organizaciones recién creadas para mapear
        # (Esto es necesario porque mutate con set_json devuelve UIDs de blank nodes solo en el contexto inmediato)
        txn = client.txn()
        query_orgs = """{ orgs(func: type(Organization)) { uid org_id } }"""
        res = txn.query(query_orgs)
        mapped_orgs = json.loads(res.json).get('orgs', [])
        for o in mapped_orgs:
            org_uid_map[o['org_id']] = o['uid']
        print(f"Cargadas {len(mapped_orgs)} organizaciones.")

        print("\n--- Cargando Clientes ---")
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
                txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))
        
        txn.commit()
        
        # Recuperar UIDs Clientes
        txn = client.txn()
        query_cust = """{ cust(func: type(Customer)) { uid customer_id } }"""
        res = txn.query(query_cust)
        mapped_cust = json.loads(res.json).get('cust', [])
        for c in mapped_cust:
            cust_uid_map[c['customer_id']] = c['uid']
        print(f"Cargados {len(mapped_cust)} clientes.")

        print("\n--- Cargando Agentes (Nodos) ---")
        agents = load_csv('agents.csv')
        for row in agents:
            obj = {
                'uid': '_:' + row['agent_id'],
                'dgraph.type': 'Agent',
                'agent_id': row['agent_id'],
                'name': row['name']
            }
            txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))
        txn.commit()

        # Recuperar UIDs Agentes
        txn = client.txn()
        query_ag = """{ ag(func: type(Agent)) { uid agent_id } }"""
        res = txn.query(query_ag)
        mapped_ag = json.loads(res.json).get('ag', [])
        for a in mapped_ag:
            agent_uid_map[a['agent_id']] = a['uid']
        
        print(f"Nodos de agentes creados. Configurando jerarquías...")

        # Configurar Jerarquías
        hierarchy_mutations = []
        for row in agents:
            supervisor_id = row['reports_to']
            if supervisor_id and supervisor_id in agent_uid_map:
                sub_uid = agent_uid_map[row['agent_id']]
                sup_uid = agent_uid_map[supervisor_id]
                rdf = f'<{sub_uid}> <reports_to> <{sup_uid}> .'
                hierarchy_mutations.append(rdf)
        
        if hierarchy_mutations:
            # CORRECCIÓN PRINCIPAL AQUÍ: .encode('utf-8')
            nquads_str = '\n'.join(hierarchy_mutations)
            txn.mutate(pydgraph.Mutation(set_nquads=nquads_str.encode('utf-8')))
            txn.commit()
            print(f"Jerarquías configuradas.")
            txn = client.txn() # Nueva transacción

        print("\n--- Cargando Incidentes (Tickets) ---")
        incidents = load_csv('incidents.csv')
        for row in incidents:
            cust_uid = cust_uid_map.get(row['reported_by'])
            agent_uid = agent_uid_map.get(row['assigned_to'])
            
            if cust_uid:
                obj = {
                    'dgraph.type': 'Incident',
                    'incident_id': row['incident_id'],
                    'status': row['status'],
                    'reported_by': [{'uid': cust_uid}]
                }
                if agent_uid:
                    obj['assigned_to'] = [{'uid': agent_uid}]

                txn.mutate(pydgraph.Mutation(set_json=json.dumps(obj).encode('utf8')))

        txn.commit()
        print(f"Cargados {len(incidents)} incidentes.")
        print("\n[ÉXITO] Base de datos poblada correctamente.")
        
    except Exception as e:
        print(f"\n[ERROR] Falló la carga de datos: {e}")
        # Si falla, intentamos descartar lo pendiente
        try:
            txn.discard()
        except:
            pass

if __name__ == '__main__':
    populate()