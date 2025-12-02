import csv
import os
import json
import connect
from datetime import datetime
from bson import ObjectId

# Importamos los managers
from Dgraph import manager as dgraph_manager
# from Cassandra import manager as cass_manager (Si existe)

DATA_DIR = 'data'

def load_json(filename):
    """Lee un JSON y devuelve la lista de datos"""
    file_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(file_path):
        print(f"[Aviso] No se encontró {file_path}")
        return []
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def parse_mongo_document(doc):
    """Convierte formato JSON extendido ($oid, $date) a tipos nativos de Python/Mongo"""
    for key, value in doc.items():
        if isinstance(value, dict):
            if "$oid" in value:
                doc[key] = ObjectId(value["$oid"])
            elif "$date" in value:
                # Maneja fechas ISO 8601
                doc[key] = datetime.fromisoformat(value["$date"].replace("Z", "+00:00"))
            else:
                doc[key] = parse_mongo_document(value)
    return doc

def populate_mongo(db):
    print("\n>>> Poblando MongoDB...")
    
    # 1. Usuarios
    users_data = load_json('usuarios.json')
    if users_data:
        db.usuarios.drop() # Limpiar colección
        clean_users = [parse_mongo_document(doc) for doc in users_data]
        if clean_users:
            db.usuarios.insert_many(clean_users)
            print(f" -> {len(clean_users)} usuarios insertados.")

    # 2. Operadores
    ops_data = load_json('operadores.json')
    if ops_data:
        db.operadores.drop()
        clean_ops = [parse_mongo_document(doc) for doc in ops_data]
        if clean_ops:
            db.operadores.insert_many(clean_ops)
            print(f" -> {len(clean_ops)} operadores insertados.")

    # 3. Tickets
    tickets_data = load_json('tickets.json')
    if tickets_data:
        db.tickets.drop()
        clean_tickets = [parse_mongo_document(doc) for doc in tickets_data]
        if clean_tickets:
            db.tickets.insert_many(clean_tickets)
            print(f" -> {len(clean_tickets)} tickets insertados.")

def populate_dgraph(client):
    # ... (Tu código existente de Dgraph aquí, sin cambios) ...
    # Solo asegúrate de que manager.set_schema(client) esté implementado
    print("\n>>> Poblando Dgraph (Lógica existente)...")
    # Puedes copiar aquí tu lógica anterior de load_csv y mutations
    pass 

def main():
    # 1. Conexiones
    dgraph_client = connect.create_client(connect.create_client_stub())
    mongo_db = connect.create_mongo_db()

    # 2. Ejecutar poblado
    populate_mongo(mongo_db)
    
    # populate_dgraph(dgraph_client) # Descomentar cuando integres todo el código de Dgraph

    print("\n[ÉXITO] Carga de datos finalizada.")

if __name__ == '__main__':
    main()