import json
from datetime import datetime
from bson import ObjectId

# --- Consultas y Lógica de Negocio ---

def get_tickets_by_user(db, user_id):
    print(f"\n--- Tickets del Usuario {user_id} ---")
    # Intentamos convertir a ObjectId si es posible, si no, buscamos como string
    try:
        query = {"usuarioId": ObjectId(user_id)}
    except:
        query = {"usuarioId": user_id}
        
    tickets = list(db.tickets.find(query))
    for t in tickets:
        print(f"[{t.get('status')}] {t.get('titulo')} (Prioridad: {t.get('prioridad')})")

def get_tickets_by_priority(db, prioridad):
    print(f"\n--- Tickets con Prioridad '{prioridad}' ---")
    tickets = list(db.tickets.find({"prioridad": prioridad}))
    for t in tickets:
        print(f"ID: {t['_id']} - {t.get('titulo')}")

def get_tickets_by_operator(db, operator_id):
    print(f"\n--- Tickets del Operador {operator_id} ---")
    try:
        query = {"operadorId": ObjectId(operator_id)}
    except:
        query = {"operadorId": operator_id}

    tickets = list(db.tickets.find(query))
    for t in tickets:
        print(f"[{t.get('status')}] {t.get('titulo')}")

def get_tickets_by_status(db, status):
    print(f"\n--- Tickets con Estado '{status}' ---")
    query = {"estado": {"$regex": f"^{status}$", "$options": "i"}}
    
    tickets = list(db.tickets.find(query))
    
    if not tickets:
        print("No se encontraron tickets con ese estado.")
        return

    for t in tickets:

        print(f"ID: {t['_id']} - {t.get('titulo', 'Sin título')} [{t.get('estado')}]")

def get_operators_by_level(db, nivel):
    print(f"\n--- Operadores Nivel '{nivel}' ---")
    ops = list(db.operadores.find({"nivel": nivel}))
    for o in ops:
        print(f"{o.get('nombre')} - {o.get('departamento')}")

def search_tickets(db, text):
    print(f"\n--- Buscar Tickets: '{text}' ---")
    # Búsqueda simple por regex en título o descripción
    query = {
        "$or": [
            {"titulo": {"$regex": text, "$options": "i"}},
            {"descripcion": {"$regex": text, "$options": "i"}}
        ]
    }
    tickets = list(db.tickets.find(query))
    for t in tickets:
        print(f"[{t.get('status')}] {t.get('titulo')}")

def get_old_open_tickets(db):
    print("\n--- Tickets Abiertos > 7 días ---")
    hace_7_dias = datetime.now() 

    pipeline = [
        {
            "$match": {
                "estado": {"$regex": "^abierto$", "$options": "i"}
            }
        },
        {
            "$project": {
                "titulo": 1,
                "fechaCreacion": 1,
                "diasAbierto": {
                    "$divide": [
                        {"$subtract": [datetime.now(), "$fechaCreacion"]}, 
                        1000 * 60 * 60 * 24 
                    ]
                }
            }
        },
        {
            "$match": {
                "diasAbierto": {"$gt": 7}
            }
        },
        {
            "$project": {
                "titulo": 1, 
                "diasAbierto": {"$round": ["$diasAbierto", 1]} 
            }
        }
    ]

    try:
        results = list(db.tickets.aggregate(pipeline))
        
        if not results:
            print("No se encontraron tickets abiertos con más de 7 días de antigüedad.")
        
        for r in results:
            print(f"• {r.get('titulo', 'Sin título')} -> {r['diasAbierto']} días abierto")
            
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")

def get_users_by_company(db, empresa):
    print(f"\n--- Usuarios de '{empresa}' ---")
    users = list(db.usuarios.find({"empresa": empresa}))
    for u in users:
        print(f"{u.get('nombre')} ({u.get('email')})")

def get_operators_by_dept(db, depto):
    print(f"\n--- Operadores de '{depto}' ---")
    ops = list(db.operadores.find({"departamento": depto}))
    for o in ops:
        print(f"{o.get('nombre')} - Turno: {o.get('turno')}")

def get_operators_by_shift(db, turno):
    print(f"\n--- Operadores Turno '{turno}' ---")
    ops = list(db.operadores.find({"turno": turno}))
    for o in ops:
        print(f"{o.get('nombre')} - Depto: {o.get('departamento')}")

# --- Pipelines de Agregación ---

def pipeline_count_by_status(db):
    print("\n--- Pipeline: Conteo por Estado ---")
    pipeline = [
        {"$group": {"_id": "$estado", "total": {"$sum": 1}}},
        {"$sort": {"total": -1}}
    ]
    
    try:
        results = list(db.tickets.aggregate(pipeline))
        
        if not results:
            print("No se encontraron resultados.")
            return

        print(f"{'Estado':<20} | {'Cantidad':<10}")
        print("-" * 33)
        for r in results:
            estado_nombre = r['_id'] if r['_id'] else "Sin estado"
            print(f"{estado_nombre:<20} | {r['total']:<10}")
            
    except Exception as e:
        print(f"Error en el pipeline: {e}")

def pipeline_count_by_priority(db):
    print("\n--- Pipeline: Conteo por Prioridad ---")
    pipeline = [
        {"$group": {"_id": "$prioridad", "total": {"$sum": 1}}}
    ]
    results = list(db.tickets.aggregate(pipeline))
    for r in results:
        print(f"{r['_id']}: {r['total']}")