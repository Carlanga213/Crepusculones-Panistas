import csv
import random
import os

# Configuración de cantidad de datos
NUM_ORGS = 10
NUM_CUSTOMERS = 50
NUM_AGENTS = 20
NUM_INCIDENTS = 200  # Solicitud: 200 registros de datos principales

# Rutas de salida
DATA_DIR = 'data'
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

# Listas auxiliares para aleatoriedad
ORG_NAMES = ["TechSolutions", "GlobalCorp", "SoftServe", "DataMinds", "NetWorks", "CloudSys", "AlphaInd", "OmegaGroup", "PyramidInc", "StarkInd"]
STATUS_LIST = ["abierto", "en_progreso", "resuelto", "cerrado", "en_espera"]
NAMES = ["Ana", "Carlos", "Luis", "Maria", "Sofia", "Jorge", "Lucia", "Miguel", "Elena", "Roberto", "David", "Carmen", "Javier", "Isabel", "Fernando"]
SURNAMES = ["Perez", "Gomez", "Lopez", "Rodriguez", "Martinez", "Fernandez", "Garcia", "Sanchez", "Diaz", "Torres"]

def get_name():
    return f"{random.choice(NAMES)} {random.choice(SURNAMES)}"

def generate_csvs():
    print(f"Generando datos en carpeta '{DATA_DIR}'...")

    # 1. Generar Organizaciones
    org_ids = []
    with open(f'{DATA_DIR}/organizations.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['org_id', 'name'])
        for i in range(NUM_ORGS):
            oid = f"org_{i+1:03d}"
            org_ids.append(oid)
            writer.writerow([oid, ORG_NAMES[i] if i < len(ORG_NAMES) else f"Org_{i}"])
    print(f" -> organizations.csv ({NUM_ORGS} registros)")

    # 2. Generar Clientes (Vinculados a Orgs)
    cust_ids = []
    with open(f'{DATA_DIR}/customers.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['customer_id', 'name', 'belongs_to_org']) # [cite: 170-173]
        for i in range(NUM_CUSTOMERS):
            cid = f"cust_{i+1:03d}"
            cust_ids.append(cid)
            org_ref = random.choice(org_ids)
            writer.writerow([cid, get_name(), org_ref])
    print(f" -> customers.csv ({NUM_CUSTOMERS} registros)")

    # 3. Generar Agentes (Con Jerarquía simple)
    agent_ids = []
    with open(f'{DATA_DIR}/agents.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['agent_id', 'name', 'reports_to']) # [cite: 175-179]
        
        # Crear primero al supervisor general
        boss_id = "agent_001"
        agent_ids.append(boss_id)
        writer.writerow([boss_id, "Jefe Supremo", ""])

        for i in range(1, NUM_AGENTS):
            aid = f"agent_{i+1:03d}"
            agent_ids.append(aid)
            # 30% probabilidad de reportar al jefe, 70% a otro agente aleatorio ya creado
            supervisor = boss_id if random.random() < 0.3 else random.choice(agent_ids[:-1])
            writer.writerow([aid, get_name(), supervisor])
    print(f" -> agents.csv ({NUM_AGENTS} registros)")

    # 4. Generar Incidentes (200 registros vinculados)
    with open(f'{DATA_DIR}/incidents.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['incident_id', 'status', 'reported_by', 'assigned_to']) # [cite: 181-186]
        for i in range(NUM_INCIDENTS):
            inc_id = f"inc_{i+1:04d}"
            status = random.choice(STATUS_LIST)
            customer = random.choice(cust_ids)
            # 80% de probabilidad de estar asignado a un agente
            agent = random.choice(agent_ids) if random.random() > 0.2 else ""
            
            writer.writerow([inc_id, status, customer, agent])
    print(f" -> incidents.csv ({NUM_INCIDENTS} registros)")
    print("\n¡Generación completada!")

if __name__ == '__main__':
    generate_csvs()