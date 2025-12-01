import argparse
import requests

BASE = "http://localhost:8000"

parser = argparse.ArgumentParser()

# Usuarios
parser.add_argument("--empresa")

# Operadores
parser.add_argument("--nivel")
parser.add_argument("--turno")
parser.add_argument("--departamento")

# Tickets
parser.add_argument("--usuario")
parser.add_argument("--operador")
parser.add_argument("--prioridad")
parser.add_argument("--estado")
parser.add_argument("--search")
parser.add_argument("--abiertos7", action="store_true")

# Pipelines
parser.add_argument("--pipeline")

# Cargar data inicial
parser.add_argument("--load", action="store_true")

args = parser.parse_args()

# -------------------------
# Cargar datos
# -------------------------
if args.load:
    r = requests.post(BASE + "/load_data")
    print(r.json())
    exit()

# -------------------------
# Usuarios
# -------------------------
if args.empresa:
    r = requests.get(BASE + "/usuarios", params={"empresa": args.empresa})
    print(r.json())
    exit()

# -------------------------
# Operadores
# -------------------------
if args.nivel or args.turno or args.departamento:
    params = {}
    if args.nivel: params["nivel"] = args.nivel
    if args.turno: params["turno"] = args.turno
    if args.departamento: params["departamento"] = args.departamento

    r = requests.get(BASE + "/operadores", params=params)
    print(r.json())
    exit()

# -------------------------
# Tickets
# -------------------------
params = {}

if args.usuario: params["usuario"] = args.usuario
if args.operador: params["operador"] = args.operador
if args.prioridad: params["prioridad"] = args.prioridad
if args.estado: params["estado"] = args.estado
if args.search: params["search"] = args.search
if args.abiertos7: params["abiertos7"] = "true"

if args.pipeline: params["pipeline"] = args.pipeline

r = requests.get(BASE + "/tickets", params=params)
print(r.json())