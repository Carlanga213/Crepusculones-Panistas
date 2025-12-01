import json
import falcon
import os
from datetime import datetime, timedelta
from bson import ObjectId
from pymongo import MongoClient

# -----------------------------
# Función para convertir Extended JSON a tipos nativos
# -----------------------------
def parse_document(doc):
    for key, value in doc.items():
        if isinstance(value, dict):
            if "$oid" in value:
                doc[key] = ObjectId(value["$oid"])
            elif "$date" in value:
                doc[key] = datetime.fromisoformat(value["$date"].replace("Z", "+00:00"))
            else:
                doc[key] = parse_document(value)
    return doc

# -----------------------------
# CONEXIÓN A MONGO
# -----------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["proyectoBD"]

usuarios = db["usuarios"]
operadores = db["operadores"]
tickets = db["tickets"]

# -----------------------------
# DIRECTORIO DATA/
# -----------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

BASE_DIR = os.path.dirname(CURRENT_DIR)

DATA_DIR = os.path.join(BASE_DIR, "data")

if not os.path.exists(DATA_DIR):
    print(f"[ERROR] No se encontró la carpeta data en: {DATA_DIR}")
else:
    print(f"[INFO] Carpeta data localizada en: {DATA_DIR}")

# -----------------------------
# CARGAR JSON A MONGO
# -----------------------------
class LoadDataResource:
    def on_post(self, req, resp):
        usuarios.drop()
        operadores.drop()
        tickets.drop()

        # Usuarios
        with open(os.path.join(DATA_DIR, "usuarios.json"), "r", encoding="utf-8") as f:
            data = json.load(f)
            data = [parse_document(d) for d in data]
            usuarios.insert_many(data)

        # Operadores
        with open(os.path.join(DATA_DIR, "operadores.json"), "r", encoding="utf-8") as f:
            data = json.load(f)
            data = [parse_document(d) for d in data]
            operadores.insert_many(data)

        # Tickets
        with open(os.path.join(DATA_DIR, "tickets.json"), "r", encoding="utf-8") as f:
            data = json.load(f)
            data = [parse_document(d) for d in data]
            tickets.insert_many(data)

        resp.media = {"status": "Datos cargados correctamente"}
        resp.status = falcon.HTTP_200

# -----------------------------
# USUARIOS
# -----------------------------
class UsuariosResource:
    def on_get(self, req, resp):
        empresa = req.get_param("empresa")

        query = {}
        if empresa:
            query["empresa"] = empresa

        data = list(usuarios.find(query))
        for d in data:
            d["_id"] = str(d["_id"])

        resp.media = {"results": data}
        resp.status = falcon.HTTP_200


# -----------------------------
# OPERADORES
# -----------------------------
class OperadoresResource:
    def on_get(self, req, resp):
        nivel = req.get_param("nivel")
        turno = req.get_param("turno")
        departamento = req.get_param("departamento")

        query = {}
        if nivel:
            query["nivel"] = nivel
        if turno:
            query["turno"] = turno
        if departamento:
            query["departamento"] = departamento

        data = list(operadores.find(query))

        # Convertir ObjectId y datetime a string
        for d in data:
            d["_id"] = str(d["_id"])
            for k, v in d.items():
                if isinstance(v, datetime):
                    d[k] = v.isoformat()  # Convierte datetime a string ISO

        resp.media = {"results": data}
        resp.status = falcon.HTTP_200

# -----------------------------
# TICKETS
# -----------------------------
class TicketsResource:
    def on_get(self, req, resp):
        try:
            usuario = req.get_param("usuario")
            operador = req.get_param("operador")
            prioridad = req.get_param("prioridad")
            estado = req.get_param("estado")
            search = req.get_param("search")
            abiertos7 = req.get_param_as_bool("abiertos7")
            pipeline = req.get_param("pipeline")

            query = {}

            if usuario:
                if ObjectId.is_valid(usuario):
                    query["usuarioId"] = ObjectId(usuario)
                else:
                    resp.media = {"error": "ID de usuario inválido"}
                    resp.status = falcon.HTTP_400
                    return

            if operador:
                if ObjectId.is_valid(operador):
                    query["operadorId"] = ObjectId(operador)
                else:
                    resp.media = {"error": "ID de operador inválido"}
                    resp.status = falcon.HTTP_400
                    return

            if prioridad:
                query["prioridad"] = prioridad

            if estado:
                query["estado"] = estado

            if search:
                query["descripcion"] = {"$regex": search, "$options": "i"}

            if abiertos7:
                hace_7 = datetime.utcnow() - timedelta(days=7)
                query["estado"] = "abierto"
                query["fechaCreacion"] = {"$lt": hace_7}

            # -----------------------------
            # PIPELINES
            # -----------------------------
            if pipeline == "estado":
                resp.media = list(tickets.aggregate([
                    {"$group": {"_id": "$estado", "total": {"$sum": 1}}},
                    {"$sort": {"total": -1}}
                ]))
                return

            if pipeline == "operador":
                resp.media = list(tickets.aggregate([
                    {"$lookup": {
                        "from": "operadores",
                        "localField": "operadorId",
                        "foreignField": "_id",
                        "as": "operador"
                    }},
                    {"$unwind": "$operador"},
                    {"$group": {"_id": "$operador.nombre", "totalTickets": {"$sum": 1}}},
                    {"$sort": {"totalTickets": -1}}
                ]))
                return

            if pipeline == "7dias":
                resp.media = list(tickets.aggregate([
                    {"$match": {"estado": "abierto"}},
                    {"$addFields": {
                        "diasAbierto": {
                            "$divide": [
                                {"$subtract": [datetime.utcnow(), "$fechaCreacion"]},
                                1000 * 60 * 60 * 24
                            ]
                        }
                    }},
                    {"$match": {"diasAbierto": {"$gt": 7}}},
                    {"$project": {
                        "_id": 0,
                        "titulo": 1,
                        "diasAbierto": {"$round": ["$diasAbierto", 1]}
                    }}
                ]))
                return

            # -----------------------------
            # CONSULTAS NORMALES
            # -----------------------------
            data = list(tickets.find(query))
            for d in data:
                d["_id"] = str(d["_id"])
                if "usuarioId" in d:
                    d["usuarioId"] = str(d["usuarioId"])
                if "operadorId" in d:
                    d["operadorId"] = str(d["operadorId"])
                if "fechaCreacion" in d and d["fechaCreacion"]:
                    d["fechaCreacion"] = d["fechaCreacion"].isoformat()
                if "fechaCierre" in d and d["fechaCierre"]:
                    d["fechaCierre"] = d["fechaCierre"].isoformat()

            resp.media = {"count": len(data), "results": data}
            resp.status = falcon.HTTP_200

        except Exception as e:
            resp.media = {"error": str(e)}
            resp.status = falcon.HTTP_500
            print("ERROR en TicketsResource:", e)