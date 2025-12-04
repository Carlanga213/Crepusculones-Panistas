import pydgraph
from cassandra.cluster import Cluster
from pymongo import MongoClient

# --- DGRAPH ---
def create_client_stub(host='localhost', port='9080'):
    return pydgraph.DgraphClientStub(f'{host}:{port}')

def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)




# --- CASSANDRA ---
def create_cassandra_session(hosts=['localhost'],port=9042):
    try:
        cluster = Cluster(hosts, port=port)
        session= cluster.connect()
        try:
            session.set_keyspace('helpdesk_system')
        except:
            pass
        return session
    except Exception as e:
        print(f"Error conectando a cassandra: {e}")
        return None






# --- MONGODB ---
def create_mongo_db():
    """
    Conecta a MongoDB y retorna la base de datos 'proyectoBD'
    """
    client = MongoClient("mongodb://localhost:27017/")
    db = client["proyectoBD"]
    return db
