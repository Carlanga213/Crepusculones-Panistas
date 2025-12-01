import pydgraph
from cassandra.cluster import Cluster

def create_client_stub(host='localhost', port='9080'):
    """
    Crea y retorna el stub de conexión para Dgraph.
    Se conecta al puerto 9080 por defecto.
    """
    return pydgraph.DgraphClientStub(f'{host}:{port}')

def create_client(client_stub):
    """
    Crea una instancia del cliente Dgraph usando el stub proporcionado.
    """
    return pydgraph.DgraphClient(client_stub)


# cassandra
def create_cassandra_session(hosts=['127.0.0.1']):
    """
    Crea la conexión al clúster de Cassandra.
    """
    try:
        cluster = Cluster(hosts)
        session = cluster.connect()
        
        try:
            session.set_keyspace('helpdesk_system')
        except:
            pass 
        return session
    except Exception as e:
        print(f"Error conectando a Cassandra: {e}")
        return None