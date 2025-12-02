import pydgraph
from cassandra.cluster import Cluster

def create_client_stub(host='localhost', port='9080'):
    return pydgraph.DgraphClientStub(f'{host}:{port}')

def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)




# cassandra
def create_cassandra_session(hosts=['localhost'], port=9042):
    try:
        cluster = Cluster(hosts, port=port) 
        session = cluster.connect()
        try:
            session.set_keyspace('helpdesk_system')
        except:
            pass 
        return session
    except Exception as e:
        print(f"Error conectando a Cassandra: {e}")
        return None