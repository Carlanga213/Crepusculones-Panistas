import pydgraph

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