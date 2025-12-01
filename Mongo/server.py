# Archivo: Mongo/server.py
import falcon
from wsgiref.simple_server import make_server
import resources  # Importa tu archivo Mongo/resources.py

# 1. Crear la aplicación API
app = falcon.App()

# 2. Instanciar los recursos que definiste en resources.py
load_data_resource = resources.LoadDataResource()
usuarios_resource = resources.UsuariosResource()
operadores_resource = resources.OperadoresResource()
tickets_resource = resources.TicketsResource()

# 3. Definir las rutas (endpoints) que usará client.py
app.add_route('/load_data', load_data_resource)
app.add_route('/usuarios', usuarios_resource)
app.add_route('/operadores', operadores_resource)
app.add_route('/tickets', tickets_resource)

# 4. Iniciar el servidor
if __name__ == '__main__':
    port = 8000
    print(f"✅ Servidor API de MongoDB corriendo en http://localhost:{port}")
    print("   (Presiona Ctrl+C para detenerlo)")
    
    with make_server('', port, app) as httpd:
        httpd.serve_forever()