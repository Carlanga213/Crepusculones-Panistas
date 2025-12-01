import connect as connect
import json
import os
import sys
# Importamos el módulo desde la carpeta Dgraph
from Dgraph import manager as dgraph_manager
# Importamos Cassandra (si ya tienes el manager)
# from Cassandra import manager as cassandra_manager

def print_json(data):
    """Ayuda visual para imprimir respuestas JSON bonitas"""
    print(json.dumps(data, indent=2, ensure_ascii=False))

# --- MENÚ DGRAPH (Ya implementado) ---
def menu_dgraph():
    # Establecer conexión
    try:
        stub = connect.create_client_stub()
        client = connect.create_client(stub)
    except Exception as e:
        print(f"Error conectando a Dgraph: {e}")
        return

    while True:
        print("\n" + "="*50)
        print("      MÓDULO DGRAPH - GESTIÓN DE RELACIONES")
        print("="*50)
        print("1.  [Req 1] Asignar Incidencia a Agente")
        print("2.  [Req 2] Vincular Incidencia con Cliente")
        print("3.  [Req 3] Definir Jerarquía (Subordinado -> Supervisor)")
        print("4.  [Req 4] Escalar Incidencia (Ruta de escalación)")
        print("5.  [Req 5] Vincular Incidencias Relacionadas")
        print("6.  [Req 6] Consultar Carga de Trabajo de Agente")
        print("7.  [Req 7] Historial de Incidencias de Cliente")
        print("8.  [Req 8] Asignar Cliente a Organización")
        print("9.  [Req 9] Consultar Incidencias por Organización")
        print("10. [Req 10] Trazabilidad Completa (Auditoría)")
        print("11. [ADMIN] Inicializar Esquema de Base de Datos")
        print("12. Volver al Menú Principal")
        
        op = input("\nSeleccione una opción: ")

        if op == '1':
            inc = input("ID Incidencia: ")
            ag = input("ID Agente: ")
            if dgraph_manager.assign_incident(client, inc, ag):
                print(">> Asignación exitosa.")
            else:
                print(">> Error: IDs no encontrados.")

        elif op == '2':
            inc = input("ID Incidencia: ")
            cust = input("ID Cliente: ")
            if dgraph_manager.link_incident_customer(client, inc, cust):
                print(">> Vinculación exitosa.")
            else:
                print(">> Error: IDs no encontrados.")

        elif op == '3':
            sub = input("ID Agente Subordinado: ")
            sup = input("ID Agente Supervisor: ")
            if dgraph_manager.set_agent_hierarchy(client, sub, sup):
                print(">> Jerarquía actualizada.")
            else:
                print(">> Error: Agentes no encontrados.")

        elif op == '4':
            inc = input("ID Incidencia: ")
            old = input("ID Agente (Escalador): ")
            new = input("ID Agente (Destino): ")
            if dgraph_manager.escalate_incident(client, inc, old, new):
                print(">> Escalación registrada correctamente.")
            else:
                print(">> Error: Datos no encontrados.")

        elif op == '5':
            inc_a = input("ID Incidencia A: ")
            inc_b = input("ID Incidencia B: ")
            if dgraph_manager.link_related_incidents(client, inc_a, inc_b):
                print(">> Incidencias relacionadas exitosamente.")
            else:
                print(">> Error: Incidencias no encontradas.")

        elif op == '6':
            ag = input("ID Agente a consultar: ")
            res = dgraph_manager.get_agent_workload(client, ag)
            print_json(res)

        elif op == '7':
            cust = input("ID Cliente a consultar: ")
            res = dgraph_manager.get_customer_history(client, cust)
            print_json(res)

        elif op == '8':
            cust = input("ID Cliente: ")
            org = input("ID Organización: ")
            if dgraph_manager.set_customer_org(client, cust, org):
                print(">> Cliente asignado a organización.")
            else:
                print(">> Error: IDs no encontrados.")

        elif op == '9':
            org = input("ID Organización a consultar: ")
            res = dgraph_manager.get_org_incidents(client, org)
            print_json(res)

        elif op == '10':
            inc = input("ID Incidencia a auditar: ")
            res = dgraph_manager.get_traceability(client, inc)
            print_json(res)

        elif op == '11':
            dgraph_manager.set_schema(client)

        elif op == '12':
            stub.close()
            break
        else:
            print("Opción inválida.")

# --- MENÚ MONGODB (Integrado desde menu.py) ---
def menu_mongo():
    # Verifica que exista el archivo cliente de mongo
    mongo_script = os.path.join("Mongo", "client.py")

    # Verifica que exista el archivo en la nueva ubicación
    if not os.path.exists(mongo_script):
        print(f"\n[ERROR] No se encontró '{mongo_script}'.")
        print("Asegúrate de haber movido el archivo 'client.py' a la carpeta 'Mongo'.")
        input("Presiona Enter para continuar...")
        return # Regresa al menú principal para evitar errores

    while True:
        print("\n" + "="*50)
        print("      MÓDULO MONGODB - GESTIÓN DE DOCUMENTOS")
        print("="*50)
        # Opciones copiadas de menu.py
        print("1.  Cargar base de datos (Inicializar)")
        print("2.  Tickets por usuario (ID)")
        print("3.  Tickets por prioridad")
        print("4.  Tickets por operador (ID)")
        print("5.  Tickets por estado")
        print("6.  Operadores por nivel/tier")
        print("7.  Buscar tickets por descripción")
        print("8.  Tickets abiertos > 7 días")
        print("9.  Usuarios por empresa")
        print("10. Operadores por departamento")
        print("11. Operadores por turno")
        print("12. Pipeline: Conteo por estado")
        print("13. Pipeline: Conteo por prioridad")
        print("14. Volver al Menú Principal")

        op = input("\nSeleccione una opción: ")

        # Lógica adaptada de menu.py usando os.system
        if op == "1":
            os.system("python client.py --load")
        
        elif op == "2":
            uid = input("ID del usuario: ")
            os.system(f"python client.py --usuario {uid}")
        
        elif op == "3":
            prioridad = input("Prioridad (alta, media, baja): ")
            os.system(f"python client.py --prioridad {prioridad}")
        
        elif op == "4":
            oid = input("ID del operador: ")
            os.system(f"python client.py --operador {oid}")
        
        elif op == "5":
            estado = input("Estado (abierto, cerrado, pendiente): ")
            os.system(f"python client.py --estado {estado}")
        
        elif op == "6":
            nivel = input("Nivel (tier 1, tier 2, tier 3): ")
            os.system(f"python client.py --nivel \"{nivel}\"")
        
        elif op == "7":
            texto = input("Texto a buscar en descripción: ")
            os.system(f"python client.py --search \"{texto}\"")
        
        elif op == "8":
            print(">> Buscando tickets abiertos con más de 7 días...")
            os.system("python client.py --old")
        
        elif op == "9":
            empresa = input("Nombre de la empresa: ")
            os.system(f"python client.py --empresa \"{empresa}\"")
        
        elif op == "10":
            depto = input("Departamento: ")
            os.system(f"python client.py --departamento \"{depto}\"")
        
        elif op == "11":
            turno = input("Turno (matutino/vespertino): ")
            os.system(f"python client.py --turno \"{turno}\"")
        
        elif op == "12":
            print(">> Ejecutando Pipeline de agregación por Estado...")
            os.system("python client.py --pipeline_estado")
            
        elif op == "13":
            print(">> Ejecutando Pipeline de agregación por Prioridad...")
            os.system("python client.py --pipeline_prioridad")

        elif op == "14":
            break
        
        else:
            print("Opción no válida.")

# --- MENÚ CASSANDRA (Placeholder) ---
def menu_cassandra():
    print("\n>> Módulo Cassandra: Pendiente de implementación completa.")
    # Aquí irían las llamadas a Cassandra/manager.py cuando esté listo

# --- MENÚ PRINCIPAL ---
def main_menu():
    while True:
        print("\n" + "*"*50)
        print("   SISTEMA INTEGRAL - CREPUSCULONES PANISTAS")
        print("*"*50)
        print("Seleccione la Base de Datos:")
        print("1. MongoDB (Usuarios, Operadores y Tickets)")
        print("2. Cassandra (Chat y Bitácoras)")
        print("3. Dgraph (Grafos y Relaciones)")
        print("4. Salir")
        
        opcion = input("\nOpción: ")
        
        if opcion == '1':
            menu_mongo()
        elif opcion == '2':
            menu_cassandra()
        elif opcion == '3':
            menu_dgraph()
        elif opcion == '4':
            print("Saliendo del sistema...")
            break
        else:
            print("Opción no válida.")

if __name__ == '__main__':
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\nPrograma terminado por el usuario.")