import connect
import json
from Dgraph import manager as dgraph_manager
from Cassandra import manager as cass_manager

def print_json(data):
    print(json.dumps(data, indent=2, ensure_ascii=False, default=str))

def menu_cassandra(session):
    if not session:
        print("No hay conexión con Cassandra.")
        return

    while True:
        print("\n" + "="*50)
        print("      CASSANDRA")
        print("="*50)
        print("1.  Nuevo mensaje de chat")
        print("2.  Ver historial de chat del ticket")
        print("3.  Cambiar estado ticket")
        print("4.  Ver historial estados ticket")
        print("5.  Ver estado actual")
        print("6.  Registrar participación agente")
        print("7.  Consultar participantes ticket")
        print("8.  Auditoría diaria")
        print("9.  Reporte de rendimiento diario")
        print("10. Volver")
        
        op = input("\nSelecciona una opción: ")
        
        if op == '1':
            cass_manager.register_message(
                session, 
                input("Ticket ID: "), 
                input("Autor ID: "), 
                input("Mensaje: "))
            print("Mensaje guardado.")
            
        elif op == '2':
            res = cass_manager.get_chat_history(
                session, 
                input("Ticket ID: "))
            print_json(res)
            
        elif op == '3':
            cass_manager.update_ticket_status(
                session, 
                input("Ticket ID: "), 
                input("Nuevo Estado: "), 
                input("Agente ID: "), 
                input("Detalles: "))
            print("Estado actualizado.")
            
        elif op == '4':
            res = cass_manager.get_status_history(
                session, 
                input("Ticket ID: "))
            print_json(res)
            
        elif op == '5':
            res = cass_manager.get_current_status(
                session, 
                input("Ticket ID: "))
            print(f"Estado Actual: {res}")
            
        elif op == '6':
            cass_manager.register_participation(
                session, 
                input("Ticket ID: "), 
                input("Agente ID: "), 
                input("Acción: "))
            print("Participación registrada.")
            
        elif op == '7':
            res = cass_manager.get_participants(
                session, 
                input("Ticket ID: "))
            print_json(res)
            
        elif op == '8':
            d = input("Fecha (YYYY-MM-DD): ")
            res = cass_manager.get_daily_audit(session, d if d else None)
            print_json(res)

        elif op == '9':
            d = input("Fecha (YYYY-MM-DD): ")
            res = cass_manager.get_daily_performance(session, d if d else None)
            print_json(res)
            
        elif op == '10':
            break

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
            # CORRECCIÓN: usar dgraph_manager en lugar de manager
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

def main_menu():
    # conexiones
    dgraph_stub = connect.create_client_stub()
    dgraph_client = connect.create_client(dgraph_stub)
    cass_session = connect.create_cassandra_session()

    while True:
        print("\n" + "*"*50)
        print("   SISTEMA INTEGRAL - CREPUSCULONES PANISTAS")
        print("*"*50)
        print("1. MongoDB (Usuarios y Tickets) [No disp]")
        print("2. Cassandra (Chat y Bitácoras)")
        print("3. Dgraph (Grafos y Relaciones)")
        print("4. Salir")
        
        opcion = input("\nOpción: ")
        
        if opcion == '1':
            print(">> Módulo MongoDB no disponible aún.")
        elif opcion == '2':
            menu_cassandra(cass_session)
        elif opcion == '3':
            from main import menu_dgraph as run_dgraph_menu
            run_dgraph_menu() 
        elif opcion == '4':
            print("Saliendo...")
            dgraph_stub.close()
            if cass_session: cass_session.shutdown()
            break
        else:
            print("Opción no válida.")

if __name__ == '__main__':
    try:
        main_menu()
    except KeyboardInterrupt:
        print("\nPrograma terminado.")