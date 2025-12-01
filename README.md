# Crepusculones-Panistas
Integrantes: <br>
Carlos Ariel Rubio Espinosa - 752504 <br>
Rogelio Sosa Luna - 751476 <br>
Ana Kamila Carrasco Torres - 750395 <br>

Descripción:

El objetivo es desarrollar un sistema de soporte al cliente que centralice toda la
información en un solo lugar, facilitando la gestión de las incidencias de los usuarios.
La plataforma se encargará de:

• Almacenar: Perfiles de clientes, tickets y detalles de cada problema. <br>
• Relacionar: Vincular a los clientes con los agentes de soporte y sus incidencias
correspondientes. <br>
• Registrar: Guardar un historial completo de cada conversación, respuesta y
cambio de estado durante todo el proceso. <br>

Se utilizarán las bases de datos MongoDB, Dgraph y Cassandra para construir una
solución robusta y ordenada que mejore tanto el trabajo del equipo de soporte como
la experiencia del cliente

<br>

Requerimientos:
Inicializar un entorno virtual de python con env e instalar las librerias necesarias:
```bash
#Si no tienes instalado pip en tu sitema:
sudo apt update
sudo apt install python3-pip

#Instalar y activar el ambiente virtual (MacOS/Linux)
python3 -m pip install virtualenv
python3 -m venv ./env
source ./env/bin/activate

#Instalar y activar el ambiente virtual (Windows)
python3 -m pip install virtualenv
python3 -m venv ./env
.\env\Scripts\Activate.ps1

#Instalar los requerimientos del proyecto
pip install -r requirements.txt
```