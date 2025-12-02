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

Inicializar las bases de datos (Cassandra, MongoDB, Dgraph) en docker:
```bash
#Crear espacio para Cassandra si no existe:
docekr run --name cassandra-dev -p 9042:9042 -d cassandra:latest

#Si ya tienes uno inicialo con el siguiente comando:
docker start cassandra-dev

#Crear espacio para MongoDB si no existe:
docker run --name mongodb -p 27017:27017 -d mongo:latest

#Si ya tienes uno inicialo con el siguiente comando:
docker start mongodb

#Crear espacio para Dgraph si no existe:
docker run --name dgraph -p 8080:8080 -p 9080:9080 -d dgraph/standalone:latest

#Si ya tienes uno inicialo con el siguiente comando:
docker start dgraph

#Si quieres descargar el GUI de dgraph es con el siguiente comando:
docker run --name ratel -p 8000:8000 
```

Iniciar el programa debe serguir los siguientes pasos:
```bash
#Cargar los datos con el populate.py:
python populate.py

#Ya cargados los datos y tener los contenedores activos se corre el main:
python main.py
```