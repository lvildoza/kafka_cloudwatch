#!/opt/prisma/pythonServiciosTI/virtualEnvironments/venv3.8/bin/python
"""
Script para obtención de métricas de Brokers de Kafka en AWS con manejo de errores.

Autor: Leandro Vildoza  
Empresa: CTL  
Fecha de creación: 01/03/2025  
Última modificación: 27/05/2025  
Versión: 1.6  

Descripción:  
- Verifica la disponibilidad de los comandos 'aws' y 'jq'.  
- Obtiene la información de los brokers de Kafka, incluyendo nombres y el perfil de AWS proporcionado.  
- Genera un único mensaje consolidado con el total de métricas obtenidas.
- Imprime los resultados en formato JSON en la terminal. 

Requisitos:  
- Python 3.8+  
- AWS CLI configurado y acceso autorizado al perfil  
- Librerías necesarias: argparse, os, json, boto3, shutil  

Uso:  
python disc_AWSKafka_Brokers.py <perfil_aws>

Ejemplo:  
python disc_AWSKafka_Brokers.py UsrAWS_008_Acquiring_Prod
"""

import argparse
import json
import boto3
import shutil

# Formatear el nombre del broker para que contenga solo los dos primeros segmentos de su nombre completo
def format_broker_name(full_broker_name):
    parts = full_broker_name.split('.')
    if len(parts) >= 3:
        return '.'.join(parts[:2])  # Toma solo los dos primeros segmentos
    return full_broker_name  # Devuelve el nombre original si no tiene suficientes puntos

# Función para salida JSON con manejo de errores
def zbx_json_output(profile, zbx_exit, zbx_value, zbx_msg=None):
    messages = {
        0: "Brokers obtenidos con exito.",
        1: "Error en la obtencion de brokers.",
        2: "Información sobre ejecucion: revision necesaria."
    }
    
    output = {
        "{#INFO}": f"disc_AWSKafka_ItemsBrokers.py {profile} Kafka",
        "{#MSG}": zbx_msg if zbx_msg else messages.get(zbx_exit, "Mensaje no definido."),
        "{#EXIT}": str(zbx_exit),
        "{#REGISTROS}": str(zbx_value)
    }
    
    return output

# Verificar si los comandos 'aws' y 'jq' están disponibles
def check_commands():
    commands = ['aws', 'jq']
    for cmd in commands:
        if not shutil.which(cmd):
            print(f"No se encontro el comando {cmd}")
            return False
    return True

# Obtener detalles de los brokers en los clusters de Kafka
def get_kafka_brokers(profile):
    session = boto3.Session(profile_name=profile)
    kafka = session.client('kafka')

    try:
        clusters_response = kafka.list_clusters()
    except Exception as e:
        return None, f"Error obteniendo clusters: {str(e)}"

    brokers = []
    for cluster in clusters_response.get('ClusterInfoList', []):
        try:
            broker_info = kafka.list_nodes(ClusterArn=cluster['ClusterArn'])
        except Exception as e:
            return None, f"Error obteniendo brokers para el cluster {cluster['ClusterName']}: {str(e)}"

        for broker in broker_info.get('NodeInfoList', []):
            full_broker_name = broker['BrokerNodeInfo'].get('Endpoints', ['N/A'])[0]
            broker_name = format_broker_name(full_broker_name)  # Aplicar formato
            broker_id = broker['BrokerNodeInfo']['BrokerId']
            brokers.append({
                "ClusterName": cluster['ClusterName'],
                "BrokerName": broker_name,
                "BrokerId": str(broker_id)
            })

    if not brokers:
        return None, "No se encontraron brokers disponibles."

    return brokers, None

# Generar salida JSON con los brokers formateados
def print_brokers_as_json(brokers, awprofile, error_msg=None):
    formatted_output = {"data": []}

    # Agregar estado de ejecución
    total_records = len(brokers) if brokers else 0
    formatted_output["data"].append(zbx_json_output(awprofile, 0 if brokers else 2, total_records, error_msg))

    # Agregar los brokers con nombres formateados
    if brokers:
        for broker in brokers:
            formatted_output["data"].append({
                "{#AWSPROFILE}": awprofile,
                "{#NAMESPACE}": "AWS/Kafka",
                "{#BROKERNAME}": broker["BrokerName"],
                "{#BROKERID}": broker["BrokerId"]
            })

    print(json.dumps(formatted_output, separators=(',', ':')))

# Función principal
def main():
    parser = argparse.ArgumentParser(description="Obtencion de brokers en AWS Kafka")
    parser.add_argument('awprofile', help="Perfil de AWS")
    parser.add_argument('clustername', nargs='?', default=None, help="Nombre del cluster para filtrar brokers (opcional)")
    args = parser.parse_args()

    awprofile = args.awprofile
    clustername_filter = args.clustername

    # Obtener todos los brokers
    brokers, error_msg = get_kafka_brokers(awprofile)

    # Filtrar si se especificó un cluster
    if clustername_filter and brokers:
        brokers = [broker for broker in brokers if broker["ClusterName"] == clustername_filter]

    # Imprimir la salida JSON con la estructura corregida
    print_brokers_as_json(brokers, awprofile, error_msg)

if __name__ == "__main__":
    if not check_commands():
        exit(1)
    main()