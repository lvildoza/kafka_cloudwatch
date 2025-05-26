#!/opt/prisma/pythonServiciosTI/virtualEnvironments/venv3.8/bin/python
"""
Script para obtención de métricas de Brokers de Kafka en AWS con manejo de errores.

Autor: Leandro Vildoza  
Empresa: CTL  
Fecha de creación: 01/03/2025  
Última modificación: 26/05/2025  
Versión: 1.5  

Descripción:  
- Verifica la disponibilidad de los comandos 'aws' y 'jq'.  
- Obtiene la información de los brokers de Kafka, incluyendo nombres y detalles de clústeres utilizando el perfil de AWS proporcionado.  
- Consulta métricas específicas de CloudWatch para cada broker de Kafka: KafkaDataLogsDiskUsed, CpuUser.
- Consulta métricas específicas de CloudWatch para cada Cluster de Kafka: offlinePartitionsCount.
- Genera un único mensaje consolidado con el total de métricas obtenidas.
- Imprime los resultados en formato JSON en la terminal. 

Requisitos:  
- Python 3.8+  
- AWS CLI configurado y acceso autorizado al perfil  
- Librerías necesarias: argparse, os, json, csv, boto3, shutil, datetime  

Uso:  
python disc_AWSKafka_ItemsBrokers.py <perfil_aws> <nombre_cluster>

Ejemplo:  
python disc_AWSKafka_Items.py UsrAWS_008_Acquiring_Prod ConcentradorTx-prod-cluster
"""
import argparse
import os
import json
import csv
import boto3
import shutil
from datetime import datetime, timedelta  # Importar timedelta

def zbx_json_output(profile, metric_type, zbx_exit, zbx_value, zbx_msg=None):
    messages = {
        0: "Métricas obtenidas con éxito.",
        1: "Error en la obtención de métricas.",
        2: "Información sobre ejecución: revisión necesaria."
    }
    
    output = {
        "{#INFO}": f"disc_AWSKafka_ItemsBrokers.py {profile} {metric_type}",
        "{#MSG}": zbx_msg if zbx_msg else messages.get(zbx_exit, "Mensaje no definido."),
        "{#EXIT}": str(zbx_exit),  # Convertir exit a string
        "{#REGISTROS}": str(zbx_value)  # Convertir registros a string
    }
    
    # Envolver el objeto dentro de un array "data"
    formatted_output = {"data": [output]}

    print(json.dumps(formatted_output, separators=(',', ':')))

# Verificar si los comandos 'aws' y 'jq' están disponibles
def check_commands():
    commands = ['aws', 'jq']
    for cmd in commands:
        if not shutil.which(cmd):
            print(f"No se encontro el comando {cmd}")
            return False
    return True

# Obtener métricas de AWS CloudWatch para los brokers de Kafka
def get_broker_metrics(profile, cluster_name, broker_id, broker_name):
    session = boto3.Session(profile_name=profile)
    cloudwatch = session.client('cloudwatch')
    metrics = cloudwatch.get_metric_data(
        MetricDataQueries=[
        {
            'Id': 'cpuUser',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/Kafka',
                    'MetricName': 'CpuUser',
                    'Dimensions': [
                        {'Name': 'Cluster Name', 'Value': cluster_name},
                        {'Name': 'Broker ID', 'Value': str(broker_id)}
                    ]
                },
                'Period': 60,    # período de 60 segundos
                'Stat': 'Average'
            },
            'ReturnData': True
        },
        {
            'Id': 'kafkaDataLogsDiskUsed',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/Kafka',
                    'MetricName': 'KafkaDataLogsDiskUsed',
                    'Dimensions': [
                        {'Name': 'Cluster Name', 'Value': cluster_name},
                        {'Name': 'Broker ID', 'Value': str(broker_id)}
                    ]
                },
                'Period': 60,  # período de 60 segundos
                'Stat': 'Average'
            },
            'ReturnData': True
        },
        {
            'Id': 'offlinePartitionsCount',
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/Kafka',
                    'MetricName': 'OfflinePartitionsCount',
                    'Dimensions': [
                        {'Name': 'Cluster Name', 'Value': cluster_name}
                    ]
                },
                'Period': 60,  # período de 60 segundos
                'Stat': 'Sum'
            },
            'ReturnData': True
        }
    ],
    StartTime=datetime.utcnow() - timedelta(minutes=1),  # Se obtiene solo el último minuto
    EndTime=datetime.utcnow()
    )
    return metrics

# Generar resultado en formato JSON
def generate_metrics_json(broker_metrics, cluster_name, broker_id, broker_name):
    json_data = {
        'ClusterName': cluster_name,
        'BrokerId': broker_id,
        'BrokerName': broker_name,
        'Metrics': {
            'CpuUser': [],
            'KafkaDataLogsDiskUsed': [],
            'offlinePartitionsCount': []
        }
    }

    metric_id_map = {
        'cpuUser': 'CpuUser',
        'kafkaDataLogsDiskUsed': 'KafkaDataLogsDiskUsed',
        'offlinePartitionsCount': 'offlinePartitionsCount'
    }

    for result in broker_metrics['MetricDataResults']:
        metric_id = result['Id']
        mapped_metric_id = metric_id_map.get(metric_id, metric_id)
        if result['Timestamps']:
            json_data['Metrics'][mapped_metric_id].append({
                'Timestamp': result['Timestamps'][-1].strftime('%Y-%m-%dT%H:%M:%SZ'),
                'Average': result['Values'][-1]
            })

    return json_data

# Obtener detalles de los brokers Kafka
def get_kafka_brokers(profile):
    session = boto3.Session(profile_name=profile)
    kafka = session.client('kafka')
    clusters = kafka.list_clusters()
    brokers = []
    for cluster in clusters['ClusterInfoList']:
        broker_info = kafka.list_nodes(ClusterArn=cluster['ClusterArn'])
        for broker in broker_info['NodeInfoList']:
            instance_type = broker['BrokerNodeInfo'].get('InstanceType', 'N/A')  # Usar 'N/A' si no está disponible
            broker_name = broker['BrokerNodeInfo'].get('Endpoints', ['N/A'])[0]  # Obtener el endpoint del broker
            brokers.append({
                'ClusterName': cluster['ClusterName'],
                'BrokerId': broker['BrokerNodeInfo']['BrokerId'],
                'BrokerName': broker_name,
                'InstanceType': instance_type
            })
    return brokers

# Formatear el nombre del broker para que contenga solo los dos primeros segmentos de su nombre completo (separados por puntos)
def format_broker_name(full_broker_name):
    parts = full_broker_name.split('.')
    if len(parts) >= 3:
        return '.'.join(parts[:2])  # Toma solo los dos primeros segmentos
    return full_broker_name  # Devuelve el nombre original si no tiene suficientes puntos

def print_metrics_as_json(all_metrics_json, awprofile):
    formatted_lines = {"data": []}
    offline_partitions_sum = {}  # Acumulador para las métricas de cluster

    # Agregar información general dentro de "data"
    formatted_lines["data"].append({
        "{#INFO}": f"disc_AWSKafka_ItemsBrokers.py {awprofile} Kafka",
        "{#MSG}": "Metricas obtenidas con exito.",
        "{#EXIT}": "0",
        "{#REGISTROS}": str(sum(len(broker_metrics['Metrics'][metric_name]) for broker_metrics in all_metrics_json for metric_name in broker_metrics['Metrics']))
    })

    # Procesar métricas de los brokers y acumular offlinePartitionsCount en el cluster
    for broker_metrics in all_metrics_json:
        cluster_name = broker_metrics['ClusterName']
        broker_name = format_broker_name(broker_metrics['BrokerName'])  # Aplicar formato
        broker_id = broker_metrics['BrokerId']

        for metric_name, data_points in broker_metrics['Metrics'].items():
            if metric_name == "offlinePartitionsCount":
                # Acumular la métrica offlinePartitionsCount a nivel de Cluster
                offline_partitions_sum[cluster_name] = offline_partitions_sum.get(cluster_name, 0) + sum(data_point['Average'] for data_point in data_points)
            else:
                # Procesar otras métricas normalmente
                for data_point in data_points:
                    namespace = 'Kafka'
                    value = f"{round(data_point['Average'], 2):.2f}"  # Formatear la salida con dos decimales
                    metric_data = {
                        "{#AWSPROFILE}": awprofile,
                        "{#NAMESPACE}": namespace,
                        "{#CLUSTERNAME}": cluster_name,
                        "{#BROKERNAME}": broker_name,
                        "{#BROKERID}": str(broker_id),
                        "{#METRICNAME}": metric_name,
                        "{#VALUE}": value,
                        "{#METRICUNIT}": "%",
                        "{#VALUETYPE}": "Average"
                    }
                    formatted_lines["data"].append(metric_data)

    # Agregar la métrica offlinePartitionsCount separado por cluster
    for cluster, total_value in offline_partitions_sum.items():
        formatted_lines["data"].append({
            "{#AWSPROFILE}": awprofile,
            "{#NAMESPACE}": "Kafka",
            "{#CLUSTERNAME}": cluster,
            "{#METRICNAME}": "offlinePartitionsCount",
            "{#VALUE}": f"{round(total_value, 2):.2f}",
            "{#VALUETYPE}": "Sum"
        })

    print(json.dumps(formatted_lines, separators=(',', ':')))

# Función principal del script

def main():
    parser = argparse.ArgumentParser(description="Obtención de métricas AWS Kafka")
    parser.add_argument('awprofile', help="Perfil de AWS")
    parser.add_argument('clustername', nargs='?', default=None, help="Nombre del cluster para filtrar métricas (opcional)")
    parser.add_argument('awsaccount', nargs='?', default="AWS", help="Cuenta de AWS")
    args = parser.parse_args()

    awprofile = args.awprofile
    clustername_filter = args.clustername  # Ahora puede ser None
    awsaccount = args.awsaccount

    # Obtener todos los brokers de Kafka
    brokers = get_kafka_brokers(awprofile)

    # Si el usuario **pasó** un cluster, filtrar por su nombre
    if clustername_filter:
        brokers = [broker for broker in brokers if broker['ClusterName'] == clustername_filter]

    # Si **no hay brokers** después del filtro, mostrar mensaje con exit=2
    if not brokers:
        zbx_json_output(awprofile, "Kafka", 2, 0, f"No se encontraron brokers para el cluster '{clustername_filter}'")
        return

    total_records = 0
    all_metrics_json = []

    for broker in brokers:
        broker_metrics = get_broker_metrics(awprofile, broker['ClusterName'], broker['BrokerId'], broker['BrokerName'])
        metrics_json = generate_metrics_json(broker_metrics, broker['ClusterName'], broker['BrokerId'], broker['BrokerName'])
        all_metrics_json.append(metrics_json)

    for broker_metrics in all_metrics_json:
        for metric_name, data_points in broker_metrics['Metrics'].items():
            total_records += len(data_points)

    # Mostrar salida filtrada con el argumento awprofile corregido
    # zbx_json_output(awprofile, "Kafka", 0, total_records)
    print_metrics_as_json(all_metrics_json, awprofile)

if __name__ == "__main__":
    main()