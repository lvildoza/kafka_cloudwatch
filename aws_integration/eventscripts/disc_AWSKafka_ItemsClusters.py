#!/opt/prisma/pythonServiciosTI/virtualEnvironments/venv3.8/bin/python
"""
Script para obtención de métricas de Cluster de Kafka en AWS con manejo de errores.

Autor: Leandro Vildoza  
Empresa: CTL  
Fecha de creación: 01/03/2025  
Última modificación: 27/05/2025  
Versión: 1.6

Descripción:  
- Verifica la disponibilidad de los comandos 'aws' y 'jq'.  
- Obtiene la información de los clusters de Kafka, incluyendo nombres y detalles de clústeres utilizando el perfil de AWS proporcionado.  
- Consulta métricas específicas de CloudWatch para cada Cluster de Kafka: offlinePartitionsCount.
- Genera un único mensaje consolidado con el total de métricas obtenidas.
- Imprime los resultados en formato JSON en la terminal. 

Requisitos:  
- Python 3.8+  
- AWS CLI configurado y acceso autorizado al perfil  
- Librerías necesarias: argparse, os, json, csv, boto3, shutil, datetime  

Uso:  
python disc_AWSKafka_ItemsClusters.py <perfil_aws> <nombre_cluster>

Ejemplo:  
python disc_AWSKafka_ItemsClusters.py UsrAWS_008_Acquiring_Prod ConcentradorTx-prod-cluster
"""
import argparse
import json
import boto3
import shutil
from datetime import datetime, timedelta

# Función para salida JSON con manejo de errores
def zbx_json_output(profile, metric_type, zbx_exit, zbx_value, zbx_msg=None):
    messages = {
        0: "Metricas obtenidas con exito.",
        1: "Error en la obtención de metricas.",
        2: "Informacion sobre ejecución: revision necesaria."
    }
    
    output = {
        "{#INFO}": f"disc_AWSKafka_ItemsClusters.py {profile} {metric_type}",
        "{#MSG}": zbx_msg if zbx_msg else messages.get(zbx_exit, "Mensaje no definido."),
        "{#EXIT}": str(zbx_exit),
        "{#REGISTROS}": str(zbx_value)
    }
    
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

# Obtener detalles de los clusters de Kafka
def get_kafka_clusters(profile):
    session = boto3.Session(profile_name=profile)
    kafka = session.client('kafka')

    try:
        clusters_response = kafka.list_clusters()
    except Exception as e:
        zbx_json_output(profile, "Kafka", 1, 0, f"Error obteniendo clusters: {str(e)}")
        return []

    clusters = [cluster['ClusterName'] for cluster in clusters_response.get('ClusterInfoList', [])]
    
    if not clusters:
        zbx_json_output(profile, "Kafka", 2, 0, "No se encontraron clusters disponibles.")
    
    return clusters

# Obtener métricas de CloudWatch para los clusters
def get_cluster_metrics(profile, cluster_name):
    session = boto3.Session(profile_name=profile)
    cloudwatch = session.client('cloudwatch')

    try:
        metrics = cloudwatch.get_metric_data(
            MetricDataQueries=[
                {
                    'Id': 'offlinePartitionsCount',
                    'MetricStat': {
                        'Metric': {
                            'Namespace': 'AWS/Kafka',
                            'MetricName': 'OfflinePartitionsCount',
                            'Dimensions': [{'Name': 'Cluster Name', 'Value': cluster_name}]
                        },
                        'Period': 60,
                        'Stat': 'Sum'
                    },
                    'ReturnData': True
                }
            ],
            StartTime=datetime.utcnow() - timedelta(minutes=1),
            EndTime=datetime.utcnow()
        )
    except Exception as e:
        zbx_json_output(profile, "Kafka", 1, 0, f"Error obteniendo metricas: {str(e)}")
        return None

    return metrics

# Generar resultado en formato JSON
def generate_metrics_json(cluster_metrics, cluster_name):
    json_data = {
        'ClusterName': cluster_name,
        'Metrics': {'offlinePartitionsCount': []}
    }

    if not cluster_metrics or not cluster_metrics.get('MetricDataResults'):
        return json_data

    for result in cluster_metrics['MetricDataResults']:
        if result['Timestamps']:
            json_data['Metrics']['offlinePartitionsCount'].append({
                'Timestamp': result['Timestamps'][-1].strftime('%Y-%m-%dT%H:%M:%SZ'),
                'Average': result['Values'][-1]
            })

    return json_data

# Generar resultado en formato JSON con el formato esperado
def print_metrics_as_json(all_metrics_json, awprofile):
    formatted_lines = {"data": []}
    offline_partitions_sum = {}  # Acumulador para las métricas de cada cluster
    total_records = 0  # Contador de métricas obtenidas

    # Procesar métricas de los clusters y acumular offlinePartitionsCount por cluster
    for cluster_metrics in all_metrics_json:
        cluster_name = cluster_metrics['ClusterName']
        total_value = sum(
            data_point['Average']
            for data_points in cluster_metrics['Metrics'].values()
            for data_point in data_points
        )

        offline_partitions_sum[cluster_name] = total_value
        total_records += len(cluster_metrics['Metrics']['offlinePartitionsCount'])

    # **Agregar información general de ejecución en `data`**
    formatted_lines["data"].append({
        "{#INFO}": f"disc_AWSKafka_ItemsClusters.py {awprofile} Kafka",
        "{#MSG}": "Metricas obtenidas con exito.",
        "{#EXIT}": "0",
        "{#REGISTROS}": str(total_records)
    })

    # **Agregar métricas de cada cluster**
    for cluster, total_value in offline_partitions_sum.items():
        formatted_lines["data"].append({
            "{#AWSPROFILE}": awprofile,
            "{#NAMESPACE}": "AWS/Kafka",
            "{#CLUSTERNAME}": cluster,
            "CLUSTERMETRIC": "offlinePartitionsCount",
            "CLUSTERVALUE": f"{round(total_value, 2):.2f}",
            "CLUSTERVALUETYPE": "Sum"
        })

    print(json.dumps(formatted_lines, separators=(',', ':')))

# Función principal
def main():
    parser = argparse.ArgumentParser(description="Obtencion de metricas AWS Kafka")
    parser.add_argument('awprofile', help="Perfil de AWS")
    parser.add_argument('clustername', nargs='?', default=None, help="Nombre del cluster para filtrar metricas (opcional)")
    args = parser.parse_args()

    awprofile = args.awprofile
    clustername_filter = args.clustername

    # Obtener todos los clusters
    clusters = get_kafka_clusters(awprofile)

    # Filtrar si se especificó un cluster
    if clustername_filter:
        clusters = [cluster for cluster in clusters if cluster == clustername_filter]

    if not clusters:
        return

    all_metrics_json = []

    for cluster_name in clusters:
        cluster_metrics = get_cluster_metrics(awprofile, cluster_name)
        metrics_json = generate_metrics_json(cluster_metrics, cluster_name)
        all_metrics_json.append(metrics_json)

    print_metrics_as_json(all_metrics_json, awprofile)

if __name__ == "__main__":
    if not check_commands():
        exit(1)
    main()