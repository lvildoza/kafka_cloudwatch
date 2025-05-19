#!/opt/prisma/pythonServiciosTI/virtualEnvironments/venv3.8/bin/python

"""
Script para obtención de métricas de clústeres de Kafka en AWS y guardado en archivos JSON y CSV.

Autor: Leandro Vildoza
Empresa: CTL
Fecha de creación: 01/03/2025
Versión: 1.0

Descripción:
- Verifica la disponibilidad de los comandos 'aws' y 'jq'.
- Obtiene nombres y ARN de clústeres de Kafka utilizando el perfil de AWS proporcionado.
- Obtiene métricas de particiones offline de CloudWatch para cada clúster.
- Genera un archivo JSON con las métricas y la información de los brokers de cada clúster.
- Guarda las métricas en archivos JSON y CSV.
- Guarda eventos importantes en un archivo de log.

Requisitos:
- Python 3.8+
- AWS CLI configurado
- Librerías: argparse, os, json, csv, boto3, shutil, datetime

Uso:
python script_cluster_kafka.py <perfil_aws>
"""

import argparse
import os
import json
import csv
import boto3
import shutil
from datetime import datetime, timedelta  # Importar timedelta

awsmetrictype = 'kafka'

# Verifica si los comandos 'aws' y 'jq' están disponibles en el sistema.
def check_commands():
    commands = ['aws', 'jq']
    for cmd in commands:
        if not shutil.which(cmd):
            print(f"No se encontró el comando {cmd}")
            log_message(f"No se encontró el comando {cmd}")
            return False
    return True

# Obtiene una lista de nombres de clústeres de Kafka utilizando el perfil de AWS proporcionado.
def get_cluster_names(profile):
    session = boto3.Session(profile_name=profile)
    kafka = session.client(awsmetrictype)
    response = kafka.list_clusters()
    cluster_names = [cluster['ClusterName'] for cluster in response['ClusterInfoList']]
    return cluster_names

# Obtiene el ARN de un clúster de Kafka dado su nombre y el perfil de AWS proporcionado.
def get_cluster_arn(profile, cluster_name):
    session = boto3.Session(profile_name=profile)
    kafka = session.client(awsmetrictype)
    response = kafka.list_clusters()
    for cluster in response['ClusterInfoList']:
        if cluster['ClusterName'] == cluster_name:
            return cluster['ClusterArn']
    return None

# Obtiene métricas de particiones offline de CloudWatch para un clúster de Kafka.
def get_cluster_metrics(profile, cluster_name):
    session = boto3.Session(profile_name=profile)
    cloudwatch = session.client('cloudwatch')
    metrics = cloudwatch.get_metric_data(
        MetricDataQueries=[
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
                    'Period': 60,
                    'Stat': 'Sum'
                },
                'ReturnData': True
            }
        ],
        StartTime=datetime.utcnow() - timedelta(minutes=1),
        EndTime=datetime.utcnow()
    )
    return metrics

# Genera un archivo JSON con las métricas y la información de los brokers de un clúster.
def generate_metrics_json(cluster_metrics, cluster_name, brokers):
    json_data = {
        'ClusterName': cluster_name,
        'Brokers': brokers,
        'MetricsCluster': {
            'OfflinePartitionsCount': []
        }
    }

    for result in cluster_metrics['MetricDataResults']:
        for i in range(len(result['Timestamps'])):
            json_data['MetricsCluster']['OfflinePartitionsCount'].append({
                'Timestamp': result['Timestamps'][i].strftime('%Y-%m-%dT%H:%M:%SZ'),
                'Sum': result['Values'][i]
            })

    return json_data

# Obtiene la información de los brokers de un clúster de Kafka utilizando el ARN y el perfil de AWS proporcionado.
def get_cluster_brokers(profile, cluster_arn):
    session = boto3.Session(profile_name=profile)
    kafka = session.client(awsmetrictype)
    response = kafka.list_nodes(ClusterArn=cluster_arn)
    
    brokers = [
        {
            'BrokerID': node['BrokerNodeInfo']['BrokerId'], 
            'BrokerName': node['BrokerNodeInfo']['Endpoints'][0]
        }
        for node in response['NodeInfoList']
    ]
    return brokers

# Guarda los datos en formato JSON en el archivo especificado.
def save_json_to_file(data, file_path):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)

# Guarda los datos en formato CSV en el archivo especificado.
def save_csv(data, file_path):
    with open(file_path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=';')
        csvwriter.writerow(['Namespace', 'MetricName', 'Dimensions', 'Value', 'Sum'])
        for cluster_data in data:
            cluster_name = cluster_data['ClusterName']
            for metric in cluster_data['MetricsCluster']['OfflinePartitionsCount']:
                # timestamp = metric['Timestamp']
                sum_value = metric['Sum']
                csvwriter.writerow(['AWS/Kafka', 'OfflinePartitionsCount', 'ClusterName', cluster_name, sum_value])

# Guarda un mensaje en el archivo de log especificado.
def log_message(message):
    with open(log_file, 'a') as log:
        log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

# Función principal que coordina la ejecución del script.
def main():
    parser = argparse.ArgumentParser(description="Obtención de métricas AWS Kafka")
    parser.add_argument('awprofile', help="Perfil de AWS")
    parser.add_argument('awsaccount', nargs='?', default="AWS", help="Cuenta de AWS")
    args = parser.parse_args()

    global log_file
    awprofile = args.awprofile
    awstype = 'Kafka'
    awsmetric = 'Cluster'
    awsaccount = args.awsaccount
    script_dir = "/usr/lib/nagios/plugins/aws_integration"
    
    if not os.path.exists(script_dir):
        script_dir = os.path.dirname(os.path.abspath(__file__))
    
    log_file = os.path.join(script_dir, 'logs', f"{datetime.now().strftime('%Y-%m-%d')}_script_{awstype}.{awsmetric}.log")

# Abrir el archivo de log en modo escritura ('w') para regenerarlo en cada ejecución
    with open(log_file, 'w') as log:
        log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Inicio del script\n")

    if not check_commands():
        log_message("Error: Comandos necesarios no encontrados.")
        exit(1)
    
    cluster_names = get_cluster_names(awprofile)

    all_metrics_json = []
    for cluster_name in cluster_names:
        cluster_arn = get_cluster_arn(awprofile, cluster_name)
        if not cluster_arn:
            message = f"No se pudo obtener el ARN del clúster: {cluster_name}"
            print(message)
            log_message(message)
            continue
        cluster_metrics = get_cluster_metrics(awprofile, cluster_name)
        brokers = get_cluster_brokers(awprofile, cluster_arn)
        metrics_json = generate_metrics_json(cluster_metrics, cluster_name, brokers)
        all_metrics_json.append(metrics_json)
        log_message(f"Metricas obtenidas para el cluster: {cluster_name}")
    
    print(json.dumps(all_metrics_json, indent=4))
    
    json_output_path = os.path.join(script_dir, 'dbs/queries', f"{awprofile}.{awstype}.{awsmetric}.queries.json")
    save_json_to_file(all_metrics_json, json_output_path)
    log_message(f"Datos JSON guardados en {json_output_path}")

    csv_output_path = os.path.join(script_dir, 'dbs/csv', f"{awprofile}.{awstype}.{awsmetric}.metrics.csv")
    save_csv(all_metrics_json, csv_output_path)
    log_message(f"Datos CSV guardados en {csv_output_path}")

if __name__ == "__main__":
    main()