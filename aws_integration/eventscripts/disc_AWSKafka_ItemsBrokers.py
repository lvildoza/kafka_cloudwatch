#!/opt/prisma/pythonServiciosTI/virtualEnvironments/venv3.8/bin/python
"""
Script para obtención de métricas de Brokers de Kafka en AWS y guardado en archivos JSON y CSV.

Autor: Leandro Vildoza  
Empresa: CTL  
Fecha de creación: 01/03/2025  
Última modificación: 18/05/2025  
Versión: 1.3  

Descripción:  
- Verifica la disponibilidad de los comandos 'aws' y 'jq'.  
- Obtiene la información de los brokers de Kafka, incluyendo nombres y detalles de clústeres utilizando el perfil de AWS proporcionado.  
- Consulta métricas específicas de CloudWatch para cada broker de Kafka: CpuUser, KafkaAppLogsDiskUsed.  
- Genera un único mensaje consolidado con el total de métricas obtenidas.  
- Genera archivos JSON con las métricas procesadas.  
- Guarda las métricas en archivos CSV para su análisis posterior.  
- Imprime los resultados en formato JSON en la terminal.  
- Registra eventos importantes en un archivo de log.

Requisitos:  
- Python 3.8+  
- AWS CLI configurado y acceso autorizado al perfil  
- Librerías necesarias: argparse, os, json, csv, boto3, shutil, datetime  

Uso:  
python disc_AWSKafka_ItemsBrokers.py <perfil_aws>  

Ejemplo:  
python disc_AWSKafka_ItemsBrokers.py UsrAWS_008_Acquiring_Prod kafka  
"""
import argparse
import os
import json
import csv
import boto3
import shutil
from datetime import datetime, timedelta  # Importar timedelta

# Función para el manejo de logs
def zbx_json_output(profile, metric_type, zbx_msg, zbx_exit, zbx_value):
    output = {
        "info": f"disc_AWSKafka_ItemsBrokers.py {profile} {metric_type}",
        "msg": zbx_msg,
        "exit": zbx_exit,
        "registros": zbx_value
    }
    print(json.dumps(output))

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

# Guardar resultado en un archivo CSV
def save_metrics_to_csv(metrics_json, filepath):
    with open(filepath, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=':')
        writer.writerow(['Namespace', 'ClusterName', 'BrokerName', 'BrokerID', 'MetricName', 'Dimensions', 'Value'])
        for broker_metrics in metrics_json:
            cluster_name = broker_metrics['ClusterName']
            broker_name = broker_metrics['BrokerName']
            broker_id = broker_metrics['BrokerId']
            for metric_name, data_points in broker_metrics['Metrics'].items():
                for data_point in data_points:
                    namespace = 'Kafka'
                    dimensions = f'#NAMESPACE:{namespace}:#CLUSTERNAME:{cluster_name}:#BROKERNAME:{broker_name}:#BROKERID:{broker_id}:#METRICNAME:{metric_name}'
                    value = data_point['Average']
                    writer.writerow([namespace, cluster_name, broker_name, str(broker_id), metric_name, str(value), dimensions, str(value), '#METRICUNIT:%', '#VALUETYPE:Average'])

## ANALIZAR SI ESTAS FUNCIONES SE MANTIENEN

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

# Guardar resultado JSON en un archivo
def save_json_to_file(json_data, filepath):
    with open(filepath, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)

# Mostrar resultado en formato JSON con cada línea como un string en el formato original
def print_metrics_as_json(all_metrics_json):
    formatted_lines = {"data": []}
    for broker_metrics in all_metrics_json:
        cluster_name = broker_metrics['ClusterName']
        broker_name = broker_metrics['BrokerName']
        broker_id = broker_metrics['BrokerId']
        for metric_name, data_points in broker_metrics['Metrics'].items():
            for data_point in data_points:
                namespace = 'Kafka'
                value = data_point['Average']
                metric_data = {
                    "{#NAMESPACE}": namespace,
                    "{#CLUSTERNAME}": cluster_name,
                    "{#BROKERNAME}": broker_name,
                    "{#BROKERID}": str(broker_id),
                    "{#METRICNAME}": metric_name,
                    "{#VALUE}": str(value),
                    "{#METRICUNIT}": "%",
                    "{#VALUETYPE}": "Average"
                }
                formatted_lines["data"].append(metric_data)
    
    print(json.dumps(formatted_lines, separators=(',', ':')))

# Función principal del script
def main():
    parser = argparse.ArgumentParser(description="Obtención de métricas AWS Kafka")
    parser.add_argument('awprofile', help="Perfil de AWS")
    parser.add_argument('awsaccount', nargs='?', default="AWS", help="Cuenta de AWS")
    args = parser.parse_args()

    awsmetric = "Kafka"
    awprofile = args.awprofile
    awsaccount = args.awsaccount
    script_dir = "/usr/lib/nagios/plugins/aws_integration"
    
    if not os.path.exists(script_dir):
        script_dir = os.path.dirname(os.path.abspath(__file__))
    
    log_file = os.path.join(script_dir, 'logs', f"{datetime.now().strftime('%Y-%m-%d')}_disc_AWSKafka_ItemsBrokers.log")

    # Verificar comandos
    if not check_commands():
        exit(1)
    
    # Obtener brokers Kafka
    brokers = get_kafka_brokers(awprofile)

    total_records = 0  # Variable para acumular el total de métricas

    all_metrics_json = []
    for broker in brokers:
        broker_metrics = get_broker_metrics(awprofile, broker['ClusterName'], broker['BrokerId'], broker['BrokerName'])
        metrics_json = generate_metrics_json(broker_metrics, broker['ClusterName'], broker['BrokerId'], broker['BrokerName'])
        all_metrics_json.append(metrics_json)
    
    for broker_metrics in all_metrics_json:
        for metric_name, data_points in broker_metrics['Metrics'].items():
            total_records += len(data_points)  # Contar cada línea de datos (métricas recolectadas)
    

    # Mostrar mensaje único con el total de métricas obtenidas
    zbx_json_output(awprofile, awsmetric, "Metricas obtenidas con exito.", "0", total_records)
    
    # Mostrar resultado en formato CSV en pantalla
    # print_metrics_as_csv(all_metrics_json)
    print_metrics_as_json(all_metrics_json)


    # Guardar resultado en un archivo JSON
    json_output_path = os.path.join(script_dir, 'dbs/queries', f"{awprofile}.{awsmetric}.queries.json")
    save_json_to_file(all_metrics_json, json_output_path)
    
    # Guardar resultado en un archivo CSV
    csv_output_path = os.path.join(script_dir, 'dbs/csv', f"{awprofile}.{awsmetric}.metrics.csv")
    save_metrics_to_csv(all_metrics_json, csv_output_path)

if __name__ == "__main__":
    main()