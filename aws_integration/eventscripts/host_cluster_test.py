#!/opt/prisma/pythonServiciosTI/virtualEnvironments/venv3.8/bin/python
import argparse
import json
import boto3
import shutil

# Función para salida JSON con manejo de errores
def zbx_json_output(profile, zbx_exit, zbx_value, zbx_msg=None):
    messages = {
        0: "Clusters obtenidos con exito.",
        1: "Error en la obtencion de clusters.",
        2: "Información sobre ejecucion: revisión necesaria."
    }
    
    output = {
        "{#INFO}": f"disc_AWSKafka_HostsCluster.py {profile} Kafka",
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

# Obtener nombres de los clusters de Kafka
def get_kafka_clusters(profile):
    session = boto3.Session(profile_name=profile)
    kafka = session.client('kafka')

    try:
        clusters_response = kafka.list_clusters()
    except Exception as e:
        return None, f"Error obteniendo clusters: {str(e)}"

    clusters = [cluster['ClusterName'] for cluster in clusters_response.get('ClusterInfoList', [])]

    if not clusters:
        return None, "No se encontraron clusters disponibles."

    return clusters, None

# Generar salida JSON sin métricas
def print_clusters_as_json(clusters, awprofile, error_msg=None):
    formatted_output = {"data": []}

    # Agregar estado de ejecución
    total_records = len(clusters) if clusters else 0
    formatted_output["data"].append(zbx_json_output(awprofile, 0 if clusters else 2, total_records, error_msg))

    # Agregar los clusters sin métricas
    if clusters:
        for cluster in clusters:
            formatted_output["data"].append({
                "{#AWSPROFILE}": awprofile,
                "{#NAMESPACE}": "AWS/Kafka",
                "{#CLUSTERNAME}": cluster
            })

    print(json.dumps(formatted_output, separators=(',', ':')))

# Función principal
def main():
    parser = argparse.ArgumentParser(description="Obtencion de clusters AWS Kafka")
    parser.add_argument('awprofile', help="Perfil de AWS")
    parser.add_argument('clustername', nargs='?', default=None, help="Nombre del cluster para filtrar (opcional)")
    args = parser.parse_args()

    awprofile = args.awprofile
    clustername_filter = args.clustername

    # Obtener todos los clusters
    clusters, error_msg = get_kafka_clusters(awprofile)

    # Filtrar si se especificó un cluster
    if clustername_filter and clusters:
        clusters = [cluster for cluster in clusters if cluster == clustername_filter]

    # Imprimir la salida con la estructura JSON corregida
    print_clusters_as_json(clusters, awprofile, error_msg)

if __name__ == "__main__":
    if not check_commands():
        exit(1)
    main()