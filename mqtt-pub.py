import paho.mqtt.client as mqtt
import csv
import argparse
import time

BROKER_ADDRESS  = "localhost"
BROKER_PORT = 8883
MQTT_TOPIC = "idc/fc04"

CSV_FILE_PATH = "online.data.csv"

def publish_data(weight, height, gender, age):
    # Configurar o cliente MQTT
    client = mqtt.Client()
    
    client.tls_set(ca_certs="caMQTT.crt",
    certfile="client.crt",
    keyfile="client.key")
    client.tls_insecure_set(True)
    
    client.connect(BROKER_ADDRESS, BROKER_PORT, 60)

    # Abrir o arquivo CSV e ler linha a linha
    with open(CSV_FILE_PATH, 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:
            # Converter a linha para JSON ou string
            message = {
                "acceleration_x": row["acceleration_x"],
                "acceleration_y": row["acceleration_y"],
                "acceleration_z": row["acceleration_z"],
                "gyro_x": row["gyro_x"],
                "gyro_y": row["gyro_y"],
                "gyro_z": row["gyro_z"],
                "weight": weight,
                "height": height,
                "gender": gender,
                "age": age
            }

            # Publicar a mensagem no tópico MQTT
            client.publish(MQTT_TOPIC, str(message))
            print(f"Mensagem enviada")

            # Aguardar 1 segundo antes de enviar a próxima linha
            time.sleep(0.1)

    # Desconectar do broker
    client.disconnect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publicar dados de sensores com parâmetros adicionais via MQTT.")
    parser.add_argument("--weight", type=float, required=True, help="Peso do usuário (kg)")
    parser.add_argument("--height", type=float, required=True, help="Altura do usuário (m)")
    parser.add_argument("--gender", type=str, required=True, choices=["Male", "Female"], help="Gênero do usuário")
    parser.add_argument("--age", type=int, required=True, help="Idade do usuário (anos)")

    # Parse dos argumentos
    args = parser.parse_args()

    # Chamar a função com os argumentos passados
    publish_data(weight=args.weight, height=args.height, gender=args.gender, age=args.age)