import csv
import datetime
import os
import time
import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np # linear algebra
import pymongo
from pymongo import MongoClient
import requests
import threading
import queue
import json
import ast
from flask import Flask, jsonify, request

app = Flask(__name__)

max_retries = 10  # Número máximo de tentativas
retry_interval = 5  # Intervalo entre as tentativas em segundos

time.sleep(5)

# Dados com intervalos de idade representados por limites numéricos
speed_data_with_ranges = [
    {"range": (20, 29), "Male": 1.36, "Female": 1.34},
    {"range": (30, 39), "Male": 1.43, "Female": 1.34},
    {"range": (40, 49), "Male": 1.43, "Female": 1.39},
    {"range": (50, 59), "Male": 1.43, "Female": 1.31},
    {"range": (60, 69), "Male": 1.34, "Female": 1.24},
    {"range": (70, 79), "Male": 1.26, "Female": 1.13},
    {"range": (80, 89), "Male": 0.97, "Female": 0.94},
]


# Conexão com MongoDB
client = MongoClient("mongodb://mongodb:27017")
db = client.iot_data
#collection = db.sensors_data

if "total_calories_burned" not in db.list_collection_names():
    db.create_collection(
        "total_calories_burned",
        validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["timestamp", "value"],
                "properties": {
                    "timestamp": {"bsonType": "date", "description": "Must be a valid ISO date"},
                    "value": {"bsonType": "double", "description": "Must be a double"}
                }
            }
        }
    )

if "distance_walked" not in db.list_collection_names():
    db.create_collection(
        "distance_walked",
        validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["timestamp", "value", "calories_burned"],
                "properties": {
                    "timestamp": {"bsonType": "date", "description": "Must be a valid ISO date"},
                    "value": {"bsonType": "double", "description": "Must be a double"}
                }
            }
        }
    )
    
if "distance_ran" not in db.list_collection_names():
    db.create_collection(
        "distance_ran",
        validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["timestamp", "value", "calories_burned"],
                "properties": {
                    "timestamp": {"bsonType": "date", "description": "Must be a valid ISO date"},
                    "value": {"bsonType": "double", "description": "Must be a double"}
                }
            }
        }
    )
    
if "steps" not in db.list_collection_names():
    db.create_collection(
    "steps",
    validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["timestamp", "value"],
            "properties": {
                "timestamp": {"bsonType": "date", "description": "Must be a valid ISO date"},
                "value": {"bsonType": "int", "description": "Must be a double"},
            }
        }
    }
)

calories_DB = db.total_calories_burned
dis_walked_DB = db.distance_walked
dis_ran_DB = db.distance_ran
steps_DB = db.steps

# Função para conectar e subscrever ao tópico
def setup_mqtt():
    client = mqtt.Client()
    
    client.tls_set(ca_certs="/app/caMQTT.crt",
    certfile="/app/processor.crt",
    keyfile="/app/processor.key")
    client.tls_insecure_set(True)
    
    client.on_message = send_file
    client.connect("172.100.10.10", 8883, 60)
    client.subscribe("idc/fc04")
    
    client.loop_forever()
            
def send_file(client, userdata, message):
    url = "http://ml_processor:5000/processar_dados/"
    
    message = message.payload.decode()
    message = message.replace("'", '"')
    data_json = json.loads(message)
    
    print("A ENVIAR DADOS")
    
    print(f"Processando mensagem: {data_json}")
    weight = data_json.pop("weight", None)
    height = data_json.pop("height", None)
    gender = data_json.pop("gender", None)
    age = data_json.pop("age", None)
    
    response = requests.post(url, json=data_json)
    print("Dados enviados")
    activity = response.json() # Dados sobre se está andar ou a correr
    print("Dados recebidos")
    print(activity[1])
    
    CurrentTime = datetime.datetime.now()

    velocity = get_velocity(gender, age)
    if activity[1] == "1":
        velocity = velocity*2
    distance = velocity

    insert_calories(CurrentTime, calculate_calories(weight, height, velocity))

    if activity[1] == "0":
        insert_dis_walked(CurrentTime, velocity, calculate_calories(weight, height, distance))
    else:
        insert_dis_ran(CurrentTime, velocity, calculate_calories(weight, height, distance))

    insert_steps(CurrentTime, calculate_steps(distance, height))

    time.sleep(1)

        
def send_training_data():
    url = "http://ml_processor:5000/training_data/"
    
    # Lista para armazenar todas as linhas do CSV
    dados_completos = []
    
    with open("/app/training_data.csv", "r") as file:
        
        reader = csv.DictReader(file, delimiter=";")
        
        for row in reader:
            dados_completos.append(row)  # Adiciona cada linha à lista
        
        for attempt in range(max_retries):
            try:
                # Tenta enviar os dados para o ml_processor
                response = requests.post(url, json={"data": dados_completos})
                
                # Verifica o status da resposta
                if response.status_code == 200:
                    print("Dados enviados com sucesso!")
                    break  # Sai do loop se a solicitação foi bem-sucedida
                else:
                    print(f"Erro inesperado: {response.status_code} - {response.text}")
        
            except requests.exceptions.ConnectionError:
                print(f"Tentativa {attempt + 1}/{max_retries}: ml_processor não está acessível. Retentando em {retry_interval} segundos...")
                time.sleep(retry_interval)  # Aguarda antes de tentar novamente
        
        else:
            # Se todas as tentativas falharem
            print("Falha ao conectar ao ml_processor após várias tentativas.")
            
        print(response.json())
        print("Dados recebidos")

# Calcula o número de passos dados com base na distância percorrida.
def calculate_steps(distance, height):

    # Calcula o comprimento médio do passo com base na altura
    step_length = height * 0.43  # Aproximação de 43% da altura

    # Calcula o número de passos
    steps = distance / step_length
    return round(steps)  # Arredonda para o número inteiro mais próximo

def get_velocity(gender, age):
    for entry in speed_data_with_ranges:
        low, high = entry["range"]
        if low <= age <= high:  # Verifica se a idade está dentro do intervalo
            return entry.get(gender, "Invalid gender")  # Retorna a velocidade para o gênero
    return "Age not in range"  # Retorna uma mensagem caso a idade não corresponda a nenhum intervalo
        
# Função para calcular calorias
def calculate_calories(weight, height, velocity):
    return 0.035 * weight + ((velocity ** 2) / height) * 0.029 * weight

# Insere calorias na base de dados
def insert_calories(current_time, calories_per_second):
    calories_DB.insert_one({
            "timestamp": current_time,
            "value": calories_per_second
        })
# Insere distancia andada
def insert_dis_walked(current_time, value, calories_burned):
    dis_walked_DB.insert_one({
            "timestamp": current_time,
            "value": value,
            "calories_burned": calories_burned
        })
    
# Insere distancia corrida
def insert_dis_ran(current_time, value, calories_burned):
    dis_ran_DB.insert_one({
            "timestamp": current_time,
            "value": value,
            "calories_burned": calories_burned
        })
    
# Insere passos
def insert_steps(current_time, value):
    steps_DB.insert_one({
            "timestamp": current_time,
            "value": value
        })
    
####----------------------------------------------------------------------####
####----------------------------------------------------------------------####
####----------------------------------------------------------------------####

if __name__ == "__main__":
    send_training_data()
    setup_mqtt()
    