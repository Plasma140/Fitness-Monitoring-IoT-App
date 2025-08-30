from pymongo import MongoClient
from flask import Flask, jsonify, request

app = Flask(__name__)

# Conexão com MongoDB
client = MongoClient("mongodb://mongodb:27017")
db = client.iot_data

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

# Endpoint HTTP para obter as calorias
@app.route('/get_daily_calories', methods=['GET'])
def get_daily_calories():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                "total_calories": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(calories_DB.aggregate(pipeline))
    
    # Formata os resultados
    calorias_diarias = [
        {
            "date": resultado["_id"],  # A data agregada
            "total_calories": resultado["total_calories"]
        }
        for resultado in resultados
    ]

    return jsonify(calorias_diarias)  # Retorna os dados em JSON

@app.route('/get_weekly_calories', methods=['GET'])
def get_weekly_calories():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%U", "date": "$timestamp"}},
                "total_calories": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]
    
    # Executa a agregação
    resultados = list(calories_DB.aggregate(pipeline))
    
    # Formata os resultados
    calorias_semanais = [
        {
            "date": resultado["_id"],  # A data agregada
            "total_calories": resultado["total_calories"]
        }
        for resultado in resultados
    ]

    return jsonify(calorias_semanais)  # Retorna os dados em JSON

@app.route('/get_monthly_calories', methods=['GET'])
def get_monthly_calories():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$timestamp"},
                    "month": {"$month": "$timestamp"}
                },
                "total_calories": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id.year": 1, "_id.month": 1}
        }
    ]
    
    # Executa a agregação
    resultados = list(calories_DB.aggregate(pipeline))
    
    # Formata os resultados
    calorias_mensais = [
        {
            "date": resultado["_id"],  # A data agregada
            "total_calories": resultado["total_calories"]
        }
        for resultado in resultados
    ]

    return jsonify(calorias_mensais)  # Retorna os dados em JSON

@app.route('/get_anually_calories', methods=['GET'])
def get_anually_calories():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$timestamp"}
                },
                "total_calories": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id.year": 1}
        }
    ]
    
    # Executa a agregação
    resultados = list(calories_DB.aggregate(pipeline))
    
    # Formata os resultados
    calorias_anuais = [
        {
            "date": resultado["_id"],  # A data agregada
            "total_calories": resultado["total_calories"]
        }
        for resultado in resultados
    ]

    return jsonify(calorias_anuais)  # Retorna os dados em JSON

####----------------------------------------------------------------------####
####----------------------------------------------------------------------####
####----------------------------------------------------------------------####

# Endpoint HTTP para obter as passos
@app.route('/get_daily_steps', methods=['GET'])
def get_daily_steps():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                "value": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(steps_DB.aggregate(pipeline))
    
    # Formata os resultados
    passos_diarias = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"]
        }
        for resultado in resultados
    ]

    return jsonify(passos_diarias)  # Retorna os dados em JSON

@app.route('/get_weekly_steps', methods=['GET'])
def get_weekly_steps():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%U", "date": "$timestamp"}},
                "value": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(steps_DB.aggregate(pipeline))
    
    # Formata os resultados
    passos_semanais = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"]
        }
        for resultado in resultados
    ]

    return jsonify(passos_semanais)  # Retorna os dados em JSON

@app.route('/get_monthly_steps', methods=['GET'])
def get_monthly_steps():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
                "value": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(steps_DB.aggregate(pipeline))
    
    # Formata os resultados
    passos_mensais = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"]
        }
        for resultado in resultados
    ]

    return jsonify(passos_mensais)  # Retorna os dados em JSON

@app.route('/get_anually_steps', methods=['GET'])
def get_anually_steps():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y", "date": "$timestamp"}},
                "value": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(steps_DB.aggregate(pipeline))
    
    # Formata os resultados
    passos_anuais = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"]
        }
        for resultado in resultados
    ]

    return jsonify(passos_anuais)  # Retorna os dados em JSON

####----------------------------------------------------------------------####
####----------------------------------------------------------------------####
####----------------------------------------------------------------------####

# Endpoint HTTP para obter a distancia andada
@app.route('/get_daily_dis_walked', methods=['GET'])
def get_daily_dis_walked():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                "value": {"$sum": "$value"},
                "data_count": {"$count": {}}  # Conta o número de documentos
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(dis_walked_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_andada_diaria = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados
    ]

    return jsonify(dis_andada_diaria)  # Retorna os dados em JSON

@app.route('/get_weekly_dis_walked', methods=['GET'])
def get_weekly_dis_walked():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%U", "date": "$timestamp"}},
                "value": {"$sum": "$value"},
                "data_count": {"$count": {}}  # Conta o número de documentos
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(dis_walked_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_andada_semanal = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados
    ]

    return jsonify(dis_andada_semanal)  # Retorna os dados em JSON

@app.route('/get_monthly_dis_walked', methods=['GET'])
def get_monthly_dis_walked():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
                "value": {"$sum": "$value"},
                "data_count": {"$count": {}}  # Conta o número de documentos
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(dis_walked_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_andada_mensal = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados
    ]

    return jsonify(dis_andada_mensal)  # Retorna os dados em JSON

@app.route('/get_anual_dis_walked', methods=['GET'])
def get_anual_dis_walked():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y", "date": "$timestamp"}},
                "value": {"$sum": "$value"},
                "data_count": {"$count": {}}  # Conta o número de documentos
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(dis_walked_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_andada_anual = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados
    ]

    return jsonify(dis_andada_anual)  # Retorna os dados em JSON

####----------------------------------------------------------------------####
####----------------------------------------------------------------------####
####----------------------------------------------------------------------####

# Endpoint HTTP para obter a distancia corrida
@app.route('/get_daily_dis_ran', methods=['GET'])
def get_daily_dis_ran():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                "value": {"$sum": "$value"},
                "data_count": {"$count": {}}  # Conta o número de documentos
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(dis_ran_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_corrida_diaria = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados
    ]

    return jsonify(dis_corrida_diaria)  # Retorna os dados em JSON

@app.route('/get_weekly_dis_ran', methods=['GET'])
def get_weekly_dis_ran():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%U", "date": "$timestamp"}},
                "value": {"$sum": "$value"},
                "data_count": {"$count": {}}  # Conta o número de documentos
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(dis_ran_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_corrida_semanal = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados
    ]

    return jsonify(dis_corrida_semanal)  # Retorna os dados em JSON

@app.route('/get_monthly_dis_ran', methods=['GET'])
def get_monthly_dis_ran():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
                "value": {"$sum": "$value"},
                "data_count": {"$count": {}}  # Conta o número de documentos
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(dis_ran_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_corrida_mensal = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados
    ]

    return jsonify(dis_corrida_mensal)  # Retorna os dados em JSON

@app.route('/get_anual_dis_ran', methods=['GET'])
def get_anual_dis_ran():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y", "date": "$timestamp"}},
                "value": {"$sum": "$value"},
                "data_count": {"$count": {}}  # Conta o número de documentos
            }
        },
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados = list(dis_ran_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_corrida_anual = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados
    ]

    return jsonify(dis_corrida_anual)  # Retorna os dados em JSON

####----------------------------------------------------------------------####
####----------------------------------------------------------------------####
####----------------------------------------------------------------------####

# Endpoint HTTP para obter a distancia total
@app.route('/get_daily_dis', methods=['GET'])
def get_daily_dis():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        # Primeira coleção: distance_ran
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                "total_value": {"$sum": "$value"},
                "data_count": {"$count": {}}
            }
        },
        # Adiciona dados da segunda coleção: distance_walked
        {
            "$unionWith": {
                "coll": "distance_walked",  # Nome da segunda coleção
                "pipeline": [
                    {
                        "$group": {
                            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                            "total_value": {"$sum": "$value"},
                            "data_count": {"$count": {}}
                        }
                    }
                ]
            }
        },
        # Agrupamento final para combinar ambas as coleções
        {
            "$group": {
                "_id": "$_id",
                "value": {"$sum": "$total_value"},  # Soma os valores de ambas as coleções
                "data_count": {"$sum": "$data_count"}  # Soma as contagens de ambas as coleções
            }
        },
        # Ordenação final por data
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados_ran = list(dis_ran_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_corrida_diaria = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados_ran
    ]

    return jsonify(dis_corrida_diaria)  # Retorna os dados em JSON

@app.route('/get_weekly_dis', methods=['GET'])
def get_weekly_dis():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        # Primeira coleção: distance_ran
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%U", "date": "$timestamp"}},
                "total_value": {"$sum": "$value"},
                "data_count": {"$count": {}}
            }
        },
        # Adiciona dados da segunda coleção: distance_walked
        {
            "$unionWith": {
                "coll": "distance_walked",  # Nome da segunda coleção
                "pipeline": [
                    {
                        "$group": {
                            "_id": {"$dateToString": {"format": "%Y-%U", "date": "$timestamp"}},
                            "total_value": {"$sum": "$value"},
                            "data_count": {"$count": {}}
                        }
                    }
                ]
            }
        },
        # Agrupamento final para combinar ambas as coleções
        {
            "$group": {
                "_id": "$_id",
                "value": {"$sum": "$total_value"},  # Soma os valores de ambas as coleções
                "data_count": {"$sum": "$data_count"}  # Soma as contagens de ambas as coleções
            }
        },
        # Ordenação final por data
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados_ran = list(dis_ran_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_corrida_diaria = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados_ran
    ]

    return jsonify(dis_corrida_diaria)  # Retorna os dados em JSON

@app.route('/get_monthly_dis', methods=['GET'])
def get_monthly_dis():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        # Primeira coleção: distance_ran
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
                "total_value": {"$sum": "$value"},
                "data_count": {"$count": {}}
            }
        },
        # Adiciona dados da segunda coleção: distance_walked
        {
            "$unionWith": {
                "coll": "distance_walked",  # Nome da segunda coleção
                "pipeline": [
                    {
                        "$group": {
                            "_id": {"$dateToString": {"format": "%Y-%m", "date": "$timestamp"}},
                            "total_value": {"$sum": "$value"},
                            "data_count": {"$count": {}}
                        }
                    }
                ]
            }
        },
        # Agrupamento final para combinar ambas as coleções
        {
            "$group": {
                "_id": "$_id",
                "value": {"$sum": "$total_value"},  # Soma os valores de ambas as coleções
                "data_count": {"$sum": "$data_count"}  # Soma as contagens de ambas as coleções
            }
        },
        # Ordenação final por data
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados_ran = list(dis_ran_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_corrida_diaria = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados_ran
    ]

    return jsonify(dis_corrida_diaria)  # Retorna os dados em JSON

@app.route('/get_anually_dis', methods=['GET'])
def get_anually_dis():
    
    # Pipeline de agregação MongoDB
    pipeline = [
        # Primeira coleção: distance_ran
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y", "date": "$timestamp"}},
                "total_value": {"$sum": "$value"},
                "data_count": {"$count": {}}
            }
        },
        # Adiciona dados da segunda coleção: distance_walked
        {
            "$unionWith": {
                "coll": "distance_walked",  # Nome da segunda coleção
                "pipeline": [
                    {
                        "$group": {
                            "_id": {"$dateToString": {"format": "%Y", "date": "$timestamp"}},
                            "total_value": {"$sum": "$value"},
                            "data_count": {"$count": {}}
                        }
                    }
                ]
            }
        },
        # Agrupamento final para combinar ambas as coleções
        {
            "$group": {
                "_id": "$_id",
                "value": {"$sum": "$total_value"},  # Soma os valores de ambas as coleções
                "data_count": {"$sum": "$data_count"}  # Soma as contagens de ambas as coleções
            }
        },
        # Ordenação final por data
        {
            "$sort": {"_id": 1}  # Ordena por data
        }
    ]
    
    # Executa a agregação
    resultados_ran = list(dis_ran_DB.aggregate(pipeline))
    
    # Formata os resultados
    dis_corrida_diaria = [
        {
            "date": resultado["_id"],  # A data agregada
            "value": resultado["value"],
            "data_count": resultado["data_count"]
        }
        for resultado in resultados_ran
    ]

    return jsonify(dis_corrida_diaria)  # Retorna os dados em JSON

####----------------------------------------------------------------------####
####----------------------------------------------------------------------####
####----------------------------------------------------------------------####

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)