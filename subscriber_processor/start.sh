#!/bin/bash

# Iniciar a primeira aplicação
python /app/subscriber_processor.py &

# Iniciar a segunda aplicação
python /app/app.py &

# Esperar para manter o contêiner em execução
wait
