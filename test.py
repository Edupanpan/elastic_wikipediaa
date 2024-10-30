import wikipedia
import requests
from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt
from collections import Counter
from datetime import datetime

# Conexión a Elasticsearch
client = Elasticsearch(
    hosts=["https://localhost:9200"],
    basic_auth=("edu", "123456"),
    verify_certs=False,
    ssl_show_warn=False
)

# Verificación del estado del clúster
if client.ping():
    print("Conexión exitosa a Elasticsearch")
else:
    print("Error al conectar a Elasticsearch")

# Nombre del índice
indexName = "wikipedia_edits"

# Definir el mapeo del índice
editsMapping = {
    "mappings": {
        "properties": {
            'titulo': {'type': 'text'},
            'resumen': {'type': 'text'},
            'nombreEditor': {'type': 'keyword'},
            'fechaEdicion': {'type': 'date'}
        }
    }
}

# Crear el índice si no existe
if not client.indices.exists(index=indexName):
    client.indices.create(index=indexName, body=editsMapping)
    print(f"Índice '{indexName}' creado exitosamente.")
else:
    print(f"Índice '{indexName}' ya existe.")

def cargar_historial_ediciones(titulo_pagina, limite=7):
    url = f"https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "titles": titulo_pagina,
        "prop": "revisions",
        "rvlimit": limite,
        "rvprop": "ids|timestamp|user|comment",
        "format": "json"
    }
    response = requests.get(url, params=params)
    data = response.json()
    page_id = next(iter(data['query']['pages']))
    revisions = data['query']['pages'][page_id]['revisions']
    cont = 1
    for revision in revisions:
        doc = {
            'titulo': titulo_pagina,
            'resumen': revision.get('comment', ''),
            'nombreEditor': revision['user'],
            'fechaEdicion': revision['timestamp']
        }
        client.index(index=indexName, document=doc)
        print(f"{cont}. {doc}\n")
        cont += 1

# Llamar a la función para cargar el historial de ediciones
cargar_historial_ediciones("Python (programming language)", limite=7)