import json
import requests
from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt

# Conexión a Elasticsearch 
es = Elasticsearch(
    hosts=["https://localhost:9200"],
    basic_auth=("edu", "123456"),
    verify_certs=False,
    ssl_show_warn=False
)

if es.ping():
    print("Conectado a Elasticsearch")
else:
    raise Exception("Error de conexión con Elasticsearch")

# Crear el Índice en Elasticsearch
indice = "wikipedia_edits"
indice_config = {
    "mappings": {
        "properties": {
            "titulo": {"type": "text"},
            "usuario": {"type": "keyword"},
            "resumen": {"type": "text"},
            "timestamp": {"type": "date"}
        }
    }
}

if not es.indices.exists(index=indice):
    es.indices.create(index=indice, body=indice_config)
    print(f"Índice '{indice}' creado.")
else:
    print(f"Índice '{indice}' ya existe.")

# Consultas
def verificar_datos(es, indice):
    query = {"size": 5, "query": {"match_all": {}}}
    resultados = es.search(index=indice, body=query)
    for i, hit in enumerate(resultados['hits']['hits'], 1):
        print(f"\nRegistro {i}:")
        print(json.dumps(hit['_source'], indent=4))

def consulta_avanzada(es, indice):
    query = {
        "size": 5,
        "query": {
            "bool": {
                "must": [
                    {"match": {"resumen": "science"}},
                    #sin filtro de fecha
                ]
            }
        }
    }

    resultados = es.search(index=indice, body=query)
    print("\nResultados de la consulta avanzada:")
    if resultados['hits']['hits']:
        for i, hit in enumerate(resultados['hits']['hits'], 1):
            print(f"\nRegistro {i}:")
            print(json.dumps(hit['_source'], indent=4))
    else:
        print("No se encontraron resultados.")




# ======= Ejecución Principal =======
if __name__ == "__main__":
    # Verificar los datos disponibles en el índice
    verificar_datos(es, indice)


    # Ejecutar la consulta avanzada
    print("\nEjecutando consulta avanzada...")
    consulta_avanzada(es, indice)


