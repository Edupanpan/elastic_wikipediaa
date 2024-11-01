import json
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
def ver_datos(es, indice):
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
                    # {"range": {"timestamp": {"gte": "2021-01-01"}}}
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


def consulta_ponderada(es, indice, termino):
    query = {
        "size": 5,
        "query": {
            "bool": {
                "should": [
                    {"match": {"titulo": {"query": termino, "boost": 3}}},
                    {"match": {"resumen": {"query": termino, "boost": 1}}}
                ]
            }
        }
    }

    resultados = es.search(index=indice, body=query)
    print("\nResultados de la búsqueda ponderada:")
    if resultados['hits']['hits']:
        for i, hit in enumerate(resultados['hits']['hits'], 1):
            print(f"\nRegistro {i}:")
            print(json.dumps(hit['_source'], indent=4))
    else:
        print("No se encontraron resultados.")
def grafico(es, indice):
    query = {
        "size": 1000,
        "query": {
            "match_all": {}
        }
    }

    resultados = es.search(index=indice, body=query)
    categorias = {}

    for hit in resultados['hits']['hits']:
        categoria = hit['_source'].get('resumen', 'Desconocido')
        if categoria in categorias:
            categorias[categoria] += 1
        else:
            categorias[categoria] = 1

    categorias_ordenadas = dict(sorted(categorias.items(), key=lambda item: item[1], reverse=True))

    plt.figure(figsize=(10, 6))
    plt.bar(categorias_ordenadas.keys(), categorias_ordenadas.values(), color='skyblue')
    plt.xlabel('Categoría de Artículo')
    plt.ylabel('Número de Ediciones')
    plt.title('Número de Ediciones por Categoría de Artículo')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.show()

        
# Ejecución Principal 
if __name__ == "__main__":

    ver_datos(es, indice)
    consulta_avanzada(es, indice)
    consulta_ponderada(es, indice, "science")
    grafico(es, indice)

