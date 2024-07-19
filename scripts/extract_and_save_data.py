import requests
from pymongo.mongo_client import MongoClient


def connect_mongo(uri):
    client = MongoClient(uri)

    try:
        client.admin.command("ping")
        print("Conexão com o MongoDB realizada com sucesso!")

    except Exception as e:
        print(e)

    return client


def create_db(client, db_name):
    db = client[db_name]

    return db


def create_collection(db, col_name):

    return db[col_name]


def extract_api_data(url):

    try:
        response = requests.get(url)
        print('Extração realizada com sucesso!')

    except Exception as e:
        print(e)

    return response.json()


def insert_data(data, collection):

    docs = collection.insert_many(data)
    lines_sum = len(docs.inserted_ids)

    print(f"Total de itens inseridos: {lines_sum}")

if __name__ == "__main__":
    
    client = connect_mongo('mongodb://localhost:27017')
    db = create_db(client, 'db_products')
    collection = create_collection(db, 'products')
    
    data = extract_api_data('https://labdados.com/produtos')
    insert_data(data, collection)