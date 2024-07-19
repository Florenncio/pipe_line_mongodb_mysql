import os

import pandas as pd
from extract_and_save_data import connect_mongo, create_collection, create_db


def view_collection(col):
    for doc in col.find():
        print(doc)


def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {f"{col_name}": f"{new_name}"}})


def select_category(col, category_name):
    query = {"Categoria do Produto": f"{category_name}"}
    list_products = []

    for doc in col.find(query):
        list_products.append(doc)

    return list_products


def make_regex(col, field, regex):
    query = {f"{field}": {"$regex": f"{regex}"}}
    list_regex = []

    for doc in col.find(query):
        list_regex.append(doc)

    return list_regex


def create_dataframe(list_products):
    df = pd.DataFrame(list_products)

    return df


def format_data(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")


def save_csv(df, file_name):
    directory = "./data"

    if not os.path.exists(directory):
        os.makedirs(directory)

    file_path = os.path.join(directory, f"{file_name}.csv")
    df.to_csv(file_path, index=False)

    print("Arquivo salvo!")


if __name__ == "__main__":

    client = connect_mongo("mongodb://localhost:27017")
    db = create_db(client, "db_products")
    col = create_collection(db, "products")

    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")

    list_livros = select_category(col, "livros")
    df_livros = create_dataframe(list_livros)
    format_data(df_livros)
    save_csv(df_livros, "vendas_livros")

    list_products = make_regex(col, "Data da Compra", "202[1-9]")
    df_products = create_dataframe(list_livros)
    format_data(df_products)
    save_csv(df_products, "vendas_2021_em_diante")
