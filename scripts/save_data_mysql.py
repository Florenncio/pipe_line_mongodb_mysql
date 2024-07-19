import os

import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from mysql.connector import Error


def connect_mysql(host_name, user_name, pw):
    try:
        cnx = mysql.connector.connect(host=host_name, user=user_name, password=pw)

        if cnx.is_connected():
            print("Conexão ao MySQL realizada com sucesso!")
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")

    return cnx


def create_cursor(cnx):
    cursor = cnx.cursor()

    return cursor


def create_database(cursor, db_name):
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        print(f"Banco de dados {db_name} criado com sucesso!")
    except Error as e:
        print(f"Erro ao criar banco de dados: {e}")


def show_databases(cursor):

    try:
        cursor.execute("SHOW DATABASES;")

        for db in cursor:
            print(db)

    except Error as e:
        print(f"Erro ao tentar visualizar o banco de dados: {e}")


def create_products_table(cursor, db_name, tb_name):

    try:
        cursor.execute(
            f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{tb_name}(
            id VARCHAR(100),
            produto VARCHAR(100),
            categoria_produto VARCHAR(100),
            preco FLOAT(10,2),
            frete FLOAT(10,2),
            data_compra DATE,
            vendedor VARCHAR(100),
            local_compra VARCHAR(100),
            avaliacao_compra INT,
            tipo_pagamento VARCHAR(100),
            qntd_parcelas INT,
            latitude FLOAT(10,2),
            longitude FLOAT(10,2),
            
            PRIMARY KEY (id)
        );            
        """
        )
        print("Tabela de produtos criada com sucesso!")
    except Error as e:
        print(f"Erro ao criar tabela de produtos: {e}")


def show_tables(cursor, db_name):

    try:
        cursor.execute(f"USE {db_name}")
        cursor.execute("SHOW TABLES;")

        for tb in cursor:
            print(tb)

    except Error as e:
        print(f"Erro ao tentar visualizar as tables: {e}")


def read_csv(file_name):
    directory = "./data"

    file_path = os.path.join(directory, f"{file_name}.csv")
    df = pd.read_csv(file_path)

    return df


def add_product_data(cnx, cursor, df, db_name, tb_name):

    list_data = [tuple(row) for index, row in df.iterrows()]

    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

    try:
        cursor.executemany(sql, list_data)

    except Error as e:
        print("Erro ao inserir os dados: {e}")

    finally:
        cnx.commit()


if __name__ == "__main__":

    # Carregando variaveis de ambiente
    load_dotenv()
    host = "localhost"
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")

    # Nome das entidades do banco de dados
    db = "db_products"
    table = "tb_products"

    # Configurando acesso
    cnx = connect_mysql(host, user, password)
    cursor = create_cursor(cnx)

    # Criando banco de dados
    create_database(cursor, db)
    show_databases(cursor)

    # Criando tabela
    create_products_table(cursor, db, table)
    show_tables(cursor, db)

    # Adicionando dados
    df = read_csv("vendas_livros")
    add_product_data(cnx, cursor, df, db, table)
