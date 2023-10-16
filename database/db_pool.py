import os
import psycopg2
import psycopg2.pool
import yaml

dir_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(dir_path, 'config', 'config.yml')


def create_connection_pool(min_conn=8, max_conn=100):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)['database']

    connection = psycopg2.pool.SimpleConnectionPool(
        minconn=min_conn,
        maxconn=max_conn,
        user=config['username'],
        password=config['password'],
        host=config['host'],
        port=config['port'],
        database=config['db_name']
    )

    return connection


def set_search_path(connection):
    cursor = connection.cursor()
    cursor.execute('SET search_path TO exchange')
    connection.commit()


connection_pool = create_connection_pool()


def get_connection():
    connection = connection_pool.getconn()

    set_search_path(connection)

    return connection


def release_connection(connection):
    connection_pool.putconn(connection)


