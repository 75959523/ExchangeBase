from database.db_pool import get_connection, release_connection


def fetch_symbol_names(table_name):
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute(f"SELECT symbol_name FROM {table_name} order by symbol_id;")
    coins = [row[0] for row in cursor.fetchall()]
    cursor.close()
    release_connection(connection)
    return coins


def get_symbols():
    symbols = fetch_symbol_names("symbols")
    reference = fetch_symbol_names("reference")
    return symbols, reference
