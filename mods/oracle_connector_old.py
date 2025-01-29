from dotenv import load_dotenv
import pandas as pd
import os
import cx_Oracle
import logging

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Load environment variables
load_dotenv()

# Variável global para controlar inicialização do Oracle Client
oracle_client_initialized = False

def init_oracle_client():
    global oracle_client_initialized
    if not oracle_client_initialized:
        instantclient_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'instantclient_19_21')
        if os.path.exists(instantclient_path):
            try:
                cx_Oracle.init_oracle_client(lib_dir=instantclient_path)
                oracle_client_initialized = True
            except cx_Oracle.ProgrammingError:
                logger.error("Oracle Client already initialized.")
        else:
            logger.error(f"Oracle Instant Client not found on path: {instantclient_path}")
            raise FileNotFoundError(f"Oracle Instant Client not found on path: {instantclient_path}")

def connect_to_oracle():
    global oracle_client_initialized
    if not oracle_client_initialized:
        init_oracle_client()

    conn_string = os.getenv("ORACLE_CONN_STRING")
    
    if not conn_string:
        logger.error("ORACLE_CONN_STRING environment variable is not defined.")
        raise ValueError("ORACLE_CONN_STRING environment variable is not defined.")
    
    try:
        connection = cx_Oracle.connect(conn_string)
        return connection
    except cx_Oracle.Error as e:
        logger.error(f"Error connecting to Oracle: {e}")
        raise

def fetch_data(query):
    connection = None
    try:
        connection = connect_to_oracle()
        df = pd.read_sql(query, con=connection)
        return df
    except cx_Oracle.Error as e:
        logger.error(f"Error when executing query: {e}")
        raise
    finally:
        if connection:
            connection.close()

def insert_data(table_name, data, batch_size=500):
    # Utilize o batch size para definir o tamanho do lote a ser inserido
    connection = connect_to_oracle()
    cursor = connection.cursor()
    try:
        if isinstance(data, pd.Series):
            data = data.to_frame().T  # Converte para DataFrame se necessário

        placeholders = ', '.join([':' + str(i+1) for i in range(len(data.columns))])
        query = f"INSERT INTO {table_name} VALUES ({placeholders})"

        rows = [tuple(row) for _, row in data.iterrows()]
        
        total_rows = len(rows)
        for i in range(0, total_rows, batch_size):
            batch = rows[i:i + batch_size]
            cursor.executemany(query, batch)  # Insere o lote atual
            connection.commit()

    except cx_Oracle.Error as e:
        logger.error(f"Error inserting data: {e}")
        raise
    finally:
        cursor.close()
        connection.close()

def clear_data(table_name):
    connection = connect_to_oracle()
    cursor = connection.cursor()
    try:
        delete = f"DELETE FROM {table_name}"
        cursor.execute(delete)
        connection.commit()
    
    except cx_Oracle.Error as e:
        logger.error(f"Error cleaning table {table_name}: {e}")
        raise
    
    finally:
        cursor.close()
        connection.close()

# Testando conexão
if __name__ == '__main__':
    try:
        connect_to_oracle()
    except Exception as e:
        logger.error(f"Failed to connect to Oracle: {e}")
