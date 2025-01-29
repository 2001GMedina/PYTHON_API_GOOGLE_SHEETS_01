from dotenv import load_dotenv
import pandas as pd
import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Carregar variáveis de ambiente
load_dotenv()

# Variável global para controlar inicialização do Oracle Client
oracle_client_initialized = False

def init_oracle_client():
    global oracle_client_initialized
    if not oracle_client_initialized:
        instantclient_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'instantclient_19_21')
        if os.path.exists(instantclient_path):
            try:
                import cx_Oracle  # Certifique-se de que cx_Oracle é importado no início
                cx_Oracle.init_oracle_client(lib_dir=instantclient_path)
                oracle_client_initialized = True
            except cx_Oracle.ProgrammingError:
                logger.error("Oracle Client already initialized.")
        else:
            logger.error(f"Oracle Instant Client not found on path: {instantclient_path}")
            raise FileNotFoundError(f"Oracle Instant Client not found on path: {instantclient_path}")

def create_oracle_connection():
    global oracle_client_initialized
    if not oracle_client_initialized:
        init_oracle_client()

    conn_string = os.getenv("ORACLE_CONN_STRING")
    
    if not conn_string:
        logger.error("ORACLE_CONN_STRING environment variable is not defined.")
        raise ValueError("ORACLE_CONN_STRING environment variable is not defined.")
    
    try:
        # Usando SQLAlchemy para criar uma engine de conexão
        engine = create_engine(f'oracle+cx_oracle://{conn_string}')
        return engine
    except Exception as e:
        logger.error(f"Error connecting to Oracle using SQLAlchemy: {e}")
        raise

def fetch_data(query):
    engine = create_oracle_connection()
    try:
        # Usando pandas para ler os dados diretamente da conexão SQLAlchemy
        df = pd.read_sql(query, con=engine)
        return df
    except Exception as e:
        logger.error(f"Error when executing query: {e}")
        raise

def insert_data(table_name, data, batch_size=500):
    engine = create_oracle_connection()
    try:
        # Converte o nome da tabela para minúsculas para garantir a correspondência com o banco de dados Oracle
        table_name = table_name.lower()

        if isinstance(data, pd.Series):
            data = data.to_frame().T  # Converte para DataFrame se necessário

        # Usando SQLAlchemy e pandas para inserção de dados
        with engine.connect() as connection:
            data.to_sql(table_name, con=connection, if_exists='append', index=False, chunksize=batch_size)
            logger.info(f"Inserted {len(data)} rows into {table_name}")

    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        raise

def clear_data(table_name):
    engine = create_oracle_connection()
    try:
        # Deletando todos os registros da tabela usando SQLAlchemy com `text()`
        with engine.connect() as connection:
            # Usando `text()` para criar uma consulta executável
            query = text(f"DELETE FROM {table_name}")
            connection.execute(query)
            connection.commit()  # Comitando a transação
            logger.info(f"Cleared data from {table_name}")
    
    except Exception as e:
        logger.error(f"Error cleaning table {table_name}: {e}")
        raise

# Testando conexão
if __name__ == '__main__':
    try:
        engine = create_oracle_connection()
        logger.info("Connection successful!")
    except Exception as e:
        logger.error(f"Failed to connect to Oracle: {e}")
