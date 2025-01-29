import pandas as pd
import logging

# Log configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def clean_data(df):
    try:
        if 'DATA' in df.columns:
            df['DATA'] = pd.to_datetime(df['DATA'], format='%d/%m/%Y', errors='coerce')

        df = df.dropna()

        logger.info(f"Data processed. {len(df)} rows ajusted.")
        return df

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return pd.DataFrame()