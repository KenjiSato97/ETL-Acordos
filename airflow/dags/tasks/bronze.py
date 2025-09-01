import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import boto3
import io
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def google_sheet_to_minio_etl(sheet_id, sheet_name, bucket_name, endpoint_url, access_key, secret_key):
    # Configuração do cliente MinIO
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    def get_google_sheet_data(sheet_id, sheet_name):
        try:
            scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            creds = Credentials.from_service_account_file('/opt/airflow/config_airflow/credentials.json', scopes=scope)
            client = gspread.authorize(creds)
            sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
            try:
                data = sheet.get_all_records()
            except gspread.exceptions.GSpreadException as e:
                if 'A linha de cabeçalho na planilha não é única.' in str(e):
                    logging.warning(f"Erro ao usar get_all_records() (cabeçalhos duplicados): {e}")
                    expected_headers = {
                        'Geral': ['Data de Celebração', 'Parceiro', 'Tipo de Parceiro', 'Continente', 'Região', 'Local de Assinatura', 'Tipo de Acordo', 'Título', 'Objetivo', 'Recursos', 'Tipo de Documento', 'Vigência', 'Link']
                    }.get(sheet_name, None)
                    if expected_headers:
                        data = sheet.get_all_records(expected_headers=expected_headers)
                    else:
                        raise
            if not data:
                raise ValueError(f"Nenhum dado foi retornado para a planilha {sheet_name}")

            df = pd.DataFrame(data)
            return df
        except Exception as e:
            logging.error(f"Erro ao obter dados da planilha do Google: {e}")
            raise

    def normalize_column_names(df):
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        return df
    
    def transform_columns(df):
        
        if 'data_de_celebração' in df.columns:
            df['data_de_celebração'] = pd.to_datetime(df['data_de_celebração'], errors='coerce', format='%d/%m/%Y')
        if 'vigência' in df.columns:
            df['vigência'] = pd.to_datetime(df['vigência'], errors='coerce', format='%d/%m/%Y')
        if 'titulo' in df.columns:
            df['titulo'] = df['titulo'].astype(str)  
            df['titulo'] = df['titulo'].str.strip()
            df['titulo'] = df['titulo'].str[:255]  
            
        return df
    
    def handle_missing_values(df):
        df = df.fillna(value=pd.NA)  
        return df

    try:
        df = get_google_sheet_data(sheet_id, sheet_name)
        df = normalize_column_names(df)
        logging.info(f"Dados da planilha {sheet_name} normalizados com sucesso")
        df = transform_columns(df)
        logging.info(f"Dados da planilha {sheet_name} transformados com sucesso")
        df = handle_missing_values(df)
        logging.info(f"Dados nulos da planilha {sheet_name} tratados com sucesso")

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        minio_client.put_object(Bucket=bucket_name, Key=f"{sheet_name}/brz_{sheet_name}.parquet", Body=parquet_buffer.getvalue())
    except Exception as e:
        logging.error(f"Erro ao processar a planilha {sheet_name}: {e}")
        raise

    # Conectar ao MariaDB e escrever os dados
    mysql_hook = MySqlHook(mysql_conn_id='local_mariadb')
    connection = mysql_hook.get_conn()

    try:
        with connection.cursor() as cursor:
            # Criar a tabela se não existir
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS brz_{sheet_name} (
                {', '.join([f'{col} TEXT' if col.lower() in ['título', 'objetivo'] else (f'{col} DATE' if col.lower() == 'data_de_celebração' else f'{col} VARCHAR(255)') for col in df.columns])}
            )
            """
            cursor.execute(create_table_sql)
            logging.info(f"Tabela brz_{sheet_name} verificada/criada no MariaDB")

            # Inserir ou atualizar dados na tabela
            for index, row in df.iterrows():
                # Atualizar dados se já existir
                update_sql = f"""
                UPDATE brz_{sheet_name}
                SET {', '.join([f'{col} = %s' for col in df.columns])}
                WHERE {df.columns[0]} = %s
                """
                cursor.execute(update_sql, tuple(row.tolist()) + (row[df.columns[0]],))

                # Inserir novos dados se não existir
                insert_sql = f"""
                INSERT INTO brz_{sheet_name} ({', '.join(df.columns)})
                VALUES ({', '.join(['%s'] * len(df.columns))})
                ON DUPLICATE KEY UPDATE {', '.join([f'{col} = VALUES({col})' for col in df.columns])}
                """
                cursor.execute(insert_sql, tuple(row))

            connection.commit()
            logging.info(f"Dados inseridos/atualizados na tabela brz_{sheet_name} no MariaDB")

    except Exception as e:
        logging.error(f"Erro ao conectar ao MariaDB ou inserir dados: {e}")

    finally:
        if connection:
            connection.close()
    logging.info("Processo ETL concluído com sucesso.")