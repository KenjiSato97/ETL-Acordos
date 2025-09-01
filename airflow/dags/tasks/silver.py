import boto3
import pandas as pd
import io
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_and_load_silver(bucket_name, key, endpoint_url, access_key, secret_key, bucket2_name='silver'):
    """
    Função para tratamento de nulos, normalização de texto, criação de colunas derivadas e validação de regras de negócio, salvando a camada silver
    resultante no MinIO e MariaDB.
    """
    # Configurar cliente MinIO
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    try:
        # Listar objetos no bucket para verificar a chave correta
        objects = minio_client.list_objects_v2(Bucket=bucket_name)
        keys = [obj['Key'] for obj in objects.get('Contents', [])]
        if key not in keys:
            logging.error(f"Erro: A chave especificada '{key}' não existe no bucket '{bucket_name}'. Chaves disponíveis: {keys}")
            raise minio_client.exceptions.NoSuchKey(f"A chave especificada '{key}' não existe no bucket '{bucket_name}'.")

        # Ler dados do MinIO
        response = minio_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_parquet(io.BytesIO(response['Body'].read()))
        
        #Tratamento de valores nulos
        df['continente'].fillna('não informado')
        df['continente'] = df['continente'].replace('-', 'não informado')
        df['região'] = df['região'].fillna('não informado')
        df['região'] = df['região'].replace('-', 'não informado')
        df['local_de_assinatura'] = df['local_de_assinatura'].fillna('não informado')
        df['local_de_assinatura'] = df['local_de_assinatura'].replace('-', 'não informado')
        df['tipo_de_acordo'] = df['tipo_de_acordo'].fillna('não informado')
        df['tipo_de_acordo'] = df['tipo_de_acordo'].replace('-', 'não informado')
        df['título'] = df['título'].fillna('não informado')
        df['título'] = df['título'].replace('-', 'não informado')
        df['objetivo'] = df['objetivo'].fillna('não informado')
        df['objetivo'] = df['objetivo'].replace('-', 'não informado')
        df['recursos'] = df['recursos'].fillna('não informado')
        df['recursos'] = df['recursos'].replace('-', 'não informado')
        df['tipo_de_documento'] = df['tipo_de_documento'].fillna('não informado')
        df['tipo_de_documento'] = df['tipo_de_documento'].replace('-', 'não informado')


        #Normalização de todas as colunas textuais
        df['parceiro'] = df['parceiro'].str.strip().str.title()
        df['tipo_de_parceiro'] = df['tipo_de_parceiro'].str.strip().str.title()
        df['continente'] = df['continente'].str.strip().str.title()
        df['região'] = df['região'].str.strip().str.title()
        df['local_de_assinatura'] = df['local_de_assinatura'].str.strip().str.title()
        df['tipo_de_acordo'] = df['tipo_de_acordo'].str.strip().str.title()
        df['título'] = df['título'].str.strip().str.title()
        df['objetivo'] = df['objetivo'].str.strip().str.title()
        df['recursos'] = df['recursos'].str.strip().str.title()
        df['tipo_de_documento'] = df['tipo_de_documento'].str.strip().str.title()


        # Criação de novas colunas
        df['ano'] = df['data_de_celebração'].dt.year

        # Transformação da camada silver 
        slv_acordos_df = df[['parceiro', 'tipo_de_parceiro', 'continente', 'região', 'local_de_assinatura', 'tipo_de_acordo', 'título', 'objetivo', 'recursos', 'tipo_de_documento', 'ano']].drop_duplicates()

    
        
        # Salvar camada silver no MinIO
        for slv_df, slv_name in [
            (slv_acordos_df, 'slv_acordos'),
            
        ]:
            output_buffer = io.BytesIO()
            slv_df.to_parquet(output_buffer, index=False)
            output_buffer.seek(0)
            slv_key = f'silver/{slv_name}.parquet'
            minio_client.put_object(
                Bucket=bucket2_name,
                Key=slv_key,
                Body=output_buffer.getvalue()
            )
            logging.info(f"Camada '{slv_name}' salva no MinIO com a chave: {slv_key}")
            
            # Criar e popular tabelas no MariaDB
            mysql_hook = MySqlHook(mysql_conn_id='local_mariadb')
            connection = mysql_hook.get_conn()
            with connection.cursor() as cursor:
                # Criar tabela slv_clientes
                create_table_acordos_sql = """
                CREATE TABLE IF NOT EXISTS slv_acordos (
                    cod_acordo INT PRIMARY KEY AUTO_INCREMENT,
                    parceiro VARCHAR(255),
                    tipo_de_parceiro VARCHAR(255),
                    continente VARCHAR(255),
                    região VARCHAR(255),
                    local_de_assinatura VARCHAR(255),
                    tipo_de_acordo VARCHAR(255),
                    título TEXT,
                    objetivo TEXT,
                    recursos VARCHAR(255),
                    tipo_de_documento VARCHAR(255),
                    ano VARCHAR(255)
                )
                """
                cursor.execute(create_table_acordos_sql)
                logging.info("Tabela slv_acordos criada/verificada com sucesso.")
                
                
                # Inserir dados
                if slv_name == 'slv_acordos':
                    for _, row in slv_df.iterrows():
                        insert_sql = """
                        INSERT INTO slv_acordos
                        (parceiro, tipo_de_parceiro, continente, região, local_de_assinatura, tipo_de_acordo, título, objetivo, recursos, tipo_de_documento, ano)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        parceiro=VALUES(parceiro),
                        tipo_de_parceiro=VALUES(tipo_de_parceiro),
                        continente=VALUES(continente),
                        região=VALUES(região),
                        local_de_assinatura=VALUES(local_de_assinatura),
                        tipo_de_acordo=VALUES(tipo_de_acordo),
                        título=VALUES(título),
                        objetivo=VALUES(objetivo),
                        recursos=VALUES(recursos),
                        tipo_de_documento=VALUES(tipo_de_documento),
                        ano=VALUES(ano)
                        """ 
                        cursor.execute(insert_sql, tuple(row))
                    connection.commit()
                    logging.info("Dados inseridos/atualizados na tabela slv_acordos.")
                
                
    except Exception as e:
        logging.error(f"Erro ao processar camada: {e}")
        raise
    finally:
        if 'connection' in locals() and connection and not connection.closed:
            connection.close()