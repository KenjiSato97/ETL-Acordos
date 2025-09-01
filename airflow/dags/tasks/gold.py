import boto3
import pandas as pd
import io
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def transform_and_load_gold(bucket_name, key, endpoint_url, access_key, secret_key, bucket3_name='gold'):
    """
    Agregação de dados, criação de dimensões hierárquicas, filtragem irrelevante e formatação para visualização, salvando a camada gold
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
            raise Exception(f"A chave especificada '{key}' não existe no bucket '{bucket_name}'.")

        # Ler dados do MinIO
        response = minio_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_parquet(io.BytesIO(response['Body'].read()))

        # Criação de dimensões hierárquicas
        df_hier = df.copy()
        df_hier['local_completo'] = df_hier['continente'] + ' > ' + df_hier['região'] + ' > ' + df_hier['local_de_assinatura']
        df_hier['acordo_recurso'] = df_hier['tipo_de_acordo'] + ' - ' + df_hier['recursos']

        #Separação em dois dataframes diferentes os acordos que são de tipo de parceiro diferentes
        df_pais = df_hier[df_hier['tipo_de_parceiro'] == 'País']
        df_org = df_hier[df_hier['tipo_de_parceiro'] == 'Organização']

        




 # Transformação da camada gold
        gld_acordos_df = df[['parceiro', 'tipo_de_parceiro', 'continente', 'região', 'local_de_assinatura', 'tipo_de_acordo', 'título', 'objetivo', 'recursos', 'tipo_de_documento', 'ano']].drop_duplicates()

 # Salvar camada gold no MinIO
        for gld_df, gld_name in [
            (gld_acordos_df, 'gld_acordos'),
            (df_hier[['local_completo', 'acordo_recurso']], 'gld_hier'),
            (df_pais[['local_completo', 'acordo_recurso']], 'gld_pais'),
            (df_org[['local_completo', 'acordo_recurso']], 'gld_org')
        ]:
            output_buffer = io.BytesIO()
            gld_df.to_parquet(output_buffer, index=False)
            output_buffer.seek(0)
            gld_key = f'gold/{gld_name}.parquet'
            minio_client.put_object(
                Bucket=bucket3_name,
                Key=gld_key,
                Body=output_buffer.getvalue()
            )
            logging.info(f"Camada '{gld_name}' salva no MinIO com a chave: {gld_key}")
            
            # Criar e popular tabelas no MariaDB
            mysql_hook = MySqlHook(mysql_conn_id='local_mariadb')
            connection = mysql_hook.get_conn()
            with connection.cursor() as cursor:
                # Criar tabela gld_acordos
                create_table_acordos_sql = """
                CREATE TABLE IF NOT EXISTS gld_acordos (
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
                logging.info("Tabela gld_acordos criada/verificada com sucesso.")

                # Criar tabela gld_hier
                create_table_hier_sql = """
                CREATE TABLE IF NOT EXISTS gld_hier (
                    cod_local INT PRIMARY KEY AUTO_INCREMENT,
                    local_completo VARCHAR(255),
                    acordo_recurso VARCHAR(255)
                )
                """
                cursor.execute(create_table_hier_sql)
                logging.info("Tabela gld_hier criada/verificada com sucesso.")

                # Criar tabela gld_pais 
                create_table_pais_sql = """
                CREATE TABLE IF NOT EXISTS gld_pais (
                    cod_local INT PRIMARY KEY AUTO_INCREMENT,
                    local_completo VARCHAR(255),
                    acordo_recurso VARCHAR(255)
                )
                """
                cursor.execute(create_table_pais_sql)
                logging.info("Tabela gld_pais criada/verificada com sucesso.")

                # Criar tabela gld_org
                create_table_org_sql = """
                CREATE TABLE IF NOT EXISTS gld_org (
                    cod_local INT PRIMARY KEY AUTO_INCREMENT,
                    local_completo VARCHAR(255),
                    acordo_recurso VARCHAR(255)
                )
                """
                cursor.execute(create_table_org_sql)
                logging.info("Tabela gld_org criada/verificada com sucesso.")
                
                
                # Inserir dados
                if gld_name == 'gld_acordos':
                    for _, row in gld_df.iterrows():
                        insert_sql = """
                        INSERT INTO gld_acordos
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
                    logging.info("Dados inseridos/atualizados na tabela gld_acordos.")

                elif gld_name == 'gld_hier':
                    for _, row in gld_df.iterrows():
                        insert_sql = """
                        INSERT INTO gld_hier
                        (local_completo, acordo_recurso)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE
                        local_completo=VALUES(local_completo),
                        acordo_recurso=VALUES(acordo_recurso)
                        """
                        cursor.execute(insert_sql, tuple(row))
                    connection.commit()
                    logging.info("Dados inseridos/atualizados na tabela gld_hier.")

                elif gld_name == 'gld_pais':
                    for _, row in gld_df.iterrows():
                        insert_sql = """
                        INSERT INTO gld_pais
                        (local_completo, acordo_recurso)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE
                        local_completo=VALUES(local_completo),
                        acordo_recurso=VALUES(acordo_recurso)
                        """
                        cursor.execute(insert_sql, tuple(row))
                    connection.commit()
                    logging.info("Dados inseridos/atualizados na tabela gld_pais.")

                elif gld_name == 'gld_org':
                    for _, row in gld_df.iterrows():
                        insert_sql = """
                        INSERT INTO gld_org
                        (local_completo, acordo_recurso)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE
                        local_completo=VALUES(local_completo),
                        acordo_recurso=VALUES(acordo_recurso)
                        """
                        cursor.execute(insert_sql, tuple(row))
                    connection.commit()
                    logging.info("Dados inseridos/atualizados na tabela gld_org.")


    except Exception as e:
        logging.error(f"Erro ao processar camada: {e}")
        raise
    finally:
        if 'connection' in locals() and connection and not connection.closed:
            connection.close()