#etl_rfb_dados.py
import argparse
import os
import sys
import time
import logging
import shutil
import requests
import zipfile
import pathlib
import gc
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv  # Lembre-se de criar o aquivo .env com as configurações DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from bs4 import BeautifulSoup
from datetime import datetime

# Garantir diretório de logs
LOG_DIR = pathlib.Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Criar arquivo de log com codificação UTF-8
log_file = LOG_DIR / "etl_rfb_dados_log.txt"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8"
)

# Diretórios fixos para armazenar os arquivos
BASE_DIR = pathlib.Path().resolve()  # Diretório do script
OUTPUT_FILES_PATH = BASE_DIR / "files_downloaded"
EXTRACTED_FILES_PATH = BASE_DIR / "files_extracted"
ERRO_FILES_PATH = BASE_DIR / "files_error"

logging.info("Iniciando ETL - dados_rfb")

# carrega o arquivo de configuração .env
# Localiza o .env
dotenv_path = find_dotenv()

if not dotenv_path:
    logging.error("Arquivo de configuração .env não encontrado no diretório do projeto.")
    logging.info("ETL Finalizado.")
    sys.exit(1)

# Carrega o arquivo
load_dotenv(dotenv_path)


def connect_db(autocommit=False):
    from urllib.parse import quote_plus

    user = os.getenv("DB_USER")
    passw = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT") or "5432"
    database = os.getenv("DB_NAME")

    # Validação básica
    if not all([user, passw, host, database]):
        logging.error("Erro: variáveis de ambiente DB_* incompletas no .env")
        raise ValueError("Configuração do banco incompleta. Verifique o arquivo .env.")

    # Escapa caracteres especiais no usuário e senha
    user_escaped = quote_plus(user)
    passw_escaped = quote_plus(passw)

    # Monta a URL de conexão segura
    conn_str = f"postgresql://{user_escaped}:{passw_escaped}@{host}:{port}/{database}"

    # SQLAlchemy engine
    engine = create_engine(conn_str)

    # Conexão psycopg2
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=passw,
        host=host,
        port=port
    )

    if autocommit:
        conn.set_session(autocommit=True)

    return conn, engine


# Gerar base URL dinâmica (Ano e Mês atual)
def verificar_nova_atualizacao():
    # URL base
    base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    response = requests.get(base_url)

    if response.status_code != 200:
        logging.error(f"Erro ao acessar {base_url}: código {response.status_code}")
        return None

    # Parse do HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    datas = []

    # Coleta os pares (ano-mes, data de atualização)
    for tr in soup.find_all('tr'):
        tds = tr.find_all('td')
        if len(tds) >= 3:
            a_tag = tds[1].find('a')
            data_txt = tds[2].text.strip()
            if a_tag and data_txt:
                href = a_tag.get('href')
                if href and href.endswith('/') and len(href.rstrip('/')) == 7:
                    ano_mes = href.rstrip('/')  # ex: "2025-03"
                    try:
                        data_atualizacao = datetime.strptime(data_txt, "%Y-%m-%d %H:%M")
                        datas.append((ano_mes, data_atualizacao))
                    except:
                        continue

    if not datas:
        logging.info("Nenhuma pasta válida encontrada no site da RFB.")
        return None
    
    # Identifica a mais recente
    mais_recente = max(datas, key=lambda x: x[1])
    ano_mes, data_atualizacao = mais_recente
    ano, mes = map(int, ano_mes.split('-'))

    # cria a tabela info_dados
    if criar_tabela_info_dados():
        # Conecta ao banco para verificar se já existe
        conn, engine = connect_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT 1 FROM info_dados
            WHERE ano = %s AND mes = %s AND data_atualizacao = %s
            LIMIT 1;
        """, (ano, mes, data_atualizacao))

        existe = cur.fetchone()
        cur.close()
        conn.close()

        if existe:
            logging.info("A versão mais recente já está registrada no banco de dados.")
            return None

    # Se não existir, retorna os dados para serem usados posteriormente
    return {
        'ano': ano,
        'mes': mes,
        'data_atualizacao': data_atualizacao,
        'url': f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{ano}-{mes:02d}/"
    }


# Cria tabela info_dados e indexes
def criar_tabela_info_dados():
    conn, engine = connect_db()
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS info_dados (
            id SERIAL PRIMARY KEY,
            ano INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            data_atualizacao TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (ano, mes)
        );
        """)
        
        # Cria índice apenas se não existir
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = 'idx_info_dados_data_atualizacao'
                AND n.nspname = 'public'
            ) THEN
                CREATE INDEX idx_info_dados_data_atualizacao ON info_dados (data_atualizacao);
            END IF;
        END$$;
        """)

        # Criação da função e trigger para updated_at
        cur.execute("""
        CREATE OR REPLACE FUNCTION atualiza_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """)

        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger WHERE tgname = 'trg_atualiza_updated_at'
            ) THEN
                CREATE TRIGGER trg_atualiza_updated_at
                BEFORE UPDATE ON info_dados
                FOR EACH ROW
                EXECUTE FUNCTION atualiza_updated_at();
            END IF;
        END$$;
        """)

        conn.commit()
        logging.info("Tabelas info_dados e estruturas criadas/verificadas com sucesso!")
    conn.close()


# Cria as tabelas no banco de dados se elas não existirem.
def create_tables(conn):
    """
    Cria as tabelas no banco de dados se elas não existirem.
    """
    with conn.cursor() as cur:
        cur.execute("""
        BEGIN;

        -- Tabela: cnae
        CREATE TABLE IF NOT EXISTS public.cnae (
            codigo text PRIMARY KEY,
            descricao text
        );

        -- Tabela: empresa
        CREATE TABLE IF NOT EXISTS public.empresa (
            cnpj_basico text PRIMARY KEY,
            razao_social text,
            natureza_juridica text,
            qualificacao_responsavel text,
            capital_social double precision,
            porte_empresa text,
            ente_federativo_responsavel text
        );
                    
        -- Tabela: empresa_porte
        CREATE TABLE IF NOT EXISTS empresa_porte (
            codigo INTEGER PRIMARY KEY,
            descricao TEXT
        );
        COMMENT ON TABLE empresa_porte IS '01-Microempresa 03-Empresa de Pequeno Porte 05-Demais';

        INSERT INTO empresa_porte (codigo, descricao) VALUES
            (1, 'Microempresa'),
            (3, 'Empresa de Pequeno Porte'),
            (5, 'Demais');
        ON CONFLICT (codigo) DO NOTHING;

        -- Tabela: estabelecimento
        CREATE TABLE IF NOT EXISTS public.estabelecimento (
            cnpj_basico text,
            cnpj_ordem text,
            cnpj_dv text,
            identificador_matriz_filial text,
            nome_fantasia text,
            situacao_cadastral text,
            data_situacao_cadastral text,
            motivo_situacao_cadastral text,
            nome_cidade_exterior text,
            pais text,
            data_inicio_atividade text,
            cnae_fiscal_principal text,
            cnae_fiscal_secundaria text,
            tipo_logradouro text,
            logradouro text,
            numero text,
            complemento text,
            bairro text,
            cep text,
            uf text,
            municipio text,
            ddd_1 text,
            telefone_1 text,
            ddd_2 text,
            telefone_2 text,
            ddd_fax text,
            fax text,
            correio_eletronico text,
            situacao_especial text,
            data_situacao_especial text,
            PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
        );
                    
        -- Tabela: estabelecimento_situacao_cadastral
        CREATE TABLE IF NOT EXISTS estabelecimento_situacao_cadastral (
            codigo INTEGER PRIMARY KEY,
            descricao TEXT
        );
        COMMENT ON TABLE estabelecimento_situacao_cadastral IS '01-Nula 02-Ativa 03-Suspensa 04-Inapta 05-Ativa Não Regular 08-Baixada';

        INSERT INTO estabelecimento_situacao_cadastral (codigo, descricao) VALUES
            (1, 'Nula'),
            (2, 'Ativa'),
            (3, 'Suspensa'),
            (4, 'Inapta'),
            (5, 'Ativa Não Regular'),
            (8, 'Baixada');
        ON CONFLICT (codigo) DO NOTHING;
                    
        -- Tabela: moti
        CREATE TABLE IF NOT EXISTS public.moti (
            codigo text PRIMARY KEY,
            descricao text
        );

        -- Tabela: munic
        CREATE TABLE IF NOT EXISTS public.munic (
            codigo text PRIMARY KEY,
            descricao text
        );

        -- Tabela: natju
        CREATE TABLE IF NOT EXISTS public.natju (
            codigo text PRIMARY KEY,
            descricao text
        );

        -- Tabela: pais
        CREATE TABLE IF NOT EXISTS public.pais (
            codigo text PRIMARY KEY,
            descricao text
        );

        -- Tabela: quals
        CREATE TABLE IF NOT EXISTS public.quals (
            codigo text PRIMARY KEY,
            descricao text
        );

        -- Tabela: simples
        CREATE TABLE IF NOT EXISTS public.simples (
            cnpj_basico text PRIMARY KEY,
            opcao_pelo_simples text,
            data_opcao_simples text,
            data_exclusao_simples text,
            opcao_mei text,
            data_opcao_mei text,
            data_exclusao_mei text
        );

        -- Tabela: socios
        CREATE TABLE IF NOT EXISTS public.socios (
            cnpj_basico text PRIMARY KEY,
            identificador_socio text,
            nome_socio_razao_social text,
            cpf_cnpj_socio text,
            qualificacao_socio text,
            data_entrada_sociedade text,
            pais text,
            representante_legal text,
            nome_do_representante text,
            qualificacao_representante_legal text,
            faixa_etaria text
        );

        COMMENT ON TABLE public.socios IS 'Tabela com os dados dos sócios das empresas';

        -- Tabela: socios_identificador
        CREATE TABLE IF NOT EXISTS socios_identificador (
            codigo INTEGER PRIMARY KEY,
            descricao TEXT
        );
        COMMENT ON TABLE socios_identificador IS '1-Pessoa Juridica 2-Pessoa Fisica 3-Sócio Estrangeiro';

        INSERT INTO socios_identificador (codigo, descricao) VALUES
            (1, 'Pessoa Jurídica'),
            (2, 'Pessoa Física'),
            (3, 'Sócio Estrangeiro');
        ON CONFLICT (codigo) DO NOTHING;                 

        COMMIT;
        """)
        conn.commit()
    logging.info("Tabelas criadas/verificadas com sucesso!")


# insere os dados na tabela info_dados
def inserir_info_dados(conn, info):
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO info_dados (ano, mes, data_atualizacao)
        VALUES (%s, %s, %s)
        ON CONFLICT (ano, mes)
        DO UPDATE SET data_atualizacao = EXCLUDED.data_atualizacao;
    """, (info['ano'], info['mes'], info['data_atualizacao']))

    conn.commit()
    logging.info(f"Atualização inserida no banco: {info['ano']}-{info['mes']:02d}")


# traz a lista de arquivos da url
def get_files(base_url):
    response = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        logging.info(f"Baixando arquivos do mês: {base_url}")

        # Identificar arquivos ZIP disponíveis para download
        files = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".zip")]

        if not files:
            logging.info("Nenhum arquivo .zip encontrado para o mês atual.")
            return []

        return sorted(files)
    else:
        logging.error(f'Erro ao acessar {base_url}: código {response.status_code}')
        return []


# Função para verificar se o arquivo já foi baixado e se é necessário atualizar
def check_diff(url, file_name):
    if not os.path.isfile(file_name):
        return True

    try:
        response = requests.head(url, timeout=10)
        new_size = int(response.headers.get("content-length", 0))
    except Exception as e:
        logging.warning(f"Erro ao verificar cabeçalho de {url}: {e}")
        return True

    old_size = os.path.getsize(file_name)
    if new_size != old_size:
        os.remove(file_name)
        return True

    return False


# Função para baixar arquivos com barra de progresso
def download_file(url, output_path):
    response = requests.get(url, stream=True)
    file_name = os.path.join(output_path, url.split("/")[-1])

    if not check_diff(url, file_name):
            logging.info(f"Arquivo {file_name} já está atualizado.")
            return file_name
    
    logging.info(f"Baixando {file_name}...")
    
    with open(file_name, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logging.info(f"Download concluído para {file_name}")
    return file_name


# Função para extrair arquivos ZIP
def extract_files(zip_path, extract_to):
    logging.info(f"Extraindo {zip_path} para {extract_to}...")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


# Cria os indices nas tabelas, exceto da info_dados
def criar_indices():
    try:
        conn, _ = connect_db(autocommit=True)  # <- agora com autocommit
        cur = conn.cursor()

        logging.info("Iniciando criação dos índices...")

        indices = [
            ("empresa", "cnpj_basico"),
            ("estabelecimento", "cnpj_basico"),
            ("socios", "cnpj_basico"),
            ("simples", "cnpj_basico"),
        ]

        for tabela, coluna in indices:
            nome_indice = f"{tabela}_{coluna}_idx"
            try:
                sql = f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {nome_indice} ON {tabela} ({coluna});'
                cur.execute(sql)
                logging.info(f"Índice {nome_indice} criado com sucesso.")
            except Exception as e:
                logging.error(f"Erro ao criar o índice {nome_indice}: {e}", exc_info=True)

        cur.close()
        conn.close()
        gc.collect()
        logging.info("Criação dos índices finalizada.")

    except Exception as e:
        logging.error(f"Erro geral ao criar índices: {e}", exc_info=True)


def move_file_error(extracted_file_path, arquivo):
    try:
        shutil.move(extracted_file_path, ERRO_FILES_PATH / arquivo)
        logging.info(f"Arquivo {arquivo} movido para a pasta de erro: {ERRO_FILES_PATH}")
    except Exception as move_err:
        logging.error(f"Erro ao mover o arquivo {arquivo} para a pasta erro: {move_err}")


# Função principal do ETL
def etl_process():
    try:
        # Criar os diretórios caso não existam
        OUTPUT_FILES_PATH.mkdir(parents=True, exist_ok=True)
        EXTRACTED_FILES_PATH.mkdir(parents=True, exist_ok=True)
        ERRO_FILES_PATH.mkdir(parents=True, exist_ok=True)

        logging.info(f"Diretórios definidos:\n - Output: {OUTPUT_FILES_PATH}\n - Extraídos: {EXTRACTED_FILES_PATH}\n - Arquivos com erro: {ERRO_FILES_PATH}")

        start_time = time.time()

        info = verificar_nova_atualizacao()
        
        files = get_files(info['url'])

        logging.info(f"Arquivos encontrados ({len(files)}): {sorted(files)}")

        # Baixar arquivos
        zip_files = [download_file(info['url'] + file, OUTPUT_FILES_PATH) for file in files]

        # Extrair arquivos
        for zip_file in zip_files:
            extract_files(zip_file, EXTRACTED_FILES_PATH)

        logging.info("Todos os arquivos foram baixados e extraídos. Iniciando processamento dos dados.")
        
        # Conectar ao banco de dados
        conn, engine = connect_db()
        cur = conn.cursor()

        # Criar tabelas antes de inserir dados
        create_tables(conn)
        
        # Processar os arquivos após download completo
        arquivos_empresa = []
        arquivos_estabelecimento = []
        arquivos_socios = []
        arquivos_simples = []
        arquivos_cnae = []
        arquivos_moti = []
        arquivos_munic = []
        arquivos_natju = []
        arquivos_pais = []
        arquivos_quals = []

        # Arquivos com erro no processamento
        arquivos_com_erro = []
        
        for file in os.listdir(EXTRACTED_FILES_PATH):
            if "EMPRE" in file:
                arquivos_empresa.append(file)
            elif "ESTABELE" in file:
                arquivos_estabelecimento.append(file)
            elif "SOCIO" in file:
                arquivos_socios.append(file)
            elif "SIMPLES" in file:
                arquivos_simples.append(file)
            elif "CNAE" in file:
                arquivos_cnae.append(file)
            elif "MOTI" in file:
                arquivos_moti.append(file)
            elif "MUNIC" in file:
                arquivos_munic.append(file)
            elif "NATJU" in file:
                arquivos_natju.append(file)
            elif "PAIS" in file:
                arquivos_pais.append(file)
            elif "QUALS" in file:
                arquivos_quals.append(file)

        # deixar em ordem alfabética
        arquivos_empresa.sort()
        arquivos_estabelecimento.sort()
        arquivos_socios.sort()
        arquivos_simples.sort()
        arquivos_cnae.sort()
        arquivos_moti.sort()
        arquivos_munic.sort()
        arquivos_natju.sort()
        arquivos_pais.sort()
        arquivos_quals.sort()
        
        # Começa arquivos_empresa
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "empresa" CASCADE;')
        conn.commit()
        for arquivo in arquivos_empresa:
            logging.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logging.warning(f"Arquivo não encontrado: {extracted_file_path}")
                continue

            try:
                empresa_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: object}

                for i, chunk in enumerate(pd.read_csv(extracted_file_path,
                                                    sep=';',
                                                    header=None,
                                                    dtype=empresa_dtypes,
                                                    encoding='latin-1',
                                                    chunksize=2_000_000)):

                    chunk.columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']

                    def tratar_capital(x):
                        try:
                            return float(str(x).replace(',', '.'))
                        except:
                            return 0.0

                    chunk['capital_social'] = chunk['capital_social'].apply(tratar_capital)
                    
                    try:
                        chunk.to_sql(name='empresa', con=engine, if_exists='append', index=False, chunksize=10_000)
                        logging.info(f"Arquivo {arquivo} / parte {i} inserido com sucesso!")
                    except Exception as e:
                        logging.error(f"Erro ao inserir chunk {i} do arquivo {arquivo}: {e}")
                        try:
                            conn.rollback()
                            logging.warning("Rollback realizado. Tentando inserir novamente o chunk...")

                            # Tenta de novo
                            chunk.to_sql(
                                name='empresa',
                                con=engine,
                                if_exists='append',
                                index=False,
                                chunksize=10_000
                            )
                            logging.info(f"Chunk {i} reprocessado com sucesso!")
                        except Exception as segunda_falha:
                            logging.error(f"Falha novamente ao reprocessar chunk {i}: {segunda_falha}")
                            break  # desiste desse arquivo

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)

            finally:
                if 'chunk' in locals():
                    del chunk
                gc.collect()

        logging.info("Arquivos de empresa finalizados!")

        # Começa arquivos_estabelecimento
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "estabelecimento" CASCADE;')
        conn.commit()
        for arquivo in arquivos_estabelecimento:
            logging.info(f"Trabalhando no arquivo: {arquivo}")
            
            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logging.warning(f"Arquivo não encontrado: {extracted_file_path}")
                continue

            try:
                estabelecimento_dtypes = {
                    0: object, 1: object, 2: object, 3: 'Int32', 4: object, 5: 'Int32', 6: 'Int32',
                    7: 'Int32', 8: object, 9: object, 10: 'Int32', 11: 'Int32', 12: object, 13: object,
                    14: object, 15: object, 16: object, 17: object, 18: object, 19: object,
                    20: 'Int32', 21: object, 22: object, 23: object, 24: object, 25: object,
                    26: object, 27: object, 28: object, 29: 'Int32'
                }

                for i, chunk in enumerate(pd.read_csv(
                    extracted_file_path,
                    sep=';',
                    header=None,
                    dtype=estabelecimento_dtypes,
                    encoding='latin-1',
                    chunksize=2_000_000
                )):

                    chunk.columns = [
                        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
                        'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
                        'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
                        'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
                        'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep',
                        'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
                        'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial',
                        'data_situacao_especial'
                    ]

                    chunk.to_sql(
                        name='estabelecimento',
                        con=engine,
                        if_exists='append',
                        index=False,
                        chunksize=10_000
                    )

                    logging.info(f"Arquivo {arquivo} / parte {i} inserido com sucesso no banco de dados!")

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)

            finally:
                if 'chunk' in locals():
                    del chunk
                gc.collect()

        logging.info("Arquivos de estabelecimento finalizados!")

        # Arquivos de socios:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "socios" CASCADE;')
        conn.commit()
        for arquivo in arquivos_socios:
            logging.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logging.warning(f"Arquivo não encontrado: {extracted_file_path}")
                continue

            try:
                socios_dtypes = {
                    0: object, 1: 'Int32', 2: object, 3: object, 4: 'Int32',
                    5: 'Int32', 6: 'Int32', 7: object, 8: object,
                    9: 'Int32', 10: 'Int32'
                }

                for i, chunk in enumerate(pd.read_csv(
                    extracted_file_path,
                    sep=';',
                    header=None,
                    dtype=socios_dtypes,
                    encoding='latin-1',
                    chunksize=2_000_000
                )):
                    # Renomear colunas
                    chunk.columns = [
                        'cnpj_basico',
                        'identificador_socio',
                        'nome_socio_razao_social',
                        'cpf_cnpj_socio',
                        'qualificacao_socio',
                        'data_entrada_sociedade',
                        'pais',
                        'representante_legal',
                        'nome_do_representante',
                        'qualificacao_representante_legal',
                        'faixa_etaria'
                    ]

                    # Gravar dados no banco:
                    chunk.to_sql(
                        name='socios',
                        con=engine,
                        if_exists='append',
                        index=False,
                        chunksize=10_000
                    )

                    logging.info(f"Arquivo {arquivo} / parte {i} inserido com sucesso no banco de dados!")

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)

            finally:
                if 'chunk' in locals():
                    del chunk
                gc.collect()

        logging.info("Arquivos de socios finalizados!")

        # Arquivos de simples:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "simples" CASCADE;')
        conn.commit()
        for arquivo in arquivos_simples:
            logging.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logging.warning(f"Arquivo não encontrado: {extracted_file_path}")
                continue

            try:
                simples_dtypes = {
                    0: object,
                    1: object,
                    2: 'Int32',
                    3: 'Int32',
                    4: object,
                    5: 'Int32',
                    6: 'Int32'
                }

                for i, chunk in enumerate(pd.read_csv(
                    extracted_file_path,
                    sep=';',
                    header=None,
                    dtype=simples_dtypes,
                    encoding='latin-1',
                    chunksize=2_000_000
                )):
                    # Renomear colunas
                    chunk.columns = [
                        'cnpj_basico',
                        'opcao_pelo_simples',
                        'data_opcao_simples',
                        'data_exclusao_simples',
                        'opcao_mei',
                        'data_opcao_mei',
                        'data_exclusao_mei'
                    ]

                    # Gravar dados no banco
                    chunk.to_sql(
                        name='simples',
                        con=engine,
                        if_exists='append',
                        index=False,
                        chunksize=10_000
                    )

                    logging.info(f"Arquivo {arquivo} / parte {i} inserido com sucesso no banco de dados!")

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)
            finally:
                if 'chunk' in locals():
                    del chunk
                gc.collect()

        logging.info("Arquivos do simples finalizados!")

        # Arquivos de cnae:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "cnae" CASCADE;')
        conn.commit()
        for arquivo in arquivos_cnae:
            logging.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logging.warning(f"Arquivo não encontrado: {extracted_file_path}")
                continue

            try:
                cnae = pd.read_csv(
                    filepath_or_buffer=extracted_file_path,
                    sep=';',
                    header=None,
                    dtype='object',
                    encoding='latin-1'
                )

                # Renomear colunas
                cnae.columns = ['codigo', 'descricao']

                # Gravar dados no banco
                cnae.to_sql(
                    name='cnae',
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=10_000
                )

                logging.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)
            finally:
                if 'cnae' in locals():
                    del cnae
                gc.collect()

        logging.info("Arquivos de cnae finalizados!")

        # Arquivos de moti:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "moti" CASCADE;')
        conn.commit()
        for arquivo in arquivos_moti:
            logging.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logging.warning(f"Arquivo não encontrado: {extracted_file_path}")
                continue

            try:
                moti_dtypes = {
                    0: 'Int32',
                    1: object
                }

                moti = pd.read_csv(
                    filepath_or_buffer=extracted_file_path,
                    sep=';',
                    header=None,
                    dtype=moti_dtypes,
                    encoding='latin-1'
                )

                # Renomear colunas
                moti.columns = ['codigo', 'descricao']

                # Gravar dados no banco
                moti.to_sql(
                    name='moti',
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=10_000
                )

                logging.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)
            finally:
                if 'moti' in locals():
                    del moti
                gc.collect()

        logging.info("Arquivos de moti finalizados!")

        # Arquivos de munic:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "munic" CASCADE;')
        conn.commit()
        for arquivo in arquivos_munic:
            logging.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logging.warning(f"Arquivo não encontrado: {extracted_file_path}")
                continue

            try:
                munic_dtypes = {
                    0: 'Int32',
                    1: object
                }

                munic = pd.read_csv(
                    filepath_or_buffer=extracted_file_path,
                    sep=';',
                    header=None,
                    dtype=munic_dtypes,
                    encoding='latin-1'
                )

                # Renomear colunas
                munic.columns = ['codigo', 'descricao']

                # Gravar dados no banco
                munic.to_sql(
                    name='munic',
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=10_000
                )

                logging.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)
            finally:
                if 'munic' in locals():
                    del munic
                gc.collect()

        logging.info("Arquivos de munic finalizados!")

        # Arquivos de natju:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "natju" CASCADE;')
        conn.commit()
        for arquivo in arquivos_natju:
            logging.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logging.warning(f"Arquivo não encontrado: {extracted_file_path}")
                continue

            try:
                natju_dtypes = {
                    0: 'Int32',
                    1: object
                }

                natju = pd.read_csv(
                    filepath_or_buffer=extracted_file_path,
                    sep=';',
                    header=None,
                    dtype=natju_dtypes,
                    encoding='latin-1'
                )

                # Renomear colunas
                natju.columns = ['codigo', 'descricao']

                # Gravar dados no banco
                natju.to_sql(
                    name='natju',
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=10_000
                )

                logging.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)
            finally:
                if 'natju' in locals():
                    del natju
                gc.collect()

        logging.info("Arquivos de natju finalizados!")

        # Arquivos de pais:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "pais" CASCADE;')
        conn.commit()
        for arquivo in arquivos_pais:
            logging.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logging.warning(f"Arquivo não encontrado: {extracted_file_path}")
                continue

            try:
                pais_dtypes = {
                    0: 'Int32',
                    1: object
                }

                pais = pd.read_csv(
                    filepath_or_buffer=extracted_file_path,
                    sep=';',
                    header=None,
                    dtype=pais_dtypes,
                    encoding='latin-1'
                )

                # Renomear colunas
                pais.columns = ['codigo', 'descricao']

                # Gravar dados no banco
                pais.to_sql(
                    name='pais',
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=10_000
                )

                logging.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)
            finally:
                if 'pais' in locals():
                    del pais
                gc.collect()

        logging.info("Arquivos de pais finalizados!")

        # Arquivos de qualificação de sócios:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "quals" CASCADE;')
        conn.commit()
        for arquivo in arquivos_quals:
            logging.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logging.warning(f"Arquivo não encontrado: {extracted_file_path}")
                continue

            try:
                quals_dtypes = {
                    0: 'Int32',
                    1: object
                }

                quals = pd.read_csv(
                    filepath_or_buffer=extracted_file_path,
                    sep=';',
                    header=None,
                    dtype=quals_dtypes,
                    encoding='latin-1'
                )

                # Renomear colunas
                quals.columns = ['codigo', 'descricao']

                # Gravar dados no banco
                quals.to_sql(
                    name='quals',
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=10_000
                )

                logging.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)
            finally:
                if 'quals' in locals():
                    del quals
                gc.collect()

        # Grava os dados e gera um txt com os arquivos com erro
        if arquivos_com_erro:
            logging.warning(f"Arquivos com erro: {arquivos_com_erro}")
            
            with open("arquivos_com_erro.txt", "w", encoding="utf-8") as f:
                for nome in arquivos_com_erro:
                    f.write(nome + "\n")
            for arquivo in arquivos_com_erro:
                # Move os arquivos com erro para uma subpasta chamada erro/ para facilitar o controle:
                shutil.move(extracted_file_path, ERRO_FILES_PATH / arquivo)

        # Inserir os dados da ultima atualização na tabela info_dados
        inserir_info_dados(conn, info)

        cur.close() # Fecha a conexão com o banco
        conn.close() # Fecha a conexão com o banco

        logging.info("Arquivos de quals finalizados!")

        # Processo de inserção finalizado
        logging.info('Processo de carga dos arquivos finalizado!')

        # Criação dos índices
        criar_indices()

        # Remover arquivos após a inserção no banco
        shutil.rmtree(OUTPUT_FILES_PATH)
        shutil.rmtree(EXTRACTED_FILES_PATH)
        logging.info("Arquivos removidos após a carga no banco.")

        total_time = time.time() - start_time
        logging.info(f"ETL concluído com sucesso em {total_time:.2f} segundos!")

    except Exception as e:
        logging.error(f"Erro no processo ETL: {e}", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Receita Federal do Brasil")
    parser.add_argument(
        "--etl",
        action="store_true",
        help="Executa o processo completo de ETL"
    )
    parser.add_argument(
        "--inserir_indice",
        action="store_true",
        help="Cria índices nas tabelas do banco"
    )

    args = parser.parse_args()

    if args.etl:
        etl_process()

    if args.inserir_indice:
        criar_indices()
