# etl_rfb_dados.py
import argparse
import os
import sys
import logging
import shutil
import requests
import zipfile
import pathlib
import gc
import tempfile
from datetime import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv
from bs4 import BeautifulSoup

# =============================================================================
# CONFIGURAÇÃO DE LOG
# =============================================================================

LOG_DIR = pathlib.Path("logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / "etl_rfb_dados_log.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)

# =============================================================================
# DIRETÓRIOS BÁSICOS
# =============================================================================

BASE_DIR = pathlib.Path().resolve()
OUTPUT_FILES_PATH = BASE_DIR / "files_downloaded"
EXTRACTED_FILES_PATH = BASE_DIR / "files_extracted"
ERRO_FILES_PATH = BASE_DIR / "files_error"

# Tamanho de chunk para EMPRESA (Pandas)
EMPRESA_CHUNK_ROWS = 200_000

logger.info("Iniciando ETL - dados_rfb")

# =============================================================================
# CARREGA .env
# =============================================================================

ENV_PATH_PARENT = BASE_DIR.parent / "dados_rfb_env" / ".env"
ENV_PATH_LOCAL = BASE_DIR / ".env"

if os.path.exists(ENV_PATH_PARENT):
    dotenv_path = find_dotenv(str(ENV_PATH_PARENT))
else:
    dotenv_path = find_dotenv(str(ENV_PATH_LOCAL))

if not dotenv_path:
    logger.error("Arquivo de configuração .env não encontrado.")
    logger.info("ETL Finalizado.")
    sys.exit(1)

load_dotenv(dotenv_path)


# =============================================================================
# FUNÇÕES UTILITÁRIAS
# =============================================================================

def converter_segundos(tempo_inicial: datetime, tempo_final: datetime) -> str:
    """
    Converte diferença de tempo em frase amigável: X horas, Y minutos, Z segundos.
    """
    diferenca = tempo_final - tempo_inicial
    total_segundos = int(diferenca.total_seconds())

    horas = total_segundos // 3600
    minutos = (total_segundos % 3600) // 60
    segundos = total_segundos % 60

    partes = []
    if horas == 1:
        partes.append("1 hora")
    elif horas > 1:
        partes.append(f"{horas} horas")

    if minutos == 1:
        partes.append("1 minuto")
    elif minutos > 1:
        partes.append(f"{minutos} minutos")

    if segundos == 1:
        partes.append("1 segundo")
    elif segundos > 1:
        partes.append(f"{segundos} segundos")
    elif segundos < 1:
        partes.append(f"{diferenca} segundos")

    return ", ".join(partes)


def connect_db(autocommit=False):
    """
    Cria conexão psycopg2 + engine SQLAlchemy.
    """
    from urllib.parse import quote_plus

    user = os.getenv("DB_USER")
    passw = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT") or "5432"
    database = os.getenv("DB_NAME")

    if passw is None or passw.strip() == "":
        logger.warning("⚠️ DB_PASSWORD está em branco ou vazia. Verifique seu .env.")
        print("\n⚠️ AVISO CRÍTICO: DB_PASSWORD está vazia!")
        print("Edite o arquivo .env e adicione: DB_PASSWORD=sua_senha_postgres\n")

    if not all([user, host, database]):
        missing = []
        if not user:
            missing.append("DB_USER")
        if not host:
            missing.append("DB_HOST")
        if not database:
            missing.append("DB_NAME")
        logger.error(f"Variáveis faltando no .env: {', '.join(missing)}")
        raise ValueError(
            f"Configuração do banco incompleta. Variáveis faltando: {', '.join(missing)}"
        )

    user_escaped = quote_plus(user)
    passw_escaped = quote_plus(passw) if passw else ""

    conn_str = f"postgresql://{user_escaped}:{passw_escaped}@{host}:{port}/{database}"

    try:
        engine = create_engine(conn_str)

        conn = psycopg2.connect(
            dbname=database, user=user, password=passw, host=host, port=port
        )

        if autocommit:
            conn.set_session(autocommit=True)

        logger.info("✅ Conexão com o banco de dados estabelecida com sucesso")
        return conn, engine

    except psycopg2.OperationalError as e:
        logger.error(f"❌ Erro de conexão com o banco de dados: {e}")
        if "password authentication failed" in str(e):
            logger.error("Falha na autenticação. Verifique a senha no .env.")
        elif "does not exist" in str(e).lower():
            logger.error("Banco de dados não existe. Crie o banco antes.")
        raise
    except Exception as e:
        logger.error(f"❌ Erro inesperado na conexão: {e}")
        raise


# =============================================================================
# INFO DADOS (VERIFICAÇÃO DE NOVA ATUALIZAÇÃO RFB)
# =============================================================================

def criar_tabela_info_dados():
    """
    Cria a tabela info_dados para registrar metadados de atualização.
    """
    conn, engine = connect_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS info_dados (
                id SERIAL PRIMARY KEY,
                ano INTEGER NOT NULL,
                mes INTEGER NOT NULL,
                data_atualizacao TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (ano, mes)
            );
            """
            )

            cur.execute(
                """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = 'idx_info_dados_data_atualizacao'
                    AND n.nspname = 'public'
                ) THEN
                    CREATE INDEX idx_info_dados_data_atualizacao
                    ON info_dados (data_atualizacao);
                END IF;
            END$$;
            """
            )

            cur.execute(
                """
            CREATE OR REPLACE FUNCTION atualiza_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
            )

            cur.execute(
                """
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
            """
            )

            conn.commit()
            logger.info("Tabela info_dados e estruturas criadas/verificadas.")
    finally:
        conn.close()
        engine.dispose()


def inserir_info_dados(conn, info):
    """
    Registra no banco a atualização mais recente.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
        INSERT INTO info_dados (ano, mes, data_atualizacao)
        VALUES (%s, %s, %s)
        ON CONFLICT (ano, mes)
        DO UPDATE SET data_atualizacao = EXCLUDED.data_atualizacao;
        """,
            (info["ano"], info["mes"], info["data_atualizacao"]),
        )
    conn.commit()
    logger.info(f"Atualização registrada: {info['ano']}-{info['mes']:02d}")


def verificar_nova_atualizacao():
    """
    Verifica no site da RFB qual é a pasta mais recente (ano-mês) e se já foi carregada.
    Retorna dict com {ano, mes, data_atualizacao, url} ou None se nada novo.
    """
    base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    response = requests.get(base_url)

    if response.status_code != 200:
        logger.error(f"Erro ao acessar {base_url}: código {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    datas = []

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 3:
            a_tag = tds[1].find("a")
            data_txt = tds[2].text.strip()
            if a_tag and data_txt:
                href = a_tag.get("href")
                if href and href.endswith("/") and len(href.rstrip("/")) == 7:
                    ano_mes = href.rstrip("/")
                    try:
                        data_atualizacao = datetime.strptime(
                            data_txt, "%Y-%m-%d %H:%M"
                        )
                        datas.append((ano_mes, data_atualizacao))
                    except Exception:
                        continue

    if not datas:
        logger.info("Nenhuma pasta válida encontrada no site da RFB.")
        return None

    mais_recente = max(datas, key=lambda x: x[1])
    ano_mes, data_atualizacao = mais_recente
    ano, mes = map(int, ano_mes.split("-"))

    # Garante tabela info_dados
    criar_tabela_info_dados()

    conn, engine = connect_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
            SELECT 1 FROM info_dados
            WHERE ano = %s AND mes = %s AND data_atualizacao = %s
            LIMIT 1;
            """,
                (ano, mes, data_atualizacao),
            )
            existe = cur.fetchone()

        if existe:
            logger.info("A versão mais recente já está registrada. Nada a fazer.")
            return None

    finally:
        conn.close()
        engine.dispose()

    return {
        "ano": ano,
        "mes": mes,
        "data_atualizacao": data_atualizacao,
        "url": f"{base_url}{ano}-{mes:02d}/",
    }


# =============================================================================
# DOWNLOAD E EXTRAÇÃO
# =============================================================================

def get_files(base_url):
    response = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        logger.info(f"Listando arquivos .zip em: {base_url}")

        files = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".zip")]

        if not files:
            logger.info("Nenhum arquivo .zip encontrado.")
            return []

        return sorted(files)

    logger.error(f"Erro ao acessar {base_url}: código {response.status_code}")
    return []


def check_diff(url, file_name):
    """
    Verifica se o arquivo local difere do remoto (tamanho). Se diferente, apaga o local.
    """
    if not os.path.isfile(file_name):
        return True

    try:
        response = requests.head(url, timeout=10)
        new_size = int(response.headers.get("content-length", 0))
    except Exception as e:
        logger.warning(f"Erro ao verificar cabeçalho de {url}: {e}")
        return True

    old_size = os.path.getsize(file_name)

    if new_size != old_size:
        os.remove(file_name)
        return True

    return False


def download_file(url, output_path):
    response = requests.get(url, stream=True)
    file_name = os.path.join(output_path, url.split("/")[-1])

    if not check_diff(url, file_name):
        logger.info(f"Arquivo {file_name} já está atualizado.")
        return file_name

    logger.info(f"Baixando {file_name}...")
    with open(file_name, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(f"Download concluído: {file_name}")
    return file_name


def extract_files(zip_path, extract_to):
    logger.info(f"Extraindo {zip_path} para {extract_to}...")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


# =============================================================================
# ÍNDICES
# =============================================================================

def criar_indices():
    """
    Cria índices importantes para consultas futuras.
    """
    try:
        conn, _ = connect_db(autocommit=True)
        with conn.cursor() as cur:
            logger.info("Iniciando criação de índices...")

            indices = [
                ("empresa", "cnpj_basico"),
                ("estabelecimento", "cnpj_basico"),
                ("socios", "cnpj_basico"),
                ("simples", "cnpj_basico"),
            ]

            for tabela, coluna in indices:
                nome_indice = f"{tabela}_{coluna}_idx"
                try:
                    sql = (
                        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS "
                        f"{nome_indice} ON {tabela} ({coluna});"
                    )
                    cur.execute(sql)
                    logger.info(f"Índice {nome_indice} verificado/criado.")
                except Exception as e:
                    logger.error(
                        f"Erro ao criar índice {nome_indice}: {e}", exc_info=True
                    )

        gc.collect()
        logger.info("Criação de índices finalizada.")
    except Exception as e:
        logger.error(f"Erro geral ao criar índices: {e}", exc_info=True)
        logger.critical("Falha na etapa de índices.")


# =============================================================================
# MANIPULAÇÃO DE ARQUIVOS COM ERRO
# =============================================================================

def move_file_error(extracted_file_path, arquivo):
    try:
        ERRO_FILES_PATH.mkdir(parents=True, exist_ok=True)
        shutil.move(extracted_file_path, ERRO_FILES_PATH / arquivo)
        logger.info(f"Arquivo {arquivo} movido para a pasta de erro: {ERRO_FILES_PATH}")
    except Exception as move_err:
        logger.error(f"Erro ao mover o arquivo {arquivo} para pasta erro: {move_err}")


# =============================================================================
# COPY UTILITÁRIOS
# =============================================================================

def copy_csv_to_postgres(conn, table_name, file_path, delimiter=";", has_header=False, null_str=""):
    """
    Faz COPY direto de um arquivo CSV/Texto para uma tabela no PostgreSQL.
    """
    with conn.cursor() as cur:
        logger.info(f"Iniciando COPY para {table_name} a partir de {file_path}")

        copy_sql = f"""
            COPY {table_name}
            FROM STDIN
            WITH (
                FORMAT CSV,
                DELIMITER '{delimiter}',
                NULL '{null_str}',
                HEADER {'TRUE' if has_header else 'FALSE'}
            );
        """

        with open(file_path, "r", encoding="latin-1") as f:
            cur.copy_expert(copy_sql, f)

    conn.commit()
    logger.info(f"✔ COPY concluído para {table_name}")


# =============================================================================
# CRIAÇÃO DE TABELAS
# =============================================================================

def create_tables():
    """
    Cria as tabelas principais do schema (se não existirem).
    """
    conn, engine = connect_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
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

            -- Tabela: socios_identificador
            CREATE TABLE IF NOT EXISTS socios_identificador (
                codigo INTEGER PRIMARY KEY,
                descricao TEXT
            );
            """
            )

            # Inserts padrões
            cur.execute(
                """
            INSERT INTO empresa_porte (codigo, descricao) VALUES
                (1, 'Microempresa'),
                (3, 'Empresa de Pequeno Porte'),
                (5, 'Demais')
            ON CONFLICT (codigo) DO NOTHING;
            """
            )

            cur.execute(
                """
            INSERT INTO estabelecimento_situacao_cadastral (codigo, descricao) VALUES
                (1, 'Nula'),
                (2, 'Ativa'),
                (3, 'Suspensa'),
                (4, 'Inapta'),
                (5, 'Ativa Não Regular'),
                (8, 'Baixada')
            ON CONFLICT (codigo) DO NOTHING;
            """
            )

            cur.execute(
                """
            INSERT INTO socios_identificador (codigo, descricao) VALUES
                (1, 'Pessoa Jurídica'),
                (2, 'Pessoa Física'),
                (3, 'Sócio Estrangeiro')
            ON CONFLICT (codigo) DO NOTHING;
            """
            )

            conn.commit()
            logger.info("Tabelas principais criadas/verificadas com sucesso.")
    finally:
        conn.close()
        engine.dispose()


# =============================================================================
# PROCESSAMENTO POR TABELA (COPY)
# =============================================================================

def process_empresa_with_pandas(conn, file_path):
    """
    EMPRESA: usa Pandas apenas para tratar capital_social (virgula para ponto)
    e depois faz COPY a partir de CSV temporário.
    """
    logger.info(f"Processando EMPRESA com Pandas + COPY: {file_path}")

    chunk_iter = pd.read_csv(
        file_path,
        sep=";",
        header=None,
        encoding="latin-1",
        dtype=str,
        chunksize=EMPRESA_CHUNK_ROWS,
    )

    for i, chunk in enumerate(chunk_iter):
        chunk.columns = [
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo_responsavel",
        ]

        # Trata capital social
        chunk["capital_social"] = (
            chunk["capital_social"]
            .fillna("0")
            .str.replace(",", ".", regex=False)
        )

        # CSV temporário
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        tmp_name = tmp.name
        tmp.close()

        chunk.to_csv(tmp_name, sep=";", index=False, header=False)

        # COPY
        copy_csv_to_postgres(conn, "empresa", tmp_name, delimiter=";", has_header=False)

        os.remove(tmp_name)
        logger.info(f"✔ Chunk {i} de EMPRESA importado com sucesso.")

    logger.info("✔ EMPRESA finalizado com sucesso.")


def process_estabele(conn, file_path):
    copy_csv_to_postgres(conn, "estabelecimento", file_path, delimiter=";", has_header=False)


def process_socios(conn, file_path):
    copy_csv_to_postgres(conn, "socios", file_path, delimiter=";", has_header=False)


def process_simples(conn, file_path):
    copy_csv_to_postgres(conn, "simples", file_path, delimiter=";", has_header=False)


def process_tabela_simples(conn, file_path, table_name):
    copy_csv_to_postgres(conn, table_name, file_path, delimiter=";", has_header=False)


def processar_arquivos(conn):
    """
    Classifica arquivos extraídos por tipo e processa cada um via COPY.
    """
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

    # Ordena alfabeticamente
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

    # Limpa tabelas antes de inserir (sem dropar estrutura)
    with conn.cursor() as cur:
        if arquivos_empresa:
            cur.execute("TRUNCATE TABLE empresa;")
        if arquivos_estabelecimento:
            cur.execute("TRUNCATE TABLE estabelecimento;")
        if arquivos_socios:
            cur.execute("TRUNCATE TABLE socios;")
        if arquivos_simples:
            cur.execute("TRUNCATE TABLE simples;")
        if arquivos_cnae:
            cur.execute("TRUNCATE TABLE cnae;")
        if arquivos_moti:
            cur.execute("TRUNCATE TABLE moti;")
        if arquivos_munic:
            cur.execute("TRUNCATE TABLE munic;")
        if arquivos_natju:
            cur.execute("TRUNCATE TABLE natju;")
        if arquivos_pais:
            cur.execute("TRUNCATE TABLE pais;")
        if arquivos_quals:
            cur.execute("TRUNCATE TABLE quals;")

    conn.commit()

    # EMPRESA
    for arquivo in arquivos_empresa:
        extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
        logger.info(f"Trabalhando (EMPRESA) no arquivo: {arquivo}")
        try:
            process_empresa_with_pandas(conn, extracted_file_path)
        except Exception as e:
            logger.error(f"Erro ao processar EMPRESA {arquivo}: {e}", exc_info=True)
            arquivos_com_erro.append(arquivo)
            move_file_error(extracted_file_path, arquivo)

    logger.info("Arquivos de EMPRESA finalizados.")

    # ESTABELECIMENTO
    for arquivo in arquivos_estabelecimento:
        extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
        logger.info(f"Trabalhando (ESTABELE) no arquivo: {arquivo}")
        try:
            process_estabele(conn, extracted_file_path)
        except Exception as e:
            logger.error(f"Erro ao processar ESTABELE {arquivo}: {e}", exc_info=True)
            arquivos_com_erro.append(arquivo)
            move_file_error(extracted_file_path, arquivo)

    logger.info("Arquivos de ESTABELECIMENTO finalizados.")

    # SOCIOS
    for arquivo in arquivos_socios:
        extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
        logger.info(f"Trabalhando (SOCIO) no arquivo: {arquivo}")
        try:
            process_socios(conn, extracted_file_path)
        except Exception as e:
            logger.error(f"Erro ao processar SOCIO {arquivo}: {e}", exc_info=True)
            arquivos_com_erro.append(arquivo)
            move_file_error(extracted_file_path, arquivo)

    logger.info("Arquivos de SOCIOS finalizados.")

    # SIMPLES
    for arquivo in arquivos_simples:
        extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
        logger.info(f"Trabalhando (SIMPLES) no arquivo: {arquivo}")
        try:
            process_simples(conn, extracted_file_path)
        except Exception as e:
            logger.error(f"Erro ao processar SIMPLES {arquivo}: {e}", exc_info=True)
            arquivos_com_erro.append(arquivo)
            move_file_error(extracted_file_path, arquivo)

    logger.info("Arquivos de SIMPLES finalizados.")

    # CNAE
    for arquivo in arquivos_cnae:
        extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
        logger.info(f"Trabalhando (CNAE) no arquivo: {arquivo}")
        try:
            process_tabela_simples(conn, extracted_file_path, "cnae")
        except Exception as e:
            logger.error(f"Erro ao processar CNAE {arquivo}: {e}", exc_info=True)
            arquivos_com_erro.append(arquivo)
            move_file_error(extracted_file_path, arquivo)

    logger.info("Arquivos de CNAE finalizados.")

    # MOTI
    for arquivo in arquivos_moti:
        extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
        logger.info(f"Trabalhando (MOTI) no arquivo: {arquivo}")
        try:
            process_tabela_simples(conn, extracted_file_path, "moti")
        except Exception as e:
            logger.error(f"Erro ao processar MOTI {arquivo}: {e}", exc_info=True)
            arquivos_com_erro.append(arquivo)
            move_file_error(extracted_file_path, arquivo)

    logger.info("Arquivos de MOTI finalizados.")

    # MUNIC
    for arquivo in arquivos_munic:
        extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
        logger.info(f"Trabalhando (MUNIC) no arquivo: {arquivo}")
        try:
            process_tabela_simples(conn, extracted_file_path, "munic")
        except Exception as e:
            logger.error(f"Erro ao processar MUNIC {arquivo}: {e}", exc_info=True)
            arquivos_com_erro.append(arquivo)
            move_file_error(extracted_file_path, arquivo)

    logger.info("Arquivos de MUNIC finalizados.")

    # NATJU
    for arquivo in arquivos_natju:
        extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
        logger.info(f"Trabalhando (NATJU) no arquivo: {arquivo}")
        try:
            process_tabela_simples(conn, extracted_file_path, "natju")
        except Exception as e:
            logger.error(f"Erro ao processar NATJU {arquivo}: {e}", exc_info=True)
            arquivos_com_erro.append(arquivo)
            move_file_error(extracted_file_path, arquivo)

    logger.info("Arquivos de NATJU finalizados.")

    # PAIS
    for arquivo in arquivos_pais:
        extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
        logger.info(f"Trabalhando (PAIS) no arquivo: {arquivo}")
        try:
            process_tabela_simples(conn, extracted_file_path, "pais")
        except Exception as e:
            logger.error(f"Erro ao processar PAIS {arquivo}: {e}", exc_info=True)
            arquivos_com_erro.append(arquivo)
            move_file_error(extracted_file_path, arquivo)

    logger.info("Arquivos de PAIS finalizados.")

    # QUALS
    for arquivo in arquivos_quals:
        extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
        logger.info(f"Trabalhando (QUALS) no arquivo: {arquivo}")
        try:
            process_tabela_simples(conn, extracted_file_path, "quals")
        except Exception as e:
            logger.error(f"Erro ao processar QUALS {arquivo}: {e}", exc_info=True)
            arquivos_com_erro.append(arquivo)
            move_file_error(extracted_file_path, arquivo)

    logger.info("Arquivos de QUALS finalizados.")

    if arquivos_com_erro:
        logger.warning(f"Arquivos com erro: {arquivos_com_erro}")
        with open("arquivos_com_erro.txt", "w", encoding="utf-8") as f:
            for nome in arquivos_com_erro:
                f.write(nome + "\n")


# =============================================================================
# ETL PRINCIPAL
# =============================================================================

def etl_process():
    try:
        OUTPUT_FILES_PATH.mkdir(parents=True, exist_ok=True)
        EXTRACTED_FILES_PATH.mkdir(parents=True, exist_ok=True)
        ERRO_FILES_PATH.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Diretórios:\n"
            f" - Download: {OUTPUT_FILES_PATH}\n"
            f" - Extraídos: {EXTRACTED_FILES_PATH}\n"
            f" - Erro: {ERRO_FILES_PATH}"
        )

        start_time = datetime.now()

        info = verificar_nova_atualizacao()
        if not info:
            logger.info("Nenhuma nova atualização disponível. Encerrando ETL.")
            return

        files = get_files(info["url"])
        logger.info(f"Arquivos encontrados ({len(files)}): {files}")

        zip_files = [
            download_file(info["url"] + file, OUTPUT_FILES_PATH) for file in files
        ]

        for zip_file in zip_files:
            extract_files(zip_file, EXTRACTED_FILES_PATH)

        logger.info(
            "Todos os arquivos foram baixados e extraídos. Iniciando carga via COPY."
        )

        # Garante estrutura de tabelas
        create_tables()

        # Conexão principal para carga
        conn, engine = connect_db()

        try:
            # Processa tudo via COPY
            processar_arquivos(conn)

            # Registra info_dados
            inserir_info_dados(conn, info)

            logger.info("Carga dos arquivos finalizada com sucesso.")

        finally:
            conn.close()
            engine.dispose()

        # Cria índices (autocommit)
        criar_indices()

        # Limpa arquivos locais
        shutil.rmtree(OUTPUT_FILES_PATH, ignore_errors=True)
        shutil.rmtree(EXTRACTED_FILES_PATH, ignore_errors=True)
        logger.info("Arquivos locais removidos após a carga.")

        logger.info(
            f"ETL concluído com sucesso em "
            f"{converter_segundos(start_time, datetime.now())}"
        )

    except Exception as e:
        logger.error(f"Erro no processo ETL: {e}", exc_info=True)
        logger.critical("Não foi possível concluir o ETL.")
        sys.exit(1)


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Receita Federal do Brasil - Dados Abertos CNPJ")
    parser.add_argument(
        "--etl",
        action="store_true",
        help="Executa o processo completo de ETL (download, extração, carga via COPY e índices).",
    )
    parser.add_argument(
        "--inserir_indice",
        action="store_true",
        help="Cria índices nas tabelas do banco (sem executar o ETL).",
    )

    args = parser.parse_args()

    if args.etl:
        etl_process()

    if args.inserir_indice:
        criar_indices()
