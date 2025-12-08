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
import psycopg2
from dotenv import load_dotenv, find_dotenv
from bs4 import BeautifulSoup
from datetime import datetime

# Diretórios fixos para armazenar os arquivos
BASE_DIR = pathlib.Path().resolve()
OUTPUT_FILES_PATH = BASE_DIR / "files_downloaded"
EXTRACTED_FILES_PATH = BASE_DIR / "files_extracted"
ERRO_FILES_PATH = BASE_DIR / "files_error"

# Garantir diretório de logs
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

logger.info("\n ================================================================================")
logger.info("Iniciando ETL - dados_rfb (versão COPY / TEMP TABLE)")

# Caminho do .env (fora do projeto, para uso com Docker)
ENV_PATH_PARENT = BASE_DIR.parent / "dados_rfb_env" / ".env"
ENV_PATH_LOCAL = BASE_DIR / ".env"

if os.path.exists(ENV_PATH_PARENT):
    dotenv_path = find_dotenv(str(ENV_PATH_PARENT))
else:
    dotenv_path = find_dotenv(str(ENV_PATH_LOCAL))

if not dotenv_path:
    logger.error("Arquivo de configuração .env não encontrado.")
    logger.info("ETL finalizado.")
    sys.exit(1)

load_dotenv(dotenv_path)


def converter_segundos(tempo_inicial: datetime, tempo_final: datetime) -> str:
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
    elif segundos == 0 and not partes:
        partes.append(f"{diferenca} segundos")

    return ", ".join(partes)


def connect_db(autocommit: bool = False):
    from urllib.parse import quote_plus

    user = os.getenv("DB_USER")
    passw = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT") or "5432"
    database = os.getenv("DB_NAME")

    if passw is None or passw.strip() == "":
        logger.warning("⚠️ DB_PASSWORD em branco ou não configurado no .env")

    if not all([user, host, database]):
        missing = []
        if not user:
            missing.append("DB_USER")
        if not host:
            missing.append("DB_HOST")
        if not database:
            missing.append("DB_NAME")
        logger.error(f"Variáveis de ambiente faltando: {', '.join(missing)}")
        raise ValueError(f"Configuração do banco incompleta: {', '.join(missing)}")

    user_escaped = quote_plus(user)
    passw_escaped = quote_plus(passw) if passw else ""

    conn_str_safe = f"postgresql://{user_escaped}:{passw_escaped}@{host}:{port}/{database}"

    try:
        conn = psycopg2.connect(conn_str_safe)
        if autocommit:
            conn.set_session(autocommit=True)
        logger.info("✅ Conectado ao banco.")
        return conn
    except Exception as e:
        logger.error(f"❌ Erro ao conectar no banco: {e}")
        raise


def criar_tabela_info_dados():
    conn = connect_db()
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
        logger.info("Tabela info_dados verificada/criada com sucesso.")
    finally:
        conn.close()


def verificar_nova_atualizacao():
    base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    resp = requests.get(base_url)
    if resp.status_code != 200:
        logger.error(f"Erro ao acessar {base_url}: {resp.status_code}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    datas = []

    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        a_tag = tds[1].find("a")
        data_txt = tds[2].get_text(strip=True)
        if not a_tag or not data_txt:
            continue
        href = a_tag.get("href")
        if not href or not href.endswith("/"):
            continue
        pasta = href.rstrip("/")
        if len(pasta) != 7:  # "YYYY-MM"
            continue
        try:
            dt = datetime.strptime(data_txt, "%Y-%m-%d %H:%M")
            datas.append((pasta, dt))
        except Exception:
            continue

    if not datas:
        logger.info("Nenhuma pasta válida encontrada no site da RFB.")
        return None

    pasta_recente, dt_recente = max(datas, key=lambda x: x[1])
    ano, mes = map(int, pasta_recente.split("-"))

    criar_tabela_info_dados()
    conn = connect_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
            SELECT 1
            FROM info_dados
            WHERE ano = %s AND mes = %s AND data_atualizacao = %s
            LIMIT 1;
            """,
                (ano, mes, dt_recente),
            )
            row = cur.fetchone()
        if row:
            logger.info(
                f"Versão mais recente ({pasta_recente}, {dt_recente}) já registrada em info_dados."
            )
            return None
    finally:
        conn.close()

    return {
        "ano": ano,
        "mes": mes,
        "data_atualizacao": dt_recente,
        "url": f"{base_url}{pasta_recente}/",
    }


def get_files(base_url: str):
    resp = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
    if resp.status_code != 200:
        logger.error(f"Erro ao acessar {base_url}: {resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    files = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".zip")]
    if not files:
        logger.info("Nenhum arquivo .zip encontrado no diretório remoto.")
        return []
    files_sorted = sorted(files)
    logger.info(f"Arquivos remotos encontrados ({len(files_sorted)}): {files_sorted}")
    return files_sorted


def check_diff(url: str, file_path: str) -> bool:
    if not os.path.isfile(file_path):
        return True
    try:
        resp = requests.head(url, timeout=10)
        if resp.status_code != 200:
            logger.warning(f"HEAD {url} retornou {resp.status_code}, forçando download.")
            return True
        new_size = int(resp.headers.get("content-length", 0))
    except Exception as e:
        logger.warning(f"Erro ao obter cabeçalho de {url}: {e}")
        return True

    old_size = os.path.getsize(file_path)
    if new_size != old_size:
        logger.info(
            f"Tamanho diferente para {file_path} (local={old_size}, remoto={new_size}), baixando novamente."
        )
        os.remove(file_path)
        return True

    logger.info(f"Arquivo local {file_path} já está atualizado.")
    return False


def download_file(url: str, output_dir: pathlib.Path) -> pathlib.Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    file_name = url.split("/")[-1]
    dest = output_dir / file_name

    if not check_diff(url, str(dest)):
        return dest

    logger.info(f"Baixando {url} -> {dest}")
    with requests.get(url, stream=True) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    logger.info(f"Download concluído: {dest}")
    return dest


def extract_files(zip_path: pathlib.Path, extract_to: pathlib.Path):
    extract_to.mkdir(parents=True, exist_ok=True)
    logger.info(f"Extraindo {zip_path} para {extract_to}")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_to)


def move_file_error(extracted_file_path: pathlib.Path, arquivo: str):
    try:
        ERRO_FILES_PATH.mkdir(parents=True, exist_ok=True)
        destino = ERRO_FILES_PATH / arquivo
        shutil.move(str(extracted_file_path), destino)
        logger.info(f"Arquivo {arquivo} movido para pasta de erro: {destino}")
    except Exception as e:
        logger.error(f"Erro ao mover arquivo {arquivo} para pasta erro: {e}")


def create_base_tables(conn):
    logger.info("Criando/recriando tabelas base (DROP + CREATE), EXCETO info_dados...")

    with conn.cursor() as cur:
        # NÃO remover info_dados
        # Mantém histórico de atualizações

        # DROP das tabelas que serão recarregadas
        cur.execute("DROP TABLE IF EXISTS empresa CASCADE;")
        cur.execute("DROP TABLE IF EXISTS estabelecimento CASCADE;")
        cur.execute("DROP TABLE IF EXISTS socios CASCADE;")
        cur.execute("DROP TABLE IF EXISTS simples CASCADE;")
        cur.execute("DROP TABLE IF EXISTS cnae CASCADE;")
        cur.execute("DROP TABLE IF EXISTS moti CASCADE;")
        cur.execute("DROP TABLE IF EXISTS munic CASCADE;")
        cur.execute("DROP TABLE IF EXISTS natju CASCADE;")
        cur.execute("DROP TABLE IF EXISTS pais CASCADE;")
        cur.execute("DROP TABLE IF EXISTS quals CASCADE;")

        cur.execute("DROP TABLE IF EXISTS empresa_porte CASCADE;")
        cur.execute("DROP TABLE IF EXISTS estabelecimento_situacao_cadastral CASCADE;")
        cur.execute("DROP TABLE IF EXISTS socios_identificador CASCADE;")

        # Agora recria apenas as tabelas que são recarregadas via ETL
        cur.execute("""
        CREATE TABLE empresa (
            cnpj_basico TEXT PRIMARY KEY,
            razao_social TEXT,
            natureza_juridica TEXT,
            qualificacao_responsavel TEXT,
            capital_social DOUBLE PRECISION,
            porte_empresa TEXT,
            ente_federativo_responsavel TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE empresa_porte (
            codigo INTEGER PRIMARY KEY,
            descricao TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE estabelecimento (
            cnpj_basico TEXT,
            cnpj_ordem TEXT,
            cnpj_dv TEXT,
            identificador_matriz_filial TEXT,
            nome_fantasia TEXT,
            situacao_cadastral TEXT,
            data_situacao_cadastral TEXT,
            motivo_situacao_cadastral TEXT,
            nome_cidade_exterior TEXT,
            pais TEXT,
            data_inicio_atividade TEXT,
            cnae_fiscal_principal TEXT,
            cnae_fiscal_secundaria TEXT,
            tipo_logradouro TEXT,
            logradouro TEXT,
            numero TEXT,
            complemento TEXT,
            bairro TEXT,
            cep TEXT,
            uf TEXT,
            municipio TEXT,
            ddd_1 TEXT,
            telefone_1 TEXT,
            ddd_2 TEXT,
            telefone_2 TEXT,
            ddd_fax TEXT,
            fax TEXT,
            correio_eletronico TEXT,
            situacao_especial TEXT,
            data_situacao_especial TEXT,
            PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
        );
        """)

        cur.execute("""
        CREATE TABLE estabelecimento_situacao_cadastral (
            codigo INTEGER PRIMARY KEY,
            descricao TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE moti (
            codigo TEXT PRIMARY KEY,
            descricao TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE munic (
            codigo TEXT PRIMARY KEY,
            descricao TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE natju (
            codigo TEXT PRIMARY KEY,
            descricao TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE pais (
            codigo TEXT PRIMARY KEY,
            descricao TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE quals (
            codigo TEXT PRIMARY KEY,
            descricao TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE simples (
            cnpj_basico TEXT PRIMARY KEY,
            opcao_pelo_simples TEXT,
            data_opcao_simples TEXT,
            data_exclusao_simples TEXT,
            opcao_mei TEXT,
            data_opcao_mei TEXT,
            data_exclusao_mei TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE socios (
            cnpj_basico TEXT,
            identificador_socio TEXT,
            nome_socio_razao_social TEXT,
            cpf_cnpj_socio TEXT,
            qualificacao_socio TEXT,
            data_entrada_sociedade TEXT,
            pais TEXT,
            representante_legal TEXT,
            nome_do_representante TEXT,
            qualificacao_representante_legal TEXT,
            faixa_etaria TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE socios_identificador (
            codigo INTEGER PRIMARY KEY,
            descricao TEXT
        );
        """)

        # INSERÇÕES FIXAS
        cur.execute("""
        INSERT INTO empresa_porte (codigo, descricao) VALUES
            (1, 'Microempresa'),
            (3, 'Empresa de Pequeno Porte'),
            (5, 'Demais');
        """)

        cur.execute("""
        INSERT INTO estabelecimento_situacao_cadastral (codigo, descricao) VALUES
            (1, 'Nula'),
            (2, 'Ativa'),
            (3, 'Suspensa'),
            (4, 'Inapta'),
            (5, 'Ativa Não Regular'),
            (8, 'Baixada');
        """)

        cur.execute("""
        INSERT INTO socios_identificador (codigo, descricao) VALUES
            (1, 'Pessoa Jurídica'),
            (2, 'Pessoa Física'),
            (3, 'Sócio Estrangeiro');
        """)

    conn.commit()
    logger.info("Tabelas recriadas com sucesso (info_dados preservado).")


def load_empresa(conn, arquivos_empresa, arquivos_com_erro):
    if not arquivos_empresa:
        logger.info("Nenhum arquivo EMPRE encontrado para carga.")
        return

    logger.info("==== Iniciando carga EMPRESA (COPY + transformação capital_social) ====")
    with conn.cursor() as cur:
        cur.execute(
            """
        CREATE TEMP TABLE empresa_tmp (
            cnpj_basico TEXT,
            razao_social TEXT,
            natureza_juridica TEXT,
            qualificacao_responsavel TEXT,
            capital_social_raw TEXT,
            porte_empresa TEXT,
            ente_federativo_responsavel TEXT
        );
        """
        )
        conn.commit()

        copy_sql = """
            COPY empresa_tmp
            FROM STDIN
            WITH (FORMAT csv, DELIMITER ';', QUOTE '"', ESCAPE '"', NULL '');
        """

        for arquivo in arquivos_empresa:
            caminho = EXTRACTED_FILES_PATH / arquivo
            if not caminho.exists():
                logger.warning(f"Arquivo EMPRE não encontrado: {caminho}")
                continue

            logger.info(f"EMPRESA - COPY para empresa_tmp: {arquivo}")
            try:
                cur.execute("TRUNCATE TABLE empresa_tmp;")
                with open(caminho, "r", encoding="latin-1", newline="") as f:
                    cur.copy_expert(copy_sql, f)

                cur.execute(
                    """
                INSERT INTO empresa (
                    cnpj_basico,
                    razao_social,
                    natureza_juridica,
                    qualificacao_responsavel,
                    capital_social,
                    porte_empresa,
                    ente_federativo_responsavel
                )
                SELECT
                    cnpj_basico,
                    razao_social,
                    natureza_juridica,
                    qualificacao_responsavel,
                    CASE
                        WHEN capital_social_raw IS NULL OR capital_social_raw = '' THEN 0.0
                        ELSE REPLACE(capital_social_raw, ',', '.')::DOUBLE PRECISION
                    END AS capital_social,
                    porte_empresa,
                    ente_federativo_responsavel
                FROM empresa_tmp;
                """
                )

                conn.commit()
                logger.info(f"EMPRESA - arquivo {arquivo} carregado com sucesso.")
            except Exception as e:
                conn.rollback()
                logger.error(f"Erro ao processar arquivo EMPRE {arquivo}: {e}", exc_info=True)
                arquivos_com_erro.append(arquivo)
                move_file_error(caminho, arquivo)

        try:
            cur.execute("DROP TABLE IF EXISTS empresa_tmp;")
            conn.commit()
        except Exception:
            conn.rollback()
    logger.info("==== Carga EMPRESA finalizada ====")


def load_generic_table(conn, table_name, temp_table_name, columns, arquivos, arquivos_com_erro, log_label):
    if not arquivos:
        logger.info(f"Nenhum arquivo {log_label} encontrado para carga.")
        return

    logger.info(f"==== Iniciando carga {log_label} (COPY) ====")
    cols_def = ", ".join(f"{c} TEXT" for c in columns)
    col_list = ", ".join(columns)

    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {temp_table_name} ({cols_def});")
        conn.commit()

        copy_sql = f"""
            COPY {temp_table_name}
            FROM STDIN
            WITH (FORMAT csv, DELIMITER ';', QUOTE '"', ESCAPE '"', NULL '');
        """

        for arquivo in arquivos:
            caminho = EXTRACTED_FILES_PATH / arquivo
            if not caminho.exists():
                logger.warning(f"Arquivo {log_label} não encontrado: {caminho}")
                continue

            logger.info(f"{log_label} - COPY para {temp_table_name}: {arquivo}")
            try:
                cur.execute(f"TRUNCATE TABLE {temp_table_name};")
                with open(caminho, "r", encoding="latin-1", newline="") as f:
                    cur.copy_expert(copy_sql, f)

                cur.execute(
                    f"""
                INSERT INTO {table_name} ({col_list})
                SELECT {col_list}
                FROM {temp_table_name};
                """
                )
                conn.commit()
                logger.info(f"{log_label} - arquivo {arquivo} carregado com sucesso.")
            except Exception as e:
                conn.rollback()
                logger.error(
                    f"Erro ao processar arquivo {log_label} {arquivo}: {e}",
                    exc_info=True,
                )
                arquivos_com_erro.append(arquivo)
                move_file_error(caminho, arquivo)

        try:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table_name};")
            conn.commit()
        except Exception:
            conn.rollback()

    logger.info(f"==== Carga {log_label} finalizada ====")


def inserir_info_dados(conn, info):
    if not info:
        return
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
    logger.info(
        f"Registro de atualização gravado em info_dados: {info['ano']}-{info['mes']:02d}"
    )


def criar_indices():
    try:
        conn = connect_db(autocommit=True)
        with conn.cursor() as cur:
            logger.info("Criando índices (CONCURRENTLY) nas tabelas principais...")

            indices = [
                ("empresa", "cnpj_basico"),
                ("estabelecimento", "cnpj_basico"),
                ("socios", "cnpj_basico"),
                ("simples", "cnpj_basico"),
            ]

            for tabela, coluna in indices:
                nome = f"{tabela}_{coluna}_idx"
                try:
                    cur.execute(
                        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {nome} ON {tabela} ({coluna});"
                    )
                    logger.info(f"Índice {nome} criado/verificado com sucesso.")
                except Exception as e:
                    logger.error(
                        f"Erro ao criar índice {nome} em {tabela}({coluna}): {e}",
                        exc_info=True,
                    )

        conn.close()
        logger.info("Criação de índices finalizada.")
    except Exception as e:
        logger.error(f"Erro geral ao criar índices: {e}", exc_info=True)


def etl_process():
    try:
        OUTPUT_FILES_PATH.mkdir(parents=True, exist_ok=True)
        EXTRACTED_FILES_PATH.mkdir(parents=True, exist_ok=True)
        ERRO_FILES_PATH.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Diretórios usados:\n"
            f" - Download: {OUTPUT_FILES_PATH}\n"
            f" - Extração: {EXTRACTED_FILES_PATH}\n"
            f" - Erros: {ERRO_FILES_PATH}"
        )

        inicio = datetime.now()

        info = verificar_nova_atualizacao()
        if not info:
            logger.info("Nenhuma nova atualização a processar. Encerrando ETL.")
            return

        files = get_files(info["url"])
        if not files:
            logger.error("Nenhum arquivo .zip encontrado para o mês selecionado.")
            return

        zip_paths = []
        for fname in files:
            url = info["url"] + fname
            zip_path = download_file(url, OUTPUT_FILES_PATH)
            zip_paths.append(zip_path)

        for zp in zip_paths:
            extract_files(zp, EXTRACTED_FILES_PATH)

        logger.info("Todos os arquivos foram baixados e extraídos. Preparando carga via COPY...")

        arquivos_empresa = []
        arquivos_estab = []
        arquivos_socios = []
        arquivos_simples = []
        arquivos_cnae = []
        arquivos_moti = []
        arquivos_munic = []
        arquivos_natju = []
        arquivos_pais = []
        arquivos_quals = []

        for fname in os.listdir(EXTRACTED_FILES_PATH):
            if "EMPRE" in fname:
                arquivos_empresa.append(fname)
            elif "ESTABELE" in fname:
                arquivos_estab.append(fname)
            elif "SOCIO" in fname:
                arquivos_socios.append(fname)
            elif "SIMPLES" in fname:
                arquivos_simples.append(fname)
            elif "CNAE" in fname:
                arquivos_cnae.append(fname)
            elif "MOTI" in fname:
                arquivos_moti.append(fname)
            elif "MUNIC" in fname:
                arquivos_munic.append(fname)
            elif "NATJU" in fname:
                arquivos_natju.append(fname)
            elif "PAIS" in fname:
                arquivos_pais.append(fname)
            elif "QUALS" in fname:
                arquivos_quals.append(fname)

        arquivos_empresa.sort()
        arquivos_estab.sort()
        arquivos_socios.sort()
        arquivos_simples.sort()
        arquivos_cnae.sort()
        arquivos_moti.sort()
        arquivos_munic.sort()
        arquivos_natju.sort()
        arquivos_pais.sort()
        arquivos_quals.sort()

        logger.info(
            "Resumo arquivos extraídos:\n"
            f"  EMPRE: {len(arquivos_empresa)}\n"
            f"  ESTABELE: {len(arquivos_estab)}\n"
            f"  SOCIO: {len(arquivos_socios)}\n"
            f"  SIMPLES: {len(arquivos_simples)}\n"
            f"  CNAE: {len(arquivos_cnae)}\n"
            f"  MOTI: {len(arquivos_moti)}\n"
            f"  MUNIC: {len(arquivos_munic)}\n"
            f"  NATJU: {len(arquivos_natju)}\n"
            f"  PAIS: {len(arquivos_pais)}\n"
            f"  QUALS: {len(arquivos_quals)}"
        )

        arquivos_com_erro = []

        conn = connect_db()
        try:
            create_base_tables(conn)

            # EMPRESA (com tratamento de capital_social)
            load_empresa(conn, arquivos_empresa, arquivos_com_erro)

            # ESTABELECIMENTO
            cols_estab = [
                "cnpj_basico",
                "cnpj_ordem",
                "cnpj_dv",
                "identificador_matriz_filial",
                "nome_fantasia",
                "situacao_cadastral",
                "data_situacao_cadastral",
                "motivo_situacao_cadastral",
                "nome_cidade_exterior",
                "pais",
                "data_inicio_atividade",
                "cnae_fiscal_principal",
                "cnae_fiscal_secundaria",
                "tipo_logradouro",
                "logradouro",
                "numero",
                "complemento",
                "bairro",
                "cep",
                "uf",
                "municipio",
                "ddd_1",
                "telefone_1",
                "ddd_2",
                "telefone_2",
                "ddd_fax",
                "fax",
                "correio_eletronico",
                "situacao_especial",
                "data_situacao_especial",
            ]
            load_generic_table(
                conn,
                "estabelecimento",
                "estabelecimento_tmp",
                cols_estab,
                arquivos_estab,
                arquivos_com_erro,
                "ESTABELECIMENTO",
            )

            # SOCIOS
            cols_socios = [
                "cnpj_basico",
                "identificador_socio",
                "nome_socio_razao_social",
                "cpf_cnpj_socio",
                "qualificacao_socio",
                "data_entrada_sociedade",
                "pais",
                "representante_legal",
                "nome_do_representante",
                "qualificacao_representante_legal",
                "faixa_etaria",
            ]
            load_generic_table(
                conn,
                "socios",
                "socios_tmp",
                cols_socios,
                arquivos_socios,
                arquivos_com_erro,
                "SOCIOS",
            )

            # SIMPLES
            cols_simples = [
                "cnpj_basico",
                "opcao_pelo_simples",
                "data_opcao_simples",
                "data_exclusao_simples",
                "opcao_mei",
                "data_opcao_mei",
                "data_exclusao_mei",
            ]
            load_generic_table(
                conn,
                "simples",
                "simples_tmp",
                cols_simples,
                arquivos_simples,
                arquivos_com_erro,
                "SIMPLES",
            )

            # CNAE
            cols_cnae = ["codigo", "descricao"]
            load_generic_table(
                conn,
                "cnae",
                "cnae_tmp",
                cols_cnae,
                arquivos_cnae,
                arquivos_com_erro,
                "CNAE",
            )

            # MOTI
            cols_moti = ["codigo", "descricao"]
            load_generic_table(
                conn,
                "moti",
                "moti_tmp",
                cols_moti,
                arquivos_moti,
                arquivos_com_erro,
                "MOTI",
            )

            # MUNIC
            cols_munic = ["codigo", "descricao"]
            load_generic_table(
                conn,
                "munic",
                "munic_tmp",
                cols_munic,
                arquivos_munic,
                arquivos_com_erro,
                "MUNIC",
            )

            # NATJU
            cols_natju = ["codigo", "descricao"]
            load_generic_table(
                conn,
                "natju",
                "natju_tmp",
                cols_natju,
                arquivos_natju,
                arquivos_com_erro,
                "NATJU",
            )

            # PAIS
            cols_pais = ["codigo", "descricao"]
            load_generic_table(
                conn,
                "pais",
                "pais_tmp",
                cols_pais,
                arquivos_pais,
                arquivos_com_erro,
                "PAIS",
            )

            # QUALS
            cols_quals = ["codigo", "descricao"]
            load_generic_table(
                conn,
                "quals",
                "quals_tmp",
                cols_quals,
                arquivos_quals,
                arquivos_com_erro,
                "QUALS",
            )

            inserir_info_dados(conn, info)

        finally:
            conn.close()

        if arquivos_com_erro:
            logger.warning(f"Arquivos com erro durante o ETL: {arquivos_com_erro}")
            erro_list_path = BASE_DIR / "arquivos_com_erro.txt"
            with open(erro_list_path, "w", encoding="utf-8") as f:
                for nome in arquivos_com_erro:
                    f.write(nome + "\n")
            logger.info(f"Lista de arquivos com erro salva em: {erro_list_path}")

        try:
            shutil.rmtree(OUTPUT_FILES_PATH)
            shutil.rmtree(EXTRACTED_FILES_PATH)
            logger.info("Pastas de download e extração removidas com sucesso.")
        except Exception as e:
            logger.warning(f"Não foi possível remover pastas temporárias: {e}")

        try:
            criar_indices()
        except Exception:
            logger.error("Falha ao criar índices pós-carga.", exc_info=True)

        fim = datetime.now()
        logger.info(
            f"ETL concluído com sucesso em {converter_segundos(inicio, fim)}."
        )
    except Exception as e:
        logger.error(f"Erro geral no processo ETL: {e}", exc_info=True)
        logger.critical("Não foi possível concluir o ETL.")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="ETL Dados Abertos CNPJ - Receita Federal do Brasil (COPY)."
    )
    parser.add_argument(
        "--etl",
        action="store_true",
        help="Executa o processo completo de ETL (download, extração, carga e índices).",
    )
    parser.add_argument(
        "--inserir_indice",
        action="store_true",
        help="Cria índices nas tabelas principais (sem recarregar dados).",
    )

    args = parser.parse_args()

    if args.etl:
        etl_process()
    elif args.inserir_indice:
        criar_indices()
    else:
        print(
            "Use --etl para rodar o ETL completo ou --inserir_indice para apenas criar índices."
        )


if __name__ == "__main__":
    main()
