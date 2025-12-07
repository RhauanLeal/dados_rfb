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
from psycopg2 import sql
from dotenv import load_dotenv, find_dotenv
from bs4 import BeautifulSoup
from datetime import datetime

# =========================
# CONFIGURAÇÃO DE LOG
# =========================
BASE_DIR = pathlib.Path().resolve()
LOG_DIR = BASE_DIR / "logs"
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

# Diretórios fixos para armazenar os arquivos
OUTPUT_FILES_PATH = BASE_DIR / "files_downloaded"
EXTRACTED_FILES_PATH = BASE_DIR / "files_extracted"
ERRO_FILES_PATH = BASE_DIR / "files_error"

logger.info("Iniciando ETL - dados_rfb (COPY)")

# =========================
# CARREGAR .env
# =========================
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


# =========================
# FUNÇÕES UTILITÁRIAS
# =========================
def converter_segundos(tempo_inicial: datetime, tempo_final: datetime) -> str:
    """
    Converte diferença entre dois datetimes em texto "X horas, Y minutos, Z segundos"
    """
    diferenca = tempo_final - tempo_inicial
    total_segundos = int(diferenca.total_seconds())

    horas = total_segundos // 3600
    minutos = (total_segundos % 3600) // 60
    segundos = total_segundos % 60

    componentes = []

    if horas == 1:
        componentes.append("1 hora")
    elif horas > 1:
        componentes.append(f"{horas} horas")

    if minutos == 1:
        componentes.append("1 minuto")
    elif minutos > 1:
        componentes.append(f"{minutos} minutos")

    if segundos == 1:
        componentes.append("1 segundo")
    elif segundos > 1:
        componentes.append(f"{segundos} segundos")
    elif segundos < 1:
        componentes.append(f"{diferenca} segundos")

    return ", ".join(componentes)


def connect_db(autocommit: bool = False):
    """
    Cria conexão com o PostgreSQL usando variáveis do .env
    """
    from urllib.parse import quote_plus

    user = os.getenv("DB_USER")
    passw = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT") or "5432"
    database = os.getenv("DB_NAME")

    if passw is None or passw.strip() == "":
        logger.warning("⚠️ DB_PASSWORD está vazio no .env")

    if not all([user, host, database]):
        missing = []
        if not user:
            missing.append("DB_USER")
        if not host:
            missing.append("DB_HOST")
        if not database:
            missing.append("DB_NAME")
        logger.error(f"Variáveis faltando no .env: {', '.join(missing)}")
        raise ValueError(f"Configuração do banco incompleta: {', '.join(missing)}")

    user_escaped = quote_plus(user)
    passw_escaped = quote_plus(passw) if passw else ""

    dsn = f"dbname={database} user={user} password={passw} host={host} port={port}"
    uri = f"postgresql://{user_escaped}:{passw_escaped}@{host}:{port}/{database}"

    try:
        conn = psycopg2.connect(dsn)
        if autocommit:
            conn.set_session(autocommit=True)
        logger.info(f"✅ Conectado ao banco: {uri}")
        return conn
    except Exception as e:
        logger.error(f"❌ Erro na conexão com o banco: {e}")
        raise


# =========================
# INFO_DADOS (controle de versão)
# =========================
def criar_tabela_info_dados():
    """
    Cria tabela info_dados para controlar a versão mais recente dos dados RFB.
    """
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
                        CREATE INDEX idx_info_dados_data_atualizacao ON info_dados (data_atualizacao);
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
        logger.info("Tabela info_dados criada/verificada com sucesso.")
    finally:
        conn.close()


def inserir_info_dados(info: dict):
    """
    Insere ou atualiza info_dados com a versão mais recente.
    """
    conn = connect_db()
    try:
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
            f"Atualização registrada em info_dados: {info['ano']}-{info['mes']:02d}"
        )
    finally:
        conn.close()


def verificar_nova_atualizacao():
    """
    Verifica no site da RFB qual é a pasta ano-mês mais recente.
    Compara com info_dados; se já tiver, retorna None.
    Caso contrário, retorna dict com ano, mes, data_atualizacao, url.
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
                        data_atualizacao = datetime.strptime(data_txt, "%Y-%m-%d %H:%M")
                        datas.append((ano_mes, data_atualizacao))
                    except Exception:
                        continue

    if not datas:
        logger.info("Nenhuma pasta válida encontrada no site da RFB.")
        return None

    # Pasta mais recente
    ano_mes, data_atualizacao = max(datas, key=lambda x: x[1])
    ano, mes = map(int, ano_mes.split("-"))

    # Garante info_dados
    criar_tabela_info_dados()

    # Verifica se já existe essa versão
    conn = connect_db()
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
            logger.info(
                f"A versão {ano}-{mes:02d} já está registrada em info_dados. Nada a fazer."
            )
            return None
    finally:
        conn.close()

    return {
        "ano": ano,
        "mes": mes,
        "data_atualizacao": data_atualizacao,
        "url": f"{base_url}{ano}-{mes:02d}/",
    }


# =========================
# CRIAÇÃO DAS TABELAS
# =========================
def create_tables():
    """
    Cria todas as tabelas necessárias do layout oficial
    (sempre garantindo estrutura correta antes do carregamento).
    """
    conn = connect_db()
    try:
        with conn.cursor() as cur:
            # Drop + create para garantir esquema consistente
            cur.execute("DROP TABLE IF EXISTS cnae, empresa, empresa_porte, estabelecimento, estabelecimento_situacao_cadastral, moti, munic, natju, pais, quals, simples, socios, socios_identificador CASCADE;")

            # cnae
            cur.execute(
                """
                CREATE TABLE public.cnae (
                    codigo TEXT PRIMARY KEY,
                    descricao TEXT
                );
                """
            )

            # empresa (capital_social NUMERIC, pois vem com vírgula)
            cur.execute(
                """
                CREATE TABLE public.empresa (
                    cnpj_basico TEXT PRIMARY KEY,
                    razao_social TEXT,
                    natureza_juridica TEXT,
                    qualificacao_responsavel TEXT,
                    capital_social NUMERIC(18,2),
                    porte_empresa TEXT,
                    ente_federativo_responsavel TEXT
                );
                """
            )

            # empresa_porte (tabela estática)
            cur.execute(
                """
                CREATE TABLE empresa_porte (
                    codigo INTEGER PRIMARY KEY,
                    descricao TEXT
                );
                """
            )

            # estabelecimento
            cur.execute(
                """
                CREATE TABLE public.estabelecimento (
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
                """
            )

            # estabelecimento_situacao_cadastral (tabela estática)
            cur.execute(
                """
                CREATE TABLE estabelecimento_situacao_cadastral (
                    codigo INTEGER PRIMARY KEY,
                    descricao TEXT
                );
                """
            )

            # moti
            cur.execute(
                """
                CREATE TABLE public.moti (
                    codigo TEXT PRIMARY KEY,
                    descricao TEXT
                );
                """
            )

            # munic
            cur.execute(
                """
                CREATE TABLE public.munic (
                    codigo TEXT PRIMARY KEY,
                    descricao TEXT
                );
                """
            )

            # natju
            cur.execute(
                """
                CREATE TABLE public.natju (
                    codigo TEXT PRIMARY KEY,
                    descricao TEXT
                );
                """
            )

            # pais
            cur.execute(
                """
                CREATE TABLE public.pais (
                    codigo TEXT PRIMARY KEY,
                    descricao TEXT
                );
                """
            )

            # quals
            cur.execute(
                """
                CREATE TABLE public.quals (
                    codigo TEXT PRIMARY KEY,
                    descricao TEXT
                );
                """
            )

            # simples
            cur.execute(
                """
                CREATE TABLE public.simples (
                    cnpj_basico TEXT PRIMARY KEY,
                    opcao_pelo_simples TEXT,
                    data_opcao_simples TEXT,
                    data_exclusao_simples TEXT,
                    opcao_mei TEXT,
                    data_opcao_mei TEXT,
                    data_exclusao_mei TEXT
                );
                """
            )

            # socios
            cur.execute(
                """
                CREATE TABLE public.socios (
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
                """
            )

            # socios_identificador (estático)
            cur.execute(
                """
                CREATE TABLE socios_identificador (
                    codigo INTEGER PRIMARY KEY,
                    descricao TEXT
                );
                """
            )

            # Inserts estáticos
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
        logger.info("Tabelas criadas/recriadas com sucesso.")
    finally:
        conn.close()


# =========================
# DOWNLOAD E EXTRAÇÃO
# =========================
def get_files(base_url: str):
    resp = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
    if resp.status_code != 200:
        logger.error(f"Erro ao acessar {base_url}: {resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    files = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".zip")]
    files = sorted(files)
    logger.info(f"Arquivos .zip encontrados ({len(files)}): {files}")
    return files


def download_file(url: str, output_path: pathlib.Path) -> pathlib.Path:
    output_path.mkdir(parents=True, exist_ok=True)
    file_name = output_path / url.split("/")[-1]
    logger.info(f"Baixando: {file_name}")

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(file_name, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    logger.info(f"Download concluído: {file_name}")
    return file_name


def extract_files(zip_path: pathlib.Path, extract_to: pathlib.Path):
    logger.info(f"Extraindo: {zip_path}")
    extract_to.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_to)


def move_file_error(path: pathlib.Path):
    ERRO_FILES_PATH.mkdir(parents=True, exist_ok=True)
    destino = ERRO_FILES_PATH / path.name
    try:
        shutil.move(str(path), str(destino))
        logger.info(f"Arquivo movido para erro: {destino}")
    except Exception as e:
        logger.error(f"Erro ao mover arquivo para erro: {path} -> {e}")


# =========================
# CARREGAMENTO VIA COPY
# =========================
def copy_csv_to_table(conn, table_name: str, file_path: pathlib.Path, columns: list):
    """
    Faz COPY FROM STDIN para uma tabela qualquer (sem tratamento especial).
    """
    logger.info(f"Iniciando COPY para {table_name} a partir de {file_path.name}")

    copy_sql = sql.SQL(
        """
        COPY {table} ({cols})
        FROM STDIN
        WITH (
            FORMAT csv,
            DELIMITER ';',
            QUOTE '"',
            ESCAPE '"',
            NULL ''
        );
        """
    ).format(
        table=sql.Identifier(table_name),
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )

    with conn.cursor() as cur, open(file_path, "r", encoding="latin-1", newline="") as f:
        cur.copy_expert(copy_sql, f)
    conn.commit()
    logger.info(f"COPY concluído em {table_name} ({file_path.name})")


def load_empresa(conn, file_paths):
    """
    EMPRECSV — precisa tratar capital_social com vírgula.
    Estratégia:
      1. COPY para tabela temporária empresa_tmp (capital_social TEXT)
      2. INSERT...SELECT com REPLACE(capital_social, ',', '.')::NUMERIC
    """
    logger.info("==== Iniciando carga EMPRESA (COPY + transformação capital_social) ====")

    with conn.cursor() as cur:
        # Tabela temporária na sessão
        cur.execute(
            """
            CREATE TEMP TABLE empresa_tmp (
                cnpj_basico TEXT,
                razao_social TEXT,
                natureza_juridica TEXT,
                qualificacao_responsavel TEXT,
                capital_social TEXT,
                porte_empresa TEXT,
                ente_federativo_responsavel TEXT
            ) ON COMMIT DROP;
            """
        )
        conn.commit()

        for path in file_paths:
            logger.info(f"EMPRESA - COPY para empresa_tmp: {path.name}")
            copy_sql = """
                COPY empresa_tmp (
                    cnpj_basico,
                    razao_social,
                    natureza_juridica,
                    qualificacao_responsavel,
                    capital_social,
                    porte_empresa,
                    ente_federativo_responsavel
                )
                FROM STDIN
                WITH (
                    FORMAT csv,
                    DELIMITER ';',
                    QUOTE '"',
                    ESCAPE '"',
                    NULL ''
                );
            """
            with open(path, "r", encoding="latin-1", newline="") as f:
                cur.copy_expert(copy_sql, f)
            conn.commit()

        logger.info("EMPRESA - Inserindo da empresa_tmp para empresa com conversão de vírgula...")
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
                    WHEN capital_social IS NULL OR capital_social = '' THEN 0
                    ELSE REPLACE(capital_social, ',', '.')::NUMERIC(18,2)
                END AS capital_social,
                porte_empresa,
                ente_federativo_responsavel
            FROM empresa_tmp;
            """
        )
        conn.commit()

    logger.info("==== EMPRESA carregada com sucesso. ====")


def load_estabelecimento(conn, file_paths):
    cols = [
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
    logger.info("==== Iniciando carga ESTABELECIMENTO ====")
    for path in file_paths:
        copy_csv_to_table(conn, "estabelecimento", path, cols)
    logger.info("==== ESTABELECIMENTO carregado. ====")


def load_socios(conn, file_paths):
    cols = [
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
    logger.info("==== Iniciando carga SOCIOS ====")
    for path in file_paths:
        copy_csv_to_table(conn, "socios", path, cols)
    logger.info("==== SOCIOS carregado. ====")


def load_simples(conn, file_paths):
    cols = [
        "cnpj_basico",
        "opcao_pelo_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ]
    logger.info("==== Iniciando carga SIMPLES ====")
    for path in file_paths:
        copy_csv_to_table(conn, "simples", path, cols)
    logger.info("==== SIMPLES carregado. ====")


def load_cnae(conn, file_paths):
    cols = ["codigo", "descricao"]
    logger.info("==== Iniciando carga CNAE ====")
    for path in file_paths:
        copy_csv_to_table(conn, "cnae", path, cols)
    logger.info("==== CNAE carregado. ====")


def load_moti(conn, file_paths):
    cols = ["codigo", "descricao"]
    logger.info("==== Iniciando carga MOTI ====")
    for path in file_paths:
        copy_csv_to_table(conn, "moti", path, cols)
    logger.info("==== MOTI carregado. ====")


def load_munic(conn, file_paths):
    cols = ["codigo", "descricao"]
    logger.info("==== Iniciando carga MUNIC ====")
    for path in file_paths:
        copy_csv_to_table(conn, "munic", path, cols)
    logger.info("==== MUNIC carregado. ====")


def load_natju(conn, file_paths):
    cols = ["codigo", "descricao"]
    logger.info("==== Iniciando carga NATJU ====")
    for path in file_paths:
        copy_csv_to_table(conn, "natju", path, cols)
    logger.info("==== NATJU carregado. ====")


def load_pais(conn, file_paths):
    cols = ["codigo", "descricao"]
    logger.info("==== Iniciando carga PAIS ====")
    for path in file_paths:
        copy_csv_to_table(conn, "pais", path, cols)
    logger.info("==== PAIS carregado. ====")


def load_quals(conn, file_paths):
    cols = ["codigo", "descricao"]
    logger.info("==== Iniciando carga QUALS ====")
    for path in file_paths:
        copy_csv_to_table(conn, "quals", path, cols)
    logger.info("==== QUALS carregado. ====")


# =========================
# ÍNDICES
# =========================
def criar_indices():
    """
    Cria índices auxiliares para acelerar joins por cnpj_basico.
    Usa autocommit porque CREATE INDEX CONCURRENTLY não pode estar em transação.
    """
    conn = connect_db(autocommit=True)
    try:
        with conn.cursor() as cur:
            logger.info("Criando índices auxiliar(es)...")

            indices = [
                ("estabelecimento", "cnpj_basico"),
                ("socios", "cnpj_basico"),
                ("simples", "cnpj_basico"),
            ]

            for tabela, coluna in indices:
                nome_indice = f"{tabela}_{coluna}_idx"
                try:
                    cur.execute(
                        sql.SQL(
                            "CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx} ON {tbl} ({col});"
                        ).format(
                            idx=sql.Identifier(nome_indice),
                            tbl=sql.Identifier(tabela),
                            col=sql.Identifier(coluna),
                        )
                    )
                    logger.info(f"Índice criado/verificado: {nome_indice}")
                except Exception as e:
                    logger.error(
                        f"Erro ao criar índice {nome_indice} em {tabela}: {e}",
                        exc_info=True,
                    )
        logger.info("Criação de índices finalizada.")
    finally:
        conn.close()
        gc.collect()


# =========================
# ETL PRINCIPAL
# =========================
def etl_process():
    start_time = datetime.now()
    try:
        # Diretórios
        OUTPUT_FILES_PATH.mkdir(parents=True, exist_ok=True)
        EXTRACTED_FILES_PATH.mkdir(parents=True, exist_ok=True)
        ERRO_FILES_PATH.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Diretórios:\n - downloads: {OUTPUT_FILES_PATH}\n - extraídos: {EXTRACTED_FILES_PATH}\n - erro: {ERRO_FILES_PATH}"
        )

        info = verificar_nova_atualizacao()
        if not info:
            logger.info("Nenhuma atualização nova encontrada. Encerrando.")
            return

        # Lista de arquivos do mês
        files = get_files(info["url"])
        if not files:
            logger.info("Nenhum arquivo .zip para processar. Encerrando.")
            return

        # Download
        zip_files = []
        for f in files:
            url = info["url"] + f
            zip_files.append(download_file(url, OUTPUT_FILES_PATH))

        # Extração
        for zf in zip_files:
            extract_files(zf, EXTRACTED_FILES_PATH)

        logger.info("Todos os arquivos foram baixados e extraídos. Preparando carga via COPY...")

        # Cria/recria tabelas
        create_tables()

        # Agrupar arquivos por tipo
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

        for fname in os.listdir(EXTRACTED_FILES_PATH):
            path = EXTRACTED_FILES_PATH / fname
            if not path.is_file():
                continue

            if "EMPRE" in fname.upper():
                arquivos_empresa.append(path)
            elif "ESTABELE" in fname.upper():
                arquivos_estabelecimento.append(path)
            elif "SOCIO" in fname.upper():
                arquivos_socios.append(path)
            elif "SIMPLES" in fname.upper():
                arquivos_simples.append(path)
            elif "CNAE" in fname.upper():
                arquivos_cnae.append(path)
            elif "MOTI" in fname.upper():
                arquivos_moti.append(path)
            elif "MUNIC" in fname.upper():
                arquivos_munic.append(path)
            elif "NATJU" in fname.upper():
                arquivos_natju.append(path)
            elif "PAIS" in fname.upper():
                arquivos_pais.append(path)
            elif "QUALS" in fname.upper():
                arquivos_quals.append(path)

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

        conn = connect_db()

        try:
            if arquivos_empresa:
                load_empresa(conn, arquivos_empresa)
            if arquivos_estabelecimento:
                load_estabelecimento(conn, arquivos_estabelecimento)
            if arquivos_socios:
                load_socios(conn, arquivos_socios)
            if arquivos_simples:
                load_simples(conn, arquivos_simples)
            if arquivos_cnae:
                load_cnae(conn, arquivos_cnae)
            if arquivos_moti:
                load_moti(conn, arquivos_moti)
            if arquivos_munic:
                load_munic(conn, arquivos_munic)
            if arquivos_natju:
                load_natju(conn, arquivos_natju)
            if arquivos_pais:
                load_pais(conn, arquivos_pais)
            if arquivos_quals:
                load_quals(conn, arquivos_quals)
        finally:
            conn.close()

        # Registra versão em info_dados
        inserir_info_dados(info)

        # Cria índices auxiliares
        criar_indices()

        # Limpa arquivos
        shutil.rmtree(OUTPUT_FILES_PATH, ignore_errors=True)
        shutil.rmtree(EXTRACTED_FILES_PATH, ignore_errors=True)

        logger.info("Arquivos temporários removidos.")
        logger.info(f"ETL concluído com sucesso em {converter_segundos(start_time, datetime.now())}")

    except Exception as e:
        logger.error(f"Erro no processo ETL: {e}", exc_info=True)
        logger.critical("Não foi possível concluir o ETL")
        sys.exit(1)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Dados Abertos CNPJ - Receita Federal (COPY baseado)")
    parser.add_argument(
        "--etl",
        action="store_true",
        help="Executa o processo completo de ETL (download, extração, carga e índices)",
    )
    parser.add_argument(
        "--inserir_indice",
        action="store_true",
        help="Apenas cria índices nas tabelas (sem recarregar arquivos)",
    )

    args = parser.parse_args()

    if args.etl:
        etl_process()

    if args.inserir_indice:
        criar_indices()
