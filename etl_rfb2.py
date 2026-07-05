# etl_rfb.py
import argparse
import os
import sys
import logging
import shutil
import threading
import base64
import re
import requests
import zipfile
import pathlib
import gc
import pandas as pd
import psycopg2
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from dotenv import load_dotenv, find_dotenv  # Lembre-se de criar o aquivo .env com as configurações DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote, quote
import io
import csv
import psutil

'''
Sistema de ETL dos dados abertos da Receita Federal do Brasil (RFB)
* verifica novas versoes no portal da RFB via Nextcloud/WebDAV;
* baixa arquivos .zip para o diretorio local de downloads;
* le os arquivos internos dos .zip diretamente com Pandas (em chunks), sem extracao previa;
* carrega os dados no PostgreSQL (to_sql com COPY), com tabelas permanentes;
* em cada carga, limpa (TRUNCATE) e recarrega as tabelas de dados;
* mantem controle de versao na tabela info_dados (ano, mes, data_atualizacao);
* registra logs em arquivo e console, e gera dump de navegacao da RFB;
* move para files_error os .zip com erro de processamento;
* aplica correcoes e criacao de indices ao final da carga;
* os arquivos baixados nao sao removidos automaticamente (remocao esta comentada no codigo).
'''

# Garantir diretório de logs
LOG_DIR = pathlib.Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Criar arquivo de log com codificação UTF-8
log_file = LOG_DIR / "etl_rfb_dados_log.txt"

# Configuração básica para arquivo
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Verificar se configurou corretamente
logger = logging.getLogger(__name__)

# Diretórios fixos para armazenar os arquivos
BASE_DIR = pathlib.Path().resolve()  # Diretório do script
OUTPUT_FILES_PATH = BASE_DIR / "files_downloaded"
ERRO_FILES_PATH = BASE_DIR / "files_error"

# carrega o arquivo de configuração .env
# Caminho relativo, subindo um nível para acessar dados_rfb/.env
ENV_PATH_PARENT = BASE_DIR.parent / "dados_rfb_env" / ".env"

# Fallback: .env dentro do próprio projeto (dev/teste)
ENV_PATH_LOCAL = BASE_DIR / ".env"

logger.info("================================================================================")
logger.info("Iniciando ETL - dados_rfb ")

# Lógica automática:
if os.path.exists(ENV_PATH_PARENT):
    dotenv_path = find_dotenv(ENV_PATH_PARENT)
else:
    dotenv_path = find_dotenv(ENV_PATH_LOCAL)

if not dotenv_path:
    logger.error("Arquivo de configuração .env não encontrado no diretório do projeto.")
    logger.info("ETL Finalizado.")
    sys.exit(1)

# Carrega o arquivo
load_dotenv(dotenv_path)


def calcular_chunks_automatico():
    """Calcula chunks ideais baseado na RAM disponível"""
     
    mem = psutil.virtual_memory()
    ram_gb = mem.total / (1024**3)
    available_gb = mem.available / (1024**3)
    
    logger.info(f"RAM total: {ram_gb:.1f}GB, Disponível: {available_gb:.1f}GB")
    
    if available_gb > 4:
        return 2_000_000, 100_000
    else:
        return 1_000_000, 50_000

CHUNK_ROWS, CHUNK_TO_SQL = calcular_chunks_automatico()
logger.info(f"Usando CHUNK_ROWS={CHUNK_ROWS:,}, CHUNK_TO_SQL={CHUNK_TO_SQL:,}")

# Downloads simultâneos (altere conforme a largura de banda Disponível)
DOWNLOAD_WORKERS = 3

# Ativado via --progress na linha de comando
_show_progress = False


def psql_insert_copy(table, conn, keys, data_iter):
    """
    Função de callback para o pandas.to_sql que utiliza o comando COPY do PostgreSQL.
    """
    # Configura o buffer de memória
    s_buf = io.StringIO()
    writer = csv.writer(s_buf, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
    writer.writerows(data_iter)
    s_buf.seek(0)

    # Acessa o cursor do driver psycopg2 bruto
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        sql = f'COPY "{table.name}" ({", ".join([f'"{k}"' for k in keys])}) FROM STDIN WITH DELIMITER \'\t\' CSV'
        cur.copy_expert(sql=sql, file=s_buf)


def converter_segundos(tempo_inicial: datetime, tempo_final: datetime) -> str:
    '''
    Converte segundos em uma frase de Horas Minutos e Segundos

    Exemplo:
    - Entrada datetime: tempo_inicial = datetime.now(), tempo_final = datetime.now()
    - Saída str: String com o a frase horas minutos e segundos.
    '''
    # Calcula a diferença entre as datas
    diferenca = tempo_final - tempo_inicial
    total_segundos = int(diferenca.total_seconds())

    # Convertendo para horas, minutos e segundos
    horas = total_segundos // 3600
    minutos = (total_segundos % 3600) // 60
    segundos = total_segundos % 60

    # Criando as strings de cada componente
    hora = ''
    minuto = ''
    segundo = ''

    if horas == 1:
        hora = f'{horas} hora'
    elif horas > 1:
        hora = f'{horas} horas'

    if minutos == 1:
        minuto = f'{minutos} minuto'
    elif minutos > 1:
        minuto = f'{minutos} minutos'

    if segundos == 1:
        segundo = f'{segundos} segundo'
    elif segundos > 1:
        segundo = f'{segundos} segundos'
    elif segundos < 1:
        segundo = f'{diferenca} segundos'

    # Juntando os componentes não vazios
    componentes = [hora, minuto, segundo]
    tempo = ', '.join([comp for comp in componentes if comp])

    return tempo


def connect_db(autocommit=False):
    from urllib.parse import quote_plus

    user = os.getenv("DB_USER")
    passw = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT") or "5432"
    database = os.getenv("DB_NAME")

    # AVISO: Verificar se a senha está em branco ou vazia
    if passw is None or passw.strip() == "":
        logger.warning("⚠️  AVISO: A senha do banco de dados (DB_PASSWORD) está em branco ou vazia.")
        logger.warning("Isso pode causar falhas de conexão se o PostgreSQL exigir autenticação.")
        logger.warning("Verifique o arquivo .env e defina DB_PASSWORD=sua_senha")
        
        # Também exibe no console para alertar o usuário
        print("\n⚠️  AVISO CRÍTICO: DB_PASSWORD está vazia!")
        print("Edite o arquivo .env e adicione: DB_PASSWORD=sua_senha_postgres")
        print("Para definir uma senha no PostgreSQL execute: sudo -u postgres psql -c \"\\password\"\n")

    # Validação básica das variáveis obrigatórias
    if not all([user, host, database]):
        logger.error("Erro: variáveis de ambiente DB_* incompletas no .env")
        missing = []
        if not user:
            missing.append("DB_USER")
        if not host:
            missing.append("DB_HOST") 
        if not database:
            missing.append("DB_NAME")
        logger.error(f"Variáveis faltando: {', '.join(missing)}")
        raise ValueError(f"Configuração do banco incompleta. Variáveis faltando: {', '.join(missing)}")

    # Escapa caracteres especiais no usuário e senha
    user_escaped = quote_plus(user)
    passw_escaped = quote_plus(passw) if passw else ""

    # Monta a URL de conexão segura
    conn_str = f"postgresql://{user_escaped}:{passw_escaped}@{host}:{port}/{database}"

    try:
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

        logger.info("✅ Conexão com o banco de dados estabelecida com sucesso")
        return conn, engine

    except psycopg2.OperationalError as e:
        logger.error(f"❌ Erro de conexão com o banco de dados: {e}")
        if "password authentication failed" in str(e):
            logger.error("Falha na autenticação. Verifique a senha no arquivo .env")
        elif "database" in str(e).lower() and "does not exist" in str(e).lower():
            logger.error("Banco de dados não existe. Execute o arquivo dados_rfb.sql primeiro")
        raise
    except Exception as e:
        logger.error(f"❌ Erro inesperado na conexão: {e}")
        raise


# Gerar base URL dinâmica (Ano e Mês atual)
URL_RAIZ = "https://arquivos.receitafederal.gov.br/"
SHARE_TOKEN = ""
WEBDAV_BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav"
CNPJ_PATH = "/Dados/Cadastros/CNPJ"
SALVAR_DUMP_TXT = False  # Ative para salvar o dump de navegação e a lista final de zips em logs/rfb_dump.txt
dump_file = LOG_DIR / "rfb_dump.txt"
_dump_pages: list[dict] = []


def obter_registro_mais_recente_db():
    """
    Consulta a tabela info_dados e retorna o registro mais recente.
    Retorna None se a tabela nao existir ou estiver vazia.
    """
    conn = None
    engine = None
    cur = None
    try:
        conn, engine = connect_db()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = 'info_dados'
            );
            """
        )
        existe_tabela = cur.fetchone()[0]

        if not existe_tabela:
            logger.warning("Tabela info_dados ainda nao existe. Sem historico de versoes.")
            return None

        cur.execute(
            """
            SELECT ano, mes, data_atualizacao
            FROM info_dados
            ORDER BY ano DESC, mes DESC
            LIMIT 1;
            """
        )
        row = cur.fetchone()
        if row:
            logger.info(f"Versao atual no banco: {row[0]}-{row[1]:02d} (data_atualizacao: {row[2]})")
            return {"ano": row[0], "mes": row[1], "data_atualizacao": row[2]}

        logger.info("Tabela info_dados existe mas esta vazia.")
        return None
    except Exception as e:
        logger.warning(f"Nao foi possivel consultar info_dados: {e}. Continuando sem comparacao.")
        return None
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        if engine is not None:
            engine.dispose()


def obter_share_token() -> str | None:
    """
    Descobre o token do share publico na pagina raiz da RFB.
    """
    logger.info(f"Buscando share token em: {URL_RAIZ}")
    try:
        resp = requests.get(URL_RAIZ, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Erro ao acessar {URL_RAIZ}: {e}")
        return None

    _dump_pages.append({"url": URL_RAIZ, "html": resp.text, "links": []})
    soup = BeautifulSoup(resp.text, "html.parser")

    og = soup.find("meta", property="og:url")
    if og:
        match = re.search(r"/index\.php/s/([A-Za-z0-9]+)", og.get("content", ""))
        if match:
            token = match.group(1)
            logger.info(f"Share token obtido via og:url: {token}")
            return token

    inp = soup.find("input", {"id": "initial-state-files_sharing-sharingToken"})
    if inp:
        try:
            decoded = base64.b64decode(inp.get("value", "")).decode("utf-8")
            token = decoded.strip('"')
            if token:
                logger.info(f"Share token obtido via initial-state: {token}")
                return token
        except Exception as e:
            logger.warning(f"Erro ao decodificar sharingToken: {e}")

    logger.error("Share token nao encontrado na pagina raiz.")
    return None


def propfind_listar(path: str) -> list[dict]:
    """
    Lista conteudo de um diretorio WebDAV.
    """
    url = f"{WEBDAV_BASE}{path}"
    credentials = base64.b64encode(f"{SHARE_TOKEN}:".encode()).decode()
    headers = {
        "Authorization": f"Basic {credentials}",
        "Depth": "1",
        "Content-Type": "application/xml",
    }
    body = (
        '<?xml version="1.0"?>'
        '<d:propfind xmlns:d="DAV:">'
        "<d:prop><d:displayname/><d:resourcetype/></d:prop>"
        "</d:propfind>"
    )
    try:
        resp = requests.request("PROPFIND", url, headers=headers, data=body, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Erro PROPFIND {url}: {e}")
        return []

    _dump_pages.append({"url": url, "html": resp.text, "links": []})

    ns = {"d": "DAV:"}
    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as e:
        logger.error(f"Erro ao parsear XML de {url}: {e}")
        return []

    itens = []
    for response in root.findall("d:response", ns):
        href_elem = response.find("d:href", ns)
        if href_elem is None:
            continue

        href = href_elem.text or ""
        prop = response.find(".//d:prop", ns)
        resourcetype = prop.find("d:resourcetype", ns) if prop is not None else None
        is_collection = resourcetype is not None and resourcetype.find("d:collection", ns) is not None
        tipo = "dir" if is_collection else "file"
        nome = unquote(href.rstrip("/").split("/")[-1])
        if not nome:
            continue
        itens.append({"nome": nome, "tipo": tipo})

    return itens


def navegar_ate_cnpj():
    """
    Descobre token e valida acesso ao caminho CNPJ.
    """
    global SHARE_TOKEN
    token = obter_share_token()
    if token:
        SHARE_TOKEN = token
    else:
        logger.warning(f"Usando token fallback: {SHARE_TOKEN}")

    logger.info(f"Verificando pasta CNPJ via WebDAV: {WEBDAV_BASE}{CNPJ_PATH}")
    itens = propfind_listar(CNPJ_PATH)
    if not itens:
        logger.error(f"Pasta CNPJ inacessivel ou vazia: {CNPJ_PATH}")
        return None
    logger.info(f"Pasta CNPJ acessivel. {len(itens)} item(s) encontrado(s).")
    return CNPJ_PATH


def obter_links_ano_mes(cnpj_path):
    """
    Lista pastas AAAA-MM no caminho CNPJ.
    """
    logger.info(f"Buscando pastas AAAA-MM em: {WEBDAV_BASE}{cnpj_path}")
    itens = propfind_listar(cnpj_path)
    resultado = []

    for item in itens:
        if item["tipo"] != "dir":
            continue
        nome = item["nome"]
        if len(nome) != 7 or nome[4] != "-":
            continue
        try:
            ano = int(nome[:4])
            mes = int(nome[5:])
            if not (1 <= mes <= 12):
                continue
            resultado.append({"ano_mes": nome, "ano": ano, "mes": mes, "path": f"{cnpj_path}/{nome}"})
        except ValueError:
            continue

    resultado.sort(key=lambda x: (x["ano"], x["mes"]))
    return resultado


def selecionar_pasta_mais_recente(lista_ano_mes, info_db):
    """
    Retorna a pasta mais recente do site se for mais nova que a do banco.
    """
    if not lista_ano_mes:
        logger.warning("Lista de pastas AAAA-MM vazia.")
        return None

    mais_recente_site = lista_ano_mes[-1]
    if info_db is None:
        logger.info(f"Sem historico no banco. Pasta mais recente do site: {mais_recente_site['ano_mes']}")
        return mais_recente_site

    tupla_site = (mais_recente_site["ano"], mais_recente_site["mes"])
    tupla_db = (info_db["ano"], info_db["mes"])

    if tupla_site > tupla_db:
        logger.info(
            f"Nova versao disponivel no site: {mais_recente_site['ano_mes']} "
            f"(banco possui: {info_db['ano']}-{info_db['mes']:02d})"
        )
        return mais_recente_site

    logger.info(
        f"Sem novidade: site={mais_recente_site['ano_mes']} "
        f"banco={info_db['ano']}-{info_db['mes']:02d}. Nenhum download necessario."
    )
    return None


def salvar_dump_txt(lista_zips: list[str]) -> None:
    """
    Grava em logs/rfb_dump.txt o conteudo carregado do site e a lista final de zips.
    """
    sep = "=" * 80
    with dump_file.open("w", encoding="utf-8") as f:
        f.write(f"{sep}\n")
        f.write(f"DUMP RFB - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total de paginas carregadas: {len(_dump_pages)}\n")
        f.write(f"{sep}\n\n")

        for i, page in enumerate(_dump_pages, 1):
            f.write(f"{'-' * 80}\n")
            f.write(f"PAGINA {i}: {page['url']}\n")
            f.write(f"{'-' * 80}\n\n")
            f.write(f"[HTML BRUTO]\n{page['html']}\n\n")

        f.write(f"{sep}\n")
        f.write(f"LISTA FINAL DE .ZIP ({len(lista_zips)} arquivo(s))\n")
        f.write(f"{sep}\n")
        for url in lista_zips:
            f.write(f"{url}\n")

    logger.info(f"Dump salvo em: {dump_file}")


def obter_lista_zips(path_pasta):
    """
    Lista arquivos .zip em uma pasta AAAA-MM.
    """
    logger.info(f"Buscando arquivos .zip em: {WEBDAV_BASE}{path_pasta}")
    itens = propfind_listar(path_pasta)
    zips = []

    for item in itens:
        if item["tipo"] == "file" and item["nome"].lower().endswith(".zip"):
            url_download = f"{WEBDAV_BASE}{path_pasta}/{quote(item['nome'])}"
            zips.append(url_download)

    return sorted(zips)


def verificar_nova_atualizacao():
    """
    Identifica a versao mais recente disponivel no site da RFB.
    Retorna dict com {ano, mes, data_atualizacao, lista_zips, update} ou None sem novidade.
    """
    criar_tabela_info_dados()
    lista_zips: list[str] = []

    try:
        info_db = obter_registro_mais_recente_db()

        conn_chk, engine_chk = connect_db()
        cur_chk = conn_chk.cursor()
        cur_chk.execute("SELECT COUNT(*) FROM info_dados;")
        update = cur_chk.fetchone()[0] > 0
        cur_chk.close()
        conn_chk.close()
        engine_chk.dispose()

        cnpj_path = navegar_ate_cnpj()
        if not cnpj_path:
            logger.error("Nao foi possivel acessar a pasta CNPJ no site da RFB.")
            return None

        lista_ano_mes = obter_links_ano_mes(cnpj_path)
        if not lista_ano_mes:
            logger.info("Nenhuma pasta AAAA-MM encontrada no site da RFB.")
            return None

        pasta = selecionar_pasta_mais_recente(lista_ano_mes, info_db)
        if not pasta:
            return None

        lista_zips = obter_lista_zips(pasta["path"])
        if not lista_zips:
            logger.warning(f"Nenhum arquivo .zip encontrado em: {pasta['path']}")
            return None

        return {
            "ano": pasta["ano"],
            "mes": pasta["mes"],
            "data_atualizacao": datetime.now(),
            "lista_zips": lista_zips,
            "update": update,
        }
    finally:
        if SALVAR_DUMP_TXT and _dump_pages:
            salvar_dump_txt(lista_zips)


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
        logger.info("Tabelas info_dados e estruturas criadas/verificadas com sucesso!")
    conn.close()
    return True


# Cria as tabelas no banco de dados se elas não existirem.
def create_tables():
    """
    Cria tabelas de forma segura, evitando conflitos com tipos
    """
    # Conectar ao banco de dados
    conn, engine = connect_db()
    try:
        with conn.cursor() as cur:
            try:
                # Tenta criar as tabelas normalmente
                cur.execute("""
                -- Tabela: cnae
                CREATE TABLE IF NOT EXISTS public.cnae (
                    codigo text PRIMARY KEY,
                    descricao text
                );

                -- Tabela: empresa
                -- Não possui indice, sera criado ao final por performance
                CREATE TABLE IF NOT EXISTS public.empresa (
                    cnpj_basico text,
                    razao_social text,
                    natureza_juridica integer,
                    qualificacao_responsavel integer,
                    capital_social double precision,
                    porte_empresa integer,
                    ente_federativo_responsavel text
                );
                            
                -- Tabela: empresa_porte
                CREATE TABLE IF NOT EXISTS public.empresa_porte (
                    codigo INTEGER PRIMARY KEY,
                    descricao TEXT
                );

                -- Tabela: estabelecimento
                -- Não possui indice, sera criado ao final por performance
                CREATE TABLE IF NOT EXISTS public.estabelecimento (
                    cnpj_basico text,
                    cnpj_ordem text,
                    cnpj_dv text,
                    identificador_matriz_filial integer,
                    nome_fantasia text,
                    situacao_cadastral integer,
                    data_situacao_cadastral date,
                    motivo_situacao_cadastral integer,
                    nome_cidade_exterior text,
                    pais integer,
                    data_inicio_atividade date,
                    cnae_fiscal_principal text,
                    cnae_fiscal_secundaria text,
                    tipo_logradouro text,
                    logradouro text,
                    numero text,
                    complemento text,
                    bairro text,
                    cep text,
                    uf text,
                    municipio integer,
                    ddd_1 text,
                    telefone_1 text,
                    ddd_2 text,
                    telefone_2 text,
                    ddd_fax text,
                    fax text,
                    correio_eletronico text,
                    situacao_especial text,
                    data_situacao_especial date
                );
                            
                -- Tabela: estabelecimento_situacao_cadastral
                CREATE TABLE IF NOT EXISTS public.estabelecimento_situacao_cadastral (
                    codigo INTEGER PRIMARY KEY,
                    descricao TEXT
                );
                            
                -- Tabela: estabelecimento_motivo
                CREATE TABLE IF NOT EXISTS public.estabelecimento_motivo (
                    codigo INTEGER PRIMARY KEY,
                    descricao text
                );

                -- Tabela: munic
                CREATE TABLE IF NOT EXISTS public.munic (
                    codigo INTEGER PRIMARY KEY,
                    descricao text
                );

                -- Tabela: empresa_natureza_juridica
                CREATE TABLE IF NOT EXISTS public.empresa_natureza_juridica (
                    codigo INTEGER PRIMARY KEY,
                    descricao text
                );

                -- Tabela: pais
                CREATE TABLE IF NOT EXISTS public.pais (
                    codigo INTEGER PRIMARY KEY,
                    descricao text
                );

                -- Tabela: socios_qualificacao
                CREATE TABLE IF NOT EXISTS public.socios_qualificacao (
                    codigo INTEGER PRIMARY KEY,
                    descricao text
                );

                -- Tabela: simples
                CREATE TABLE IF NOT EXISTS public.simples (
                    cnpj_basico text,
                    opcao_pelo_simples text,
                    data_opcao_simples date,
                    data_exclusao_simples date,
                    opcao_mei text,
                    data_opcao_mei date,
                    data_exclusao_mei date
                );

                -- Tabela: socios
                CREATE TABLE IF NOT EXISTS public.socios (
                    cnpj_basico text,
                    identificador_socio integer,
                    nome_socio_razao_social text,
                    cpf_cnpj_socio text,
                    qualificacao_socio integer,
                    data_entrada_sociedade date,
                    pais integer,
                    representante_legal text,
                    nome_do_representante text,
                    qualificacao_representante_legal integer,
                    faixa_etaria integer
                );

                -- Tabela: socios_identificador
                CREATE TABLE IF NOT EXISTS public.socios_identificador (
                    codigo INTEGER PRIMARY KEY,
                    descricao TEXT
                );
                """)
                
                # Inserts separados para evitar conflitos
                cur.execute("""
                INSERT INTO empresa_porte (codigo, descricao) VALUES
                    (1, 'Microempresa'),
                    (3, 'Empresa de Pequeno Porte'),
                    (5, 'Demais')
                ON CONFLICT (codigo) DO NOTHING;
                """)
                
                cur.execute("""
                INSERT INTO estabelecimento_situacao_cadastral (codigo, descricao) VALUES
                    (1, 'Nula'),
                    (2, 'Ativa'),
                    (3, 'Suspensa'),
                    (4, 'Inapta'),
                    (5, 'Ativa Não Regular'),
                    (8, 'Baixada')
                ON CONFLICT (codigo) DO NOTHING;
                """)
                
                cur.execute("""
                INSERT INTO socios_identificador (codigo, descricao) VALUES
                    (1, 'Pessoa Jurídica'),
                    (2, 'Pessoa Física'),
                    (3, 'Sócio Estrangeiro')
                ON CONFLICT (codigo) DO NOTHING;
                """)
                
                conn.commit()
                logger.info("Tabelas criadas/verificadas com sucesso!")
                
            except psycopg2.errors.UniqueViolation as e:
                if 'pg_type_typname_nsp_index' in str(e):
                    logger.warning("Conflito de tipo detectado. Fazendo rollback e tentando abordagem alternativa...")
                    conn.rollback()
                    
                    # Abordagem alternativa: criar tabelas uma por uma
                    tables_sql = [
                        """CREATE TABLE IF NOT EXISTS public.cnae (codigo text PRIMARY KEY, descricao text);""",
                        """CREATE TABLE IF NOT EXISTS public.empresa (cnpj_basico text, razao_social text, natureza_juridica integer, qualificacao_responsavel integer, capital_social double precision, porte_empresa integer, ente_federativo_responsavel text);""",
                        """CREATE TABLE IF NOT EXISTS empresa_porte (codigo INTEGER PRIMARY KEY, descricao TEXT);""",
                        """CREATE TABLE IF NOT EXISTS public.estabelecimento (cnpj_basico text, cnpj_ordem text, cnpj_dv text, identificador_matriz_filial integer, nome_fantasia text, situacao_cadastral integer, data_situacao_cadastral date, motivo_situacao_cadastral integer, nome_cidade_exterior text, pais integer, data_inicio_atividade date, cnae_fiscal_principal text, cnae_fiscal_secundaria text, tipo_logradouro text, logradouro text, numero text, complemento text, bairro text, cep text, uf text, municipio integer, ddd_1 text, telefone_1 text, ddd_2 text, telefone_2 text, ddd_fax text, fax text, correio_eletronico text, situacao_especial text, data_situacao_especial date);""",
                        """CREATE TABLE IF NOT EXISTS estabelecimento_situacao_cadastral (codigo INTEGER PRIMARY KEY, descricao TEXT);""",
                        """CREATE TABLE IF NOT EXISTS public.estabelecimento_motivo (codigo INTEGER PRIMARY KEY, descricao text);""",
                        """CREATE TABLE IF NOT EXISTS public.munic (codigo INTEGER PRIMARY KEY, descricao text);""",
                        """CREATE TABLE IF NOT EXISTS public.empresa_natureza_juridica (codigo INTEGER PRIMARY KEY, descricao text);""",
                        """CREATE TABLE IF NOT EXISTS public.pais (codigo INTEGER PRIMARY KEY, descricao text);""",
                        """CREATE TABLE IF NOT EXISTS public.socios_qualificacao (codigo INTEGER PRIMARY KEY, descricao text);""",
                        """CREATE TABLE IF NOT EXISTS public.simples (cnpj_basico text, opcao_pelo_simples text, data_opcao_simples date, data_exclusao_simples date, opcao_mei text, data_opcao_mei date, data_exclusao_mei date);""",
                        """CREATE TABLE IF NOT EXISTS public.socios (cnpj_basico text, identificador_socio integer, nome_socio_razao_social text, cpf_cnpj_socio text, qualificacao_socio integer, data_entrada_sociedade date, pais integer, representante_legal text, nome_do_representante text, qualificacao_representante_legal integer, faixa_etaria integer);""",
                        """CREATE TABLE IF NOT EXISTS socios_identificador (codigo INTEGER PRIMARY KEY, descricao text);"""
                    ]
                    
                    for sql in tables_sql:
                        try:
                            cur.execute(sql)
                            conn.commit()
                        except psycopg2.errors.DuplicateTable:
                            conn.rollback()
                            logger.info("Tabela já existe, continuando...")
                        except psycopg2.errors.UniqueViolation:
                            conn.rollback()
                            logger.info("Conflito de tipo ignorado, continuando...")
                        except Exception as e:
                            conn.rollback()
                            logger.warning(f"Erro ao criar tabela: {e}, continuando...")
                    
                    # Inserts após criar todas as tabelas
                    try:
                        cur.execute("INSERT INTO empresa_porte (codigo, descricao) VALUES (1, 'Microempresa'), (3, 'Empresa de Pequeno Porte'), (5, 'Demais') ON CONFLICT (codigo) DO NOTHING;")
                        cur.execute("INSERT INTO estabelecimento_situacao_cadastral (codigo, descricao) VALUES (1, 'Nula'), (2, 'Ativa'), (3, 'Suspensa'), (4, 'Inapta'), (5, 'Ativa Não Regular'), (8, 'Baixada') ON CONFLICT (codigo) DO NOTHING;")
                        cur.execute("INSERT INTO socios_identificador (codigo, descricao) VALUES (1, 'Pessoa Jurídica'), (2, 'Pessoa Física'), (3, 'Sócio Estrangeiro') ON CONFLICT (codigo) DO NOTHING;")
                        conn.commit()
                    except Exception as e:
                        conn.rollback()
                        logger.warning(f"Erro nos inserts: {e}")
                    
                    logger.info("Tabelas criadas/verificadas com abordagem alternativa!")
                else:
                    raise e
    # fecha a conexão
    finally:
        conn.close()
        engine.dispose()


# insere os dados na tabela info_dados
def inserir_info_dados(info):
    # Reabre conexão para próximo bloco
    conn, engine = connect_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO info_dados (ano, mes, data_atualizacao)
        VALUES (%s, %s, %s)
        ON CONFLICT (ano, mes)
        DO UPDATE SET data_atualizacao = EXCLUDED.data_atualizacao;
    """, (info['ano'], info['mes'], info['data_atualizacao']))

    conn.commit()
    logger.info(f"Atualização inserida no banco: {info['ano']}-{info['mes']:02d}")
    
    cur.close() # Fecha a conexão com o banco
    conn.close() # Fecha a conexão com o banco


# ---------------------------------------------------------------------------
# Sessão HTTP com headers de navegador para downloads do Nextcloud
# ---------------------------------------------------------------------------
_download_session: requests.Session | None = None
_session_lock = threading.Lock()

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

def _get_download_session() -> requests.Session:
    """
    Retorna (ou cria) uma Session configurada para download via WebDAV do share público.
    Usa Basic auth (token, '') — mesmo mecanismo usado pelo PROPFIND no scraper.
    Thread-safe via double-checked locking.
    """
    global _download_session
    if _download_session is None:
        with _session_lock:
            if _download_session is None:
                session = requests.Session()
                session.headers.update(_BROWSER_HEADERS)
                token = SHARE_TOKEN
                if token:
                    credentials = base64.b64encode(f"{token}:".encode()).decode()
                    session.headers['Authorization'] = f"Basic {credentials}"
                    logger.info(f"Sessão WebDAV inicializada com token: {token}")
                else:
                    logger.warning("SHARE_TOKEN não Disponível para autenticação WebDAV.")
                _download_session = session
    return _download_session


# Função para verificar se o arquivo já foi baixado e se é necessário atualizar
def check_diff(url, file_name):
    if not os.path.isfile(file_name):
        return True

    try:
        session = _get_download_session()
        response = session.head(url, timeout=10, allow_redirects=True)
        new_size = int(response.headers.get("content-length", 0))
    except Exception as e:
        logger.warning(f"Erro ao verificar cabeçalho de {url}: {e}")
        return True

    old_size = os.path.getsize(file_name)
    if new_size != old_size:
        os.remove(file_name)
        return True

    return False


# Função para baixar arquivos com barra de progresso
def download_file(url, output_path, tqdm_position=0):
    # Extrai o nome do arquivo da URL:
    #   WebDAV: .../public.php/webdav/Dados/.../Cnaes.zip → "Cnaes.zip"
    #   (fallback legado): .../download?path=...&files=Cnaes.zip → "Cnaes.zip"
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    if 'files' in qs:
        basename = qs['files'][0]
    else:
        basename = unquote(parsed.path.split('/')[-1])

    file_name = os.path.join(output_path, basename)

    if not check_diff(url, file_name):
        logger.info(f"Arquivo {basename} já está atualizado.")
        return file_name

    logger.info(f"Baixando {basename}...")

    session = _get_download_session()
    response = session.get(url, stream=True, timeout=300)

    if response.status_code != 200:
        logger.error(f"Erro HTTP {response.status_code} ao baixar {basename}")
        return None

    content_type = response.headers.get('Content-Type', '')
    if 'text/html' in content_type:
        logger.error(
            f"Nextcloud retornou HTML para {basename} (Content-Type: {content_type}). "
            "Token expirado ou sessão inválida."
        )
        return None

    total_bytes = int(response.headers.get('content-length', 0))
    bytes_written = 0

    if _show_progress:
        pbar = tqdm(
            total=total_bytes if total_bytes > 0 else None,
            unit='B', unit_scale=True, unit_divisor=1024,
            desc=f"{basename[:35]:<35}", ncols=90, leave=True,
            position=tqdm_position,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
        )

    with open(file_name, "wb") as f:
        for chunk in response.iter_content(chunk_size=65536):
            if chunk:
                f.write(chunk)
                bytes_written += len(chunk)
                if _show_progress:
                    pbar.update(len(chunk))

    if _show_progress:
        pbar.close()

    size_mb = bytes_written / (1024 * 1024)
    logger.info(f"Download concluído para {basename} ({size_mb:.1f} MB)")

    if bytes_written == 0:
        logger.error(f"Arquivo {basename} baixado com 0 bytes! Removendo arquivo inválido.")
        os.remove(file_name)
        return None

    return file_name


def apply_fixes(processar_simples=True):
    """
    Aplica correções estáticas na base de dados.
    """
    # É recomendável rodar isso ANTES de criar os índices para ser mais rápido
    conn, engine = connect_db()
    cur = conn.cursor()

    try:
        logger.info("APLICANDO CORREÇÕES NA BASE DE DADOS...")

        # Inserções simples (Tabelas auxiliares são pequenas, aqui é instantâneo)
        # Corrigindo Países
        logger.info("Inserindo correções em pais...")
        cur.execute("""
            INSERT INTO public.pais (codigo, descricao) 
            VALUES 
                (008, 'ABU DHABI'),
                (009, 'DIRCE'),
                (015, 'ALAND, ILHAS'),
                (150, 'JERSEY, ILHA DO CANAL'),
                (151, 'CANARIAS, ILHAS'),
                (200, 'CURACAO'),
                (321, 'GUERNSEY'),
                (359, 'MAN, ILHA DE'),
                (367, 'INGLATERRA'),
                (393, 'JERSEY'),
                (449, 'MACEDONIA (ANTIGA REP. IUGOSLAVA)'),
                (452, 'MADEIRA, ILHA DA'),
                (498, 'MOLDAVIA'),
                (578, 'PALESTINA'),
                (678, 'SAO TOME E PRINCIPE'),
                (699, 'SAO MARTINHO, ILHA DE (PARTE HOLANDESA)'),
                (737, 'SERVIA'),
                (994, 'AZERBAIJAO')
            ON CONFLICT (codigo) DO NOTHING;
        """)

        # # 1. DELETE Duplicatas - CUIDADO: Pode demorar muito se a tabela empresa for grande
        # logger.info("Removendo duplicatas da tabela empresa...")
        # query_delete_duplicatas = """
        #     DELETE FROM empresa
        #     WHERE ctid IN (
        #         SELECT ctid FROM (
        #             SELECT ctid, ROW_NUMBER() OVER (
        #                 PARTITION BY cnpj_basico ORDER BY 
        #                 CASE WHEN razao_social IS NOT NULL AND TRIM(razao_social) <> '' THEN 0 ELSE 1 END, 
        #                 ctid
        #             ) as rn FROM empresa
        #         ) t WHERE t.rn > 1
        #     );
        # """
        # cur.execute(query_delete_duplicatas)

        # 2. UPDATES em Estabelecimento - IMPORTANTE: Filtre apenas o necessário
        # logger.info("Limpando códigos de país na tabela estabelecimento...")
        # cur.execute("UPDATE estabelecimento SET pais = NULL WHERE pais = '0';")
        
        # cur.execute("""
        #     UPDATE estabelecimento 
        #     SET pais = LPAD(pais, 3, '0') 
        #     WHERE pais IS NOT NULL AND LENGTH(TRIM(pais)) = 2;
        # """)

        # # 3. Portes Vazios
        # logger.info("Corrigindo porte na tabela empresa...")
        # cur.execute("UPDATE empresa SET porte = '00' WHERE porte = '' OR porte IS NULL;")

        # 4. CNPJs problemáticos conhecidos no Simples
        if processar_simples:
            logger.info("Limpando registros problemáticos da tabela Simples...")
            # Aqui usamos DELETE normal. Como não tem PK, ele funciona sem erros.
            cur.execute("""
                DELETE FROM public.simples 
                WHERE cnpj_basico IN ('24417449', '24539162', '30721933', '30728066', 
                                    '30760363', '30847991', '30857441', '30886793', '30972017');
            """)

        conn.commit()
        logger.info("CORREÇÕES APLICADAS COM SUCESSO.")

    except Exception as e:
        conn.rollback() # Reverte se der erro para não corromper
        logger.error(f"ERRO AO APLICAR CORREÇÕES: {e}")
        logger.info("ETL Finalizado.")
        raise
    
    finally:
        cur.close()
        conn.close()
        engine.dispose() # Libera recursos da engine
        gc.collect()


def _constraint_exists(cur, table_name: str, constraint_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
          FROM pg_constraint
         WHERE conname = %s
           AND conrelid = %s::regclass
         LIMIT 1;
        """,
        [constraint_name, f"public.{table_name}"],
    )
    return cur.fetchone() is not None


# Cria os indices nas tabelas, exceto da info_dados
def criar_indices(update=False):
    logger.info("Iniciando processo de indexação e otimização de estatísticas...")
    conn = None
    cur = None
    try:
        # Importante: autocommit=True é obrigatório para CONCURRENTLY
        conn, _ = connect_db(autocommit=True)
        cur = conn.cursor()

        logger.info("Aumentando memória de manutenção para otimização...")
        cur.execute("SET maintenance_work_mem = '2GB';")

        if not update:
            logger.info("Primeira carga: criando índices. Isso pode levar várias horas...")
        else:
            logger.info("Atualização detectada: recriando índices e atualizando estatísticas...")

        # 1. Cria/recria PKs
        if update:
            logger.info("Removendo PKs antigas para recriação em atualização...")
            try:
                cur.execute('ALTER TABLE "empresa" DROP CONSTRAINT IF EXISTS "empresa_pkey" CASCADE;')
            except Exception:
                pass
            try:
                cur.execute('ALTER TABLE "estabelecimento" DROP CONSTRAINT IF EXISTS "estabelecimento_pkey" CASCADE;')
            except Exception:
                pass

        try:
            if not _constraint_exists(cur, "empresa", "empresa_pkey"):
                logger.info("Criando Chave Primária para empresa...")
                cur.execute("""
                    ALTER TABLE public.empresa
                    ADD CONSTRAINT empresa_pkey PRIMARY KEY (cnpj_basico);
                """)
            else:
                logger.info("PK empresa já existe.")
        except Exception as e:
            logger.warning(
                f"Não foi possível criar PK de empresa (continuando): {e}"
            )
            conn.rollback()

        try:
            if not _constraint_exists(cur, "estabelecimento", "estabelecimento_pkey"):
                logger.info("Criando Chave Primária Composta para Estabelecimento...")
                cur.execute("""
                    ALTER TABLE public.estabelecimento
                    ADD CONSTRAINT estabelecimento_pkey
                    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv);
                """)
            else:
                logger.info("PK estabelecimento já existe.")
        except Exception as e:
            logger.warning(
                f"Não foi possível criar PK de estabelecimento (continuando): {e}"
            )
            conn.rollback()

        # 2. Índices adicionais (inclui os críticos para consulta por CNPJ).
        indices_extras = [
            ("empresa", "cnpj_basico"),   # crítico para buscar_empresa_por_cnpj
            ("socios", "cnpj_basico"),    # crítico para buscar_socios_por_cnpj
            ("empresa", "porte_empresa"),
            ("empresa", "natureza_juridica"),
            ("estabelecimento", "situacao_cadastral"),
            ("estabelecimento", "cnae_fiscal_principal"),
            ("estabelecimento", "municipio"),
            ("estabelecimento", "uf"),
            ("cnae", "codigo"),
            ("munic", "codigo"),
        ]

        for tabela, coluna in indices_extras:
            nome_indice = f"idx_{tabela}_{coluna}"
            try:
                logger.info(f"Criando índice {nome_indice}...")
                # CONCURRENTLY evita travar tabela; IF NOT EXISTS evita erro em reexecução.
                sql = (
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {nome_indice} "
                    f"ON {tabela} ({coluna});"
                )
                cur.execute(sql)
                logger.info(f"Índice {nome_indice} finalizado.")
            except Exception as e:
                logger.error(f"Erro ao criar o índice {nome_indice}: {e}")
                conn.rollback()

        # Atualiza estatísticas do planner após indexação
        logger.info("Rodando ANALYZE para otimizar estatísticas...")
        cur.execute("ANALYZE;")

        logger.info("Processo de indexação concluído com sucesso!")

    except Exception as e:
        logger.error(f"Erro geral ao criar índices: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        gc.collect()


def move_file_error(zip_path, arquivo):
    """
    Move o arquivo ZIP que contém o arquivo com erro para a pasta de erros.
    
    Args:
        zip_path: Caminho completo do arquivo ZIP
        arquivo: Nome do arquivo dentro do ZIP que teve erro
    """
    try:
        # Garante que o diretório de erros existe
        ERRO_FILES_PATH.mkdir(parents=True, exist_ok=True)
        
        # Nome do arquivo ZIP
        zip_filename = os.path.basename(zip_path)
        
        # Caminho de destino
        destino = ERRO_FILES_PATH / zip_filename
        
        # Move o arquivo ZIP para a pasta de erros
        shutil.move(zip_path, destino)
        
        logger.warning(f"Arquivo ZIP '{zip_filename}' movido para pasta de erros devido a erro no arquivo '{arquivo}'")
        
    except Exception as e:
        logger.error(f"Erro ao mover arquivo ZIP para pasta de erros: {e}")


def parse_brazilian_float(value):
    """
    Converte valores brasileiros (vírgula decimal) para float
    Trata casos especiais: None, NaN, strings vazias, etc.

    # Uso:
    chunk['capital_social'] = chunk['capital_social'].apply(parse_brazilian_float)
    """
    if pd.isna(value):
        return 0.0

    if isinstance(value, str):
        value = value.strip()
        if value == '' or value.lower() in ['nan', 'none', 'null']:
            return 0.0

        # Remove pontos de milhar e substitui vírgula decimal por ponto
        value = value.replace('.', '').replace(',', '.')

    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def remove_duplicates_by_key(df: pd.DataFrame, key_column: str) -> pd.DataFrame:
    """
    Remove duplicatas mantendo o primeiro registro de cada chave.
    Útil para dados da RFB que podem ter CNPJs/chaves duplicadas.
    """
    duplicates = df[df.duplicated(subset=[key_column], keep=False)]
    if len(duplicates) > 0:
        logger.warning(f"Encontradas {len(duplicates) // 2} linhas duplicadas em '{key_column}'. Removendo...")

    return df.drop_duplicates(subset=[key_column], keep='first')


# Função principal do ETL
def etl_process(processar_simples=True):
    try:
        # Criar os diretórios caso não existam
        OUTPUT_FILES_PATH.mkdir(parents=True, exist_ok=True)
        ERRO_FILES_PATH.mkdir(parents=True, exist_ok=True)

        logger.info(f"Diretórios definidos:\n - Output: {OUTPUT_FILES_PATH}\n - Arquivos com erro: {ERRO_FILES_PATH}")

        start_time = datetime.now()

        info = verificar_nova_atualizacao()
        if not info:
            logger.info("Nenhuma atualização nova encontrada. Encerrando.")
            return
        
        logger.info(f"Nova atualização encontrada: {info['ano']}-{info['mes']:02d} em {info['data_atualizacao']}")

        lista_zips = info['lista_zips']

        # Filtra arquivos do Simples se necessário
        if not processar_simples:
            lista_zips = [url for url in lista_zips if 'Simples' not in url and 'SIMPLES' not in url]
            logger.info("Ignorando arquivo do Simples")

        if not lista_zips:
            logger.info("Nenhum arquivo .zip para processar. Encerrando.")
            return

        urls = ",\n".join(sorted(lista_zips))
        logger.info(f"Arquivos .zip para download ({len(lista_zips)}): \n{urls}")

        # Baixar arquivos (DOWNLOAD_WORKERS downloads simultâneos)
        logger.info(f"Iniciando downloads com {DOWNLOAD_WORKERS} worker(s) simultâneo(s)...")
        with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
            futures = [
                executor.submit(download_file, url, OUTPUT_FILES_PATH, i % DOWNLOAD_WORKERS)
                for i, url in enumerate(lista_zips)
            ]
            zip_files = [f.result() for f in futures]

        # Filtra downloads que falharam
        zip_files = [f for f in zip_files if f is not None]

        if not zip_files:
            logger.error("Nenhum arquivo foi baixado com sucesso. Encerrando.")
            return

        logger.info("Todos os arquivos foram baixados. Iniciando processamento dos dados.")

        # Processar os arquivos após download completo
        arquivos_empresa = []
        arquivos_estabelecimento = []
        arquivos_socios = []
        arquivos_simples = []
        arquivos_cnae = []
        arquivos_estabelecimento_motivo = []
        arquivos_munic = []
        arquivos_empresa_natureza_juridica = []
        arquivos_pais = []
        arquivos_socios_qualificacao = []

        # Arquivos com erro no processamento
        arquivos_com_erro = []
        
        # Coleta todos os arquivos de todos os ZIPs
        for zip_file in zip_files:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    # Cria tupla (nome_arquivo, caminho_zip) para manter referência
                    file_info = (file, zip_file)
                    
                    if "EMPRE" in file:
                        arquivos_empresa.append(file_info)
                    elif "ESTABELE" in file:
                        arquivos_estabelecimento.append(file_info)
                    elif "SOCIO" in file:
                        arquivos_socios.append(file_info)
                    elif "SIMPLES" in file:
                        arquivos_simples.append(file_info)
                    elif "CNAE" in file:
                        arquivos_cnae.append(file_info)
                    elif "MOTI" in file:
                        arquivos_estabelecimento_motivo.append(file_info)
                    elif "MUNIC" in file:
                        arquivos_munic.append(file_info)
                    elif "NATJU" in file:
                        arquivos_empresa_natureza_juridica.append(file_info)
                    elif "PAIS" in file:
                        arquivos_pais.append(file_info)
                    elif "QUALS" in file:
                        arquivos_socios_qualificacao.append(file_info)

        # Deixar em ordem alfabética (ordena pela primeira parte da tupla, que é o nome do arquivo)
        arquivos_empresa.sort(key=lambda x: x[0])
        arquivos_estabelecimento.sort(key=lambda x: x[0])
        arquivos_socios.sort(key=lambda x: x[0])
        arquivos_simples.sort(key=lambda x: x[0])
        arquivos_cnae.sort(key=lambda x: x[0])
        arquivos_estabelecimento_motivo.sort(key=lambda x: x[0])
        arquivos_munic.sort(key=lambda x: x[0])
        arquivos_empresa_natureza_juridica.sort(key=lambda x: x[0])
        arquivos_pais.sort(key=lambda x: x[0])
        arquivos_socios_qualificacao.sort(key=lambda x: x[0])
        
        # Criar tabelas antes de inserir dados
        create_tables()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Começa arquivos_empresa
        # Remove PK antes de limpar (em caso de duplicatas nos dados)
        logger.info("Removendo PRIMARY KEY de empresa...")
        try:
            cur.execute('ALTER TABLE "empresa" DROP CONSTRAINT IF EXISTS "empresa_pkey" CASCADE;')
            conn.commit()
        except Exception:
            pass

        # Limpa a tabela antes do insert
        logger.info("Limpando dados da tabela empresa (mantendo estrutura)...")
        cur.execute('TRUNCATE TABLE "empresa" ;')
        conn.commit()
 
        # Processa cada arquivo (agora é tupla: nome_arquivo, zip_path)
        for arquivo, zip_path in arquivos_empresa:
            logger.info(f"Trabalhando no arquivo: {arquivo} do ZIP: {os.path.basename(zip_path)}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(arquivo) as file:
                    try:
                        empresa_dtypes = {
                            0: object, 
                            1: object, 
                            2: 'Int32', 
                            3: 'Int32', 
                            4: object, 
                            5: 'Int32', 
                            6: object
                            }
                        
                        # Alterado para leitura em chunks para manter o consumo de RAM estável
                        for i, chunk in enumerate(pd.read_csv(
                                filepath_or_buffer=file,
                                sep=';',
                                header=None,
                                dtype=empresa_dtypes,
                                encoding='latin-1',
                                chunksize=CHUNK_ROWS)):

                            chunk.columns = ['cnpj_basico', 'razao_social', 'natureza_juridica',
                                            'qualificacao_responsavel', 'capital_social',
                                            'porte_empresa', 'ente_federativo_responsavel']

                            # Tratamento de capital social otimizado
                            chunk['capital_social'] = chunk['capital_social'].apply(parse_brazilian_float)

                            # Remove duplicatas por CNPJ (proteção contra dados duplicados da RFB)
                            chunk = remove_duplicates_by_key(chunk, 'cnpj_basico')

                            try:
                                # Gravar dados no banco usando COPY (Alta performance para HDD)
                                chunk.to_sql(
                                    name='empresa',
                                    con=engine,
                                    if_exists='append',
                                    index=False,
                                    method=psql_insert_copy  # Chama a função que criamos acima
                                )
                                logger.info(f"Arquivo {arquivo} / parte {i} inserido via COPY com sucesso!")
                                
                            except Exception as e:
                                logger.error(f"Erro ao inserir via COPY: {e}")
                                # Se falhar aqui, geralmente é erro de tipo de dado na coluna
                                break 
                                
                            finally:
                                del chunk
                                gc.collect()

                    except Exception as e:
                        logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                        arquivos_com_erro.append(arquivo)
                        move_file_error(zip_path, arquivo)

                    finally:
                        gc.collect()

        logger.info("Arquivos de empresa finalizados!")
        
        # Encerramento seguro
        try:
            cur.close()
            conn.close() # Importante fechar a conexão bruta também
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão
        conn, engine = connect_db()
        cur = conn.cursor()

        # Limpa a tabela antes do insert
        logger.info("Limpando dados da tabela estabelecimento (mantendo estrutura)...")
        cur.execute('TRUNCATE TABLE "estabelecimento";')
        conn.commit()

        # Processa cada arquivo (agora é tupla: nome_arquivo, zip_path)
        for arquivo, zip_path in arquivos_estabelecimento:
            logger.info(f"Trabalhando no arquivo: {arquivo} do ZIP: {os.path.basename(zip_path)}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(arquivo) as file:

                    try:
                        # Dtypes para evitar que o Pandas tente adivinhar e consuma RAM
                        estabelecimento_dtypes = {
                            0: object, 1: object, 2: object, 3: 'Int32', 4: object, 5: 'Int32', 6: object,
                            7: 'Int32', 8: object, 9: 'Int32', 10: object, 11: object, 12: object, 13: object,
                            14: object, 15: object, 16: object, 17: object, 18: object, 19: object,
                            20: 'Int32', 21: object, 22: object, 23: object, 24: object, 25: object,
                            26: object, 27: object, 28: object, 29: object
                        }

                        for i, chunk in enumerate(pd.read_csv(
                            filepath_or_buffer=file,
                            sep=';',
                            header=None,
                            dtype=estabelecimento_dtypes,
                            encoding='latin-1',
                            chunksize=CHUNK_ROWS,
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

                            # --- TRATAMENTO DE DATAS (Opcional, mas evita erros no Postgres) ---
                            # Converte YYYYMMDD para YYYY-MM-DD ou None se zero/inválido
                            colunas_datas = ['data_situacao_cadastral', 'data_inicio_atividade', 'data_situacao_especial']

                            chunk[colunas_datas] = chunk[colunas_datas].apply(lambda col: pd.to_datetime(col, format='%Y%m%d',errors='coerce'))

                            try:
                                # Gravar dados no banco usando COPY (Alta performance para HDD)
                                chunk.to_sql(
                                    name='estabelecimento',
                                    con=engine,
                                    if_exists='append',
                                    index=False,
                                    method=psql_insert_copy 
                                )
                                logger.info(f"Arquivo {arquivo} / parte {i} inserido via COPY!")

                            except Exception as e:
                                logger.error(f"Erro no insert do chunk {i}: {e}")
                                break

                            finally:
                                del chunk
                                gc.collect()

                    except Exception as e:
                        logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                        arquivos_com_erro.append(arquivo)
                        move_file_error(zip_path, arquivo) 

                    finally:
                        gc.collect()

        logger.info("Arquivos de estabelecimento finalizados!")

        # Encerramento seguro
        try:
            cur.close()
            conn.close() # Importante fechar a conexão bruta também
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão se necessário ou usa a existente
        conn, engine = connect_db()
        cur = conn.cursor()

        # Limpa a tabela antes do insert
        logger.info("Limpando dados da tabela socios (mantendo estrutura)...")
        cur.execute('TRUNCATE TABLE "socios";')
        conn.commit()

        # Processa cada arquivo (agora é tupla: nome_arquivo, zip_path)
        for arquivo, zip_path in arquivos_socios:
            logger.info(f"Trabalhando no arquivo: {arquivo} do ZIP: {os.path.basename(zip_path)}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(arquivo) as file:

                    try:
                        # Tipagem otimizada para Sócios
                        socios_dtypes = {
                            0: object, 1: 'Int32', 2: object, 3: object, 4: 'Int32',
                            5: object, 6: 'Int32', 7: object, 8: object,
                            9: 'Int32', 10: 'Int32'
                        }

                        for i, chunk in enumerate(pd.read_csv(
                            filepath_or_buffer=file,
                            sep=';',
                            header=None,
                            dtype=socios_dtypes,
                            encoding='latin-1',
                            chunksize=CHUNK_ROWS,
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

                            # --- TRATAMENTO DE DATAS (Opcional, mas evita erros no Postgres) ---
                            # Converte YYYYMMDD para YYYY-MM-DD ou None se zero/inválido
                            colunas_datas = ['data_entrada_sociedade']
                            
                            chunk[colunas_datas] = chunk[colunas_datas].apply(lambda col: pd.to_datetime(col, format='%Y%m%d',errors='coerce'))

                            # Gravar dados no banco usando COPY (Alta performance para HDD)
                            try:
                                chunk.to_sql(
                                    name='socios',
                                    con=engine,
                                    if_exists='append',
                                    index=False,
                                    method=psql_insert_copy  # Otimização vital
                                )
                                logger.info(f"Arquivo {arquivo} / parte {i} inserido via COPY com sucesso!")

                            except Exception as e:
                                logger.error(f"Erro ao inserir chunk {i} de sócios: {e}")
                                break

                            finally:
                                del chunk
                                gc.collect()

                    except Exception as e:
                        logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                        arquivos_com_erro.append(arquivo)
                        move_file_error(zip_path, arquivo) 

                    finally:
                        gc.collect()

        logger.info("Arquivos de sócios finalizados!")
        # Encerramento seguro
        try:
            cur.close()
            conn.close() # Importante fechar a conexão bruta também
            engine.dispose()
        except:
            pass

        gc.collect()

        if processar_simples:
            # Reabre conexão
            conn, engine = connect_db()
            cur = conn.cursor()

            # Limpa a tabela antes do insert
            logger.info("Limpando dados da tabela simples (mantendo estrutura)...")
            cur.execute('TRUNCATE TABLE "simples";')
            conn.commit()

            # Processa cada arquivo (agora é tupla: nome_arquivo, zip_path)
            for arquivo, zip_path in arquivos_simples:
                logger.info(f"Trabalhando no arquivo: {arquivo} do ZIP: {os.path.basename(zip_path)}")
                
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    with zip_ref.open(arquivo) as file:

                        try:
                            # Dtypes: Lemos datas como object (string) para limpar os '00000000' antes
                            simples_dtypes = {
                                0: object,
                                1: object,
                                2: object, # data_opcao_simples
                                3: object, # data_exclusao_simples
                                4: object,
                                5: object, # data_opcao_mei
                                6: object  # data_exclusao_mei
                            }

                            for i, chunk in enumerate(pd.read_csv(
                                filepath_or_buffer=file,
                                sep=';',
                                header=None,
                                dtype=simples_dtypes,
                                encoding='latin-1',
                                chunksize=CHUNK_ROWS,
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

                                # --- TRATAMENTO DE DATAS (Opcional, mas evita erros no Postgres) ---
                                # Converte YYYYMMDD para YYYY-MM-DD ou None se zero/inválido
                                colunas_datas = [
                                    'data_opcao_simples', 'data_exclusao_simples', 
                                    'data_opcao_mei', 'data_exclusao_mei'
                                ]
                                
                                chunk[colunas_datas] = chunk[colunas_datas].apply(lambda col: pd.to_datetime(col, format='%Y%m%d', errors='coerce'))

                                # Gravar dados no banco usando COPY (Alta performance para HDD)
                                try:
                                    chunk.to_sql(
                                        name='simples',
                                        con=engine,
                                        if_exists='append',
                                        index=False,
                                        method=psql_insert_copy # Função de alta performance
                                    )
                                    logger.info(f"Arquivo {arquivo} / parte {i} inserido via COPY com sucesso!")

                                except Exception as e:
                                    logger.error(f"Erro ao inserir chunk {i} de simples: {e}")
                                    break

                                finally:
                                    del chunk
                                    gc.collect()

                        except Exception as e:
                            logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                            arquivos_com_erro.append(arquivo)
                            move_file_error(zip_path, arquivo) 

                        finally:
                            gc.collect()

            logger.info("Arquivos do simples finalizados!")

            # Encerramento seguro de recursos
            try:
                cur.close()
                conn.close()
                engine.dispose()
            except:
                pass

            gc.collect()

        # Reabre conexão
        conn, engine = connect_db()
        cur = conn.cursor()

        # Limpa a tabela antes do insert
        logger.info("Limpando dados da tabela cnae (mantendo estrutura)...")
        cur.execute('TRUNCATE TABLE "cnae";')
        conn.commit()

        # Processa cada arquivo (agora é tupla: nome_arquivo, zip_path)
        for arquivo, zip_path in arquivos_cnae:
            logger.info(f"Trabalhando no arquivo: {arquivo} do ZIP: {os.path.basename(zip_path)}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(arquivo) as file:

                    try:
                        cnae_dtypes = {
                            0: object,
                            1: object
                        }

                        # Adicionado chunksize para manter o consumo de RAM constante e baixo
                        for i, chunk in enumerate(pd.read_csv(
                            filepath_or_buffer=file,
                            sep=';',
                            header=None,
                            dtype=cnae_dtypes,
                            encoding='latin-1',
                            chunksize=CHUNK_ROWS # Mantém o padrão de segurança
                        )):

                            # Renomear colunas
                            chunk.columns = ['codigo', 'descricao']

                            # Gravar dados no banco usando COPY (Alta performance para HDD)
                            try:
                                chunk.to_sql(
                                    name='cnae',
                                    con=engine,
                                    if_exists='append',
                                    index=False,
                                    method=psql_insert_copy
                                )
                                logger.info(f"Arquivo {arquivo} / parte {i} inserido via COPY com sucesso!")

                            except Exception as e:
                                logger.error(f"Erro ao inserir chunk {i} de CNAE: {e}")
                                break

                            finally:
                                del chunk
                                gc.collect()

                    except Exception as e:
                        logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                        arquivos_com_erro.append(arquivo)
                        move_file_error(zip_path, arquivo) 

                    finally:
                        gc.collect()

        logger.info("Arquivos de cnae finalizados!")

        # Encerramento seguro
        try:
            cur.close()
            conn.close()
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão
        conn, engine = connect_db()
        cur = conn.cursor()

        # Limpa a tabela antes do insert
        logger.info("Limpando dados da tabela estabelecimento_motivo (mantendo estrutura)...")
        cur.execute('TRUNCATE TABLE "estabelecimento_motivo" ;')
        conn.commit()

        # Processa cada arquivo (agora é tupla: nome_arquivo, zip_path)
        for arquivo, zip_path in arquivos_estabelecimento_motivo:
            logger.info(f"Trabalhando no arquivo: {arquivo} do ZIP: {os.path.basename(zip_path)}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(arquivo) as file:

                    try:
                        estabelecimento_motivo_dtypes = {
                            0: 'Int32',
                            1: object
                        }

                        # Alterado para ler em chunks para garantir baixo uso de RAM
                        for i, chunk in enumerate(pd.read_csv(
                            filepath_or_buffer=file,
                            sep=';',
                            header=None,
                            dtype=estabelecimento_motivo_dtypes,
                            encoding='latin-1',
                            chunksize=CHUNK_ROWS
                        )):

                            # Renomear colunas
                            chunk.columns = ['codigo', 'descricao']

                            # Gravar dados no banco usando COPY (Alta performance para HDD)
                            try:
                                chunk.to_sql(
                                    name='estabelecimento_motivo',
                                    con=engine,
                                    if_exists='append',
                                    index=False,
                                    method=psql_insert_copy
                                )
                                logger.info(f"Arquivo {arquivo} / parte {i} inserido via COPY com sucesso!")

                            except Exception as e:
                                logger.error(f"Erro ao inserir chunk {i} de estabelecimento_motivo: {e}")
                                break

                            finally:
                                del chunk
                                gc.collect()

                    except Exception as e:
                        logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                        arquivos_com_erro.append(arquivo)
                        move_file_error(zip_path, arquivo) 

                    finally:
                        gc.collect()

        logger.info("Arquivos de estabelecimento_motivo finalizados!")

        # Encerramento seguro de recursos
        try:
            cur.close()
            conn.close()
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão
        conn, engine = connect_db()
        cur = conn.cursor()

        # Limpa a tabela antes do insert
        logger.info("Limpando dados da tabela munic (mantendo estrutura)...")
        cur.execute('TRUNCATE TABLE "munic";')
        conn.commit()

        # Processa cada arquivo (agora é tupla: nome_arquivo, zip_path)
        for arquivo, zip_path in arquivos_munic:
            logger.info(f"Trabalhando no arquivo: {arquivo} do ZIP: {os.path.basename(zip_path)}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(arquivo) as file:

                    try:
                        munic_dtypes = {
                            0: 'Int32',
                            1: object
                        }

                        # Processamento em chunks para evitar picos de RAM
                        for i, chunk in enumerate(pd.read_csv(
                            filepath_or_buffer=file, 
                            sep=';',
                            header=None,
                            dtype=munic_dtypes,
                            encoding='latin-1',
                            chunksize=CHUNK_ROWS
                        )):

                            # Renomear colunas
                            chunk.columns = ['codigo', 'descricao']

                            # Gravar dados no banco usando COPY (Alta performance para HDD)
                            try:
                                chunk.to_sql(
                                    name='munic',
                                    con=engine,
                                    if_exists='append',
                                    index=False,
                                    method=psql_insert_copy
                                )
                                logger.info(f"Arquivo {arquivo} / parte {i} inserido via COPY!")

                            except Exception as e:
                                logger.error(f"Erro ao inserir chunk {i} de municípios: {e}")
                                break

                            finally:
                                # Limpeza agressiva por chunk
                                del chunk
                                gc.collect()

                    except Exception as e:
                        logger.error(f"Erro ao processar o arquivo de municípios {arquivo}: {e}")
                        arquivos_com_erro.append(arquivo)
                        move_file_error(zip_path, arquivo) 

                    finally:
                        gc.collect()

        logger.info("Arquivos de municípios finalizados!")

        # Encerramento seguro de recursos
        try:
            cur.close()
            conn.close()
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão
        conn, engine = connect_db()
        cur = conn.cursor()

        # Limpa a tabela antes do insert
        logger.info("Limpando dados da tabela empresa_natureza_juridica (mantendo estrutura)...")
        cur.execute('TRUNCATE TABLE "empresa_natureza_juridica";')
        conn.commit()

        # Processa cada arquivo (agora é tupla: nome_arquivo, zip_path)
        for arquivo, zip_path in arquivos_empresa_natureza_juridica:
            logger.info(f"Trabalhando no arquivo: {arquivo} do ZIP: {os.path.basename(zip_path)}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(arquivo) as file:

                    try:
                        empresa_natureza_juridica_dtypes = {
                            0: 'Int32',
                            1: object
                        }

                        # Alterado para leitura em chunks para manter o consumo de RAM estável
                        for i, chunk in enumerate(pd.read_csv(
                            filepath_or_buffer=file, 
                            sep=';',
                            header=None,
                            dtype=empresa_natureza_juridica_dtypes,
                            encoding='latin-1',
                            chunksize=CHUNK_ROWS
                        )):

                            # Renomear colunas
                            chunk.columns = ['codigo', 'descricao']

                            # Gravar dados no banco usando COPY (Alta performance para HDD)
                            try:
                                chunk.to_sql(
                                    name='empresa_natureza_juridica',
                                    con=engine,
                                    if_exists='append',
                                    index=False,
                                    method=psql_insert_copy
                                )
                                logger.info(f"Arquivo {arquivo} / parte {i} inserido via COPY!")

                            except Exception as e:
                                logger.error(f"Erro ao inserir chunk {i} de empresa_natureza_juridica: {e}")
                                break

                            finally:
                                # Limpeza de memória imediata
                                del chunk
                                gc.collect()

                    except Exception as e:
                        logger.error(f"Erro ao processar o arquivo de empresa_natureza_juridica {arquivo}: {e}")
                        arquivos_com_erro.append(arquivo)
                        move_file_error(zip_path, arquivo) 

                    finally:
                        gc.collect()

        logger.info("Arquivos de empresa_natureza_juridica finalizados!")

        # Encerramento seguro de recursos
        try:
            cur.close()
            conn.close()
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão
        conn, engine = connect_db()
        cur = conn.cursor()

        # Limpa a tabela antes do insert
        logger.info("Limpando dados da tabela pais (mantendo estrutura)...")
        cur.execute('TRUNCATE TABLE "pais";')
        conn.commit()

        # Processa cada arquivo (agora é tupla: nome_arquivo, zip_path)
        for arquivo, zip_path in arquivos_pais:
            logger.info(f"Trabalhando no arquivo: {arquivo} do ZIP: {os.path.basename(zip_path)}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(arquivo) as file:

                    try:
                        pais_dtypes = {
                            0: 'Int32',
                            1: object
                        }

                        # Leitura em chunks para estabilidade total da RAM
                        for i, chunk in enumerate(pd.read_csv(
                            filepath_or_buffer=file, 
                            sep=';',
                            header=None,
                            dtype=pais_dtypes,
                            encoding='latin-1',
                            chunksize=CHUNK_ROWS
                        )):

                            # Renomear colunas
                            chunk.columns = ['codigo', 'descricao']

                            # Gravar dados no banco usando COPY (Alta performance para HDD)
                            try:
                                chunk.to_sql(
                                    name='pais',
                                    con=engine,
                                    if_exists='append',
                                    index=False,
                                    method=psql_insert_copy
                                )
                                logger.info(f"Arquivo {arquivo} / parte {i} inserido via COPY com sucesso!")

                            except Exception as e:
                                logger.error(f"Erro ao inserir chunk {i} de países: {e}")
                                break

                            finally:
                                # Limpeza de memória
                                del chunk
                                gc.collect()

                    except Exception as e:
                        logger.error(f"Erro ao processar o arquivo de países {arquivo}: {e}")
                        arquivos_com_erro.append(arquivo)
                        move_file_error(zip_path, arquivo) 

                    finally:
                        gc.collect()

        logger.info("Arquivos de países finalizados!")

        # Encerramento seguro de conexões
        try:
            cur.close()
            conn.close()
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Arquivos de qualificação de sócios:
        # Limpa a tabela antes do insert
        logger.info("Limpando dados da tabela socios_qualificacao (mantendo estrutura)...")
        cur.execute('TRUNCATE TABLE "socios_qualificacao" ;')
        conn.commit()

        # Processa cada arquivo (agora é tupla: nome_arquivo, zip_path)
        for arquivo, zip_path in arquivos_socios_qualificacao:
            logger.info(f"Trabalhando no arquivo: {arquivo} do ZIP: {os.path.basename(zip_path)}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                with zip_ref.open(arquivo) as file:

                    try:
                        socios_qualificacao_dtypes = {
                            0: 'Int32',
                            1: object
                        }

                        # Alterado para leitura em chunks para manter o consumo de RAM estável
                        for i, chunk in enumerate(pd.read_csv(
                            filepath_or_buffer=file,
                            sep=';',
                            header=None,
                            dtype=socios_qualificacao_dtypes,
                            encoding='latin-1',
                            chunksize=CHUNK_ROWS
                        )):

                            # Renomear colunas
                            chunk.columns = ['codigo', 'descricao']

                            # Gravar dados no banco usando COPY (Alta performance para HDD)
                            try:
                                chunk.to_sql(
                                    name='socios_qualificacao',
                                    con=engine,
                                    if_exists='append',
                                    index=False,
                                    method=psql_insert_copy  # Otimização vital
                                )
                                logger.info(f"Arquivo {arquivo} / parte {i} inserido via COPY com sucesso!")

                            except Exception as e:
                                logger.error(f"Erro ao inserir chunk {i} de socios_qualificacao: {e}")
                                break

                            finally:
                                # Limpeza de memória imediata
                                del chunk
                                gc.collect()

                    except Exception as e:
                        logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                        arquivos_com_erro.append(arquivo)
                        move_file_error(zip_path, arquivo) 

                    finally:
                        gc.collect()

        logger.info("Arquivos de socios_qualificacao finalizados!")

        # Encerramento seguro de recursos
        try:
            cur.close()
            conn.close()
            engine.dispose()
        except:
            pass

        gc.collect()

        # Grava os dados e gera um txt com os arquivos com erro
        if arquivos_com_erro:
            logger.warning(f"Arquivos com erro: {arquivos_com_erro}")
            logger.warning(f"Arquivos com erro foram movidos para: {ERRO_FILES_PATH}")
           
            with open("arquivos_com_erro.txt", "w", encoding="utf-8") as f:
                for nome in arquivos_com_erro:
                    f.write(nome + "\n")
        
        # Inserir os dados da ultima atualização na tabela info_dados
        inserir_info_dados(info)

        # Processo de inserção finalizado
        logger.info('Processo de carga dos arquivos finalizado!')

        # Aplica correções nas tabelas
        apply_fixes(processar_simples=processar_simples)

        # Criação dos índices
        criar_indices(info['update'])

        # Remover arquivos após a inserção no banco
        shutil.rmtree(OUTPUT_FILES_PATH)
        logger.info("Arquivos removidos após a carga no banco.")

        logger.info(f"ETL concluído com sucesso em {converter_segundos(start_time, datetime.now())}")

    except Exception as e:
        logger.error(f"Erro no processo ETL: {e}", exc_info=True)
        logger.critical("Não foi possível iniciar o aplicativo")
        # Encerramento seguro de recursos
        try:
            cur.close()
            conn.close()
            engine.dispose()
            gc.collect()
        except:
            pass
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Receita Federal do Brasil")
    
    # Flag para rodar o ETL (será True por padrão se nada for passado)
    parser.add_argument(
        "--etl", 
        action="store_true", 
        help="Executa o processo de extração e carga."
    )
    
    # Flag para pular o simples
    parser.add_argument(
        "--no-simples", 
        action="store_false", 
        dest="processar_simples", 
        help="Não processa os dados do Simples Nacional."
    )
    
    parser.add_argument(
        "--inserir_indice", 
        action="store_true", 
        help="Cria índices nas tabelas."
    )

    parser.add_argument(
        "--fixes",
        action="store_true",
        help="Aplica correções na base."
    )

    parser.add_argument(
        "--progress",
        action="store_true",
        help="Exibe barra de progresso no terminal durante os downloads"
    )

    # Define o padrão do Simples como True
    parser.set_defaults(processar_simples=True)
    args = parser.parse_args()

    if args.progress:
        _show_progress = True

    # LÓGICA DE EXECUÇÃO PADRÃO:
    # Se o usuário não passou NENHUM argumento (nem --etl, nem --fixes, nem --inserir_indice)
    # nós forçamos a execução do ETL completo.
    if not (args.etl or args.inserir_indice or args.fixes):
        logger.info("Nenhum argumento detectado. Iniciando ETL completo por padrão...")
        args.etl = True

    # 1. Executa ETL (se solicitado ou se for o padrão)
    if args.etl:
        logger.info(f"Iniciando ETL (Processar Simples: {args.processar_simples})")
        etl_process(processar_simples=args.processar_simples)

    # 2. Executa Correções (se solicitado explicitamente)
    if args.fixes:
        apply_fixes(processar_simples=args.processar_simples)

    # 3. Executa Índices (se solicitado explicitamente)
    if args.inserir_indice:
        criar_indices()
