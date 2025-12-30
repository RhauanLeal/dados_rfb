#etl_rfb_dados.py
import argparse
import os
import sys
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
import io
import csv
import psutil

'''
Sistema de importação dos dados abertos da Receita Federal do Brasil (RFB)
* arquivos baixados via HTTP são armazenados localmente,
* arquivos são extraídos de .zip para diretórios locais, possuem codificação UNIX(LF) e ANSI (latin-1),
* arquivos extraídos via ETL são carregados via Pandas e inseridos em tabelas permanentes,
* tabela info_dados mantém controle de versões (ano, mês, data_atualizacao),
* logs são armazenados em arquivo e exibidos no console.
* tratamento de erros e movimentação de arquivos com problemas para diretório específico.
* finalizando a importação os arquivos baixados e extraídos são excluídos para economizar espaço em disco.
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
logger.info("Iniciando ETL - dados_rfb G")

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
def verificar_nova_atualizacao():
    # URL base
    base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    response = requests.get(base_url)

    if response.status_code != 200:
        logger.error(f"Erro ao acessar {base_url}: código {response.status_code}")
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
        logger.info("Nenhuma pasta válida encontrada no site da RFB.")
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
            logger.info("A versão mais recente já está registrada no banco de dados.")
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


# traz a lista de arquivos da url
def get_files(base_url, processar_simples=True):
    response = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        # Identificar arquivos ZIP disponíveis para download
        files = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".zip")]

        logger.info(f"Arquivos encontrados ({len(files)}): {sorted(files)}")

        # Se processar_simples for False, remove arquivos que contenham 'Simples' no nome
        if not processar_simples:
            logger.info("Ignorando arquivo do Simples")
            files = [f for f in files if "Simples" not in f and "SIMPLES" not in f]

        if not files:
            logger.info("Nenhum arquivo .zip encontrado para o mês atual.")
            return []

        return sorted(files)
    else:
        logger.error(f'Erro ao acessar {base_url}: código {response.status_code}')
        return []


# Função para verificar se o arquivo já foi baixado e se é necessário atualizar
def check_diff(url, file_name):
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


# Função para baixar arquivos com barra de progresso
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

    logger.info(f"Download concluído para {file_name}")
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


# Cria os indices nas tabelas, exceto da info_dados
def criar_indices():
    try:
        # Importante: autocommit=True é obrigatório para CONCURRENTLY
        conn, _ = connect_db(autocommit=True)
        cur = conn.cursor()

        logger.info("Aumentando memória de manutenção para criação de índices...")
        cur.execute("SET maintenance_work_mem = '2GB';") # Acelera muito no seu Xeon

        logger.info("Iniciando criação dos índices...")

        # 1. Criar a Chave Primária (Isso cria o índice principal do CNPJ)
        # Usamos o comando ALTER TABLE. Isso pode demorar algumas horas no HDD, mas é o correto.
        logger.info("Criando Chave Primária para empresa...")
        cur.execute("""
            ALTER TABLE public.empresa
            ADD PRIMARY KEY (cnpj_basico);
        """)

        logger.info("Criando Chave Primária Composta para Estabelecimento...")
        cur.execute("""
            ALTER TABLE public.estabelecimento 
            ADD PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv);
        """)

        # 2. Índices Adicionais (Opcionais, mas recomendados para performance)
        # Se você for buscar empresas por Município ou por CNAE:
        # Lista expandida para garantir performance em buscas reais
        indices_extras = [
            ("empresa", "porte_empresa"),
            ("estabelecimento", "situacao_cadastral"),
            ("estabelecimento", "cnae_fiscal_principal"),
            ("estabelecimento", "municipio"),
            ("estabelecimento", "uf"),
            ("cnae", "codigo"),
            ("munic", "codigo")
        ]

        for tabela, coluna in indices_extras:
            nome_indice = f"idx_{tabela}_{coluna}"
            try:
                logger.info(f"Criando índice {nome_indice}...")
                # CONCURRENTLY evita travar a tabela, IF NOT EXISTS evita erros em restarts
                sql = f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {nome_indice} ON {tabela} ({coluna});'
                cur.execute(sql)
                logger.info(f"Índice {nome_indice} finalizado.")
            except Exception as e:
                logger.error(f"Erro ao criar o índice {nome_indice}: {e}")

        # Opcional: Analisar as tabelas para atualizar as estatísticas do otimizador
        logger.info("Rodando ANALYZE para otimizar estatísticas...")
        cur.execute("ANALYZE;")

        cur.close()
        conn.close()
        gc.collect()
        logger.info("Processo de indexação concluído com sucesso!")

    except Exception as e:
        logger.error(f"Erro geral ao criar índices: {e}", exc_info=True)
        sys.exit(1)


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
        logger.info(f"URL base para download: {info['url']}")
        
        files = get_files(info['url'], processar_simples=processar_simples)
        if not files:
            logger.info("Nenhum arquivo .zip para processar. Encerrando.")
            return

        # Baixar arquivos
        zip_files = [download_file(info['url'] + file, OUTPUT_FILES_PATH) for file in files]

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
        criar_indices()

        # Remover arquivos após a inserção no banco
        # shutil.rmtree(OUTPUT_FILES_PATH)
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

    # Define o padrão do Simples como True
    parser.set_defaults(processar_simples=True)
    args = parser.parse_args()

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
