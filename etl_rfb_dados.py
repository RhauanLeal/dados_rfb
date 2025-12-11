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
EXTRACTED_FILES_PATH = BASE_DIR / "files_extracted"
ERRO_FILES_PATH = BASE_DIR / "files_error"

# Tamanho padrão de chunk para leitura dos arquivos grandes
CHUNK_ROWS  = 2_000_000  # 1 milhão de linhas por chunk (leitura)
CHUNK_TO_SQL= 10_000     # 10 mil linhas por insert no to_sql (insert)

logger.info("\n ================================================================================")
logger.info("Iniciando ETL - dados_rfb")

# carrega o arquivo de configuração .env
# Caminho relativo, subindo um nível para acessar dados_rfb/.env
ENV_PATH_PARENT = BASE_DIR.parent / "dados_rfb_env" / ".env"

# Fallback: .env dentro do próprio projeto (dev/teste)
ENV_PATH_LOCAL = BASE_DIR / ".env"

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
                        """CREATE TABLE IF NOT EXISTS public.empresa (cnpj_basico text PRIMARY KEY, razao_social text, natureza_juridica text, qualificacao_responsavel text, capital_social double precision, porte_empresa text, ente_federativo_responsavel text);""",
                        """CREATE TABLE IF NOT EXISTS empresa_porte (codigo INTEGER PRIMARY KEY, descricao TEXT);""",
                        """CREATE TABLE IF NOT EXISTS public.estabelecimento (cnpj_basico text, cnpj_ordem text, cnpj_dv text, identificador_matriz_filial text, nome_fantasia text, situacao_cadastral text, data_situacao_cadastral text, motivo_situacao_cadastral text, nome_cidade_exterior text, pais text, data_inicio_atividade text, cnae_fiscal_principal text, cnae_fiscal_secundaria text, tipo_logradouro text, logradouro text, numero text, complemento text, bairro text, cep text, uf text, municipio text, ddd_1 text, telefone_1 text, ddd_2 text, telefone_2 text, ddd_fax text, fax text, correio_eletronico text, situacao_especial text, data_situacao_especial text, PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv));""",
                        """CREATE TABLE IF NOT EXISTS estabelecimento_situacao_cadastral (codigo INTEGER PRIMARY KEY, descricao TEXT);""",
                        """CREATE TABLE IF NOT EXISTS public.moti (codigo text PRIMARY KEY, descricao text);""",
                        """CREATE TABLE IF NOT EXISTS public.munic (codigo text PRIMARY KEY, descricao text);""",
                        """CREATE TABLE IF NOT EXISTS public.natju (codigo text PRIMARY KEY, descricao text);""",
                        """CREATE TABLE IF NOT EXISTS public.pais (codigo text PRIMARY KEY, descricao text);""",
                        """CREATE TABLE IF NOT EXISTS public.quals (codigo text PRIMARY KEY, descricao text);""",
                        """CREATE TABLE IF NOT EXISTS public.simples (cnpj_basico text PRIMARY KEY, opcao_pelo_simples text, data_opcao_simples text, data_exclusao_simples text, opcao_mei text, data_opcao_mei text, data_exclusao_mei text);""",
                        """CREATE TABLE IF NOT EXISTS public.socios (cnpj_basico text PRIMARY KEY, identificador_socio text, nome_socio_razao_social text, cpf_cnpj_socio text, qualificacao_socio text, data_entrada_sociedade text, pais text, representante_legal text, nome_do_representante text, qualificacao_representante_legal text, faixa_etaria text);""",
                        """CREATE TABLE IF NOT EXISTS socios_identificador (codigo INTEGER PRIMARY KEY, descricao TEXT);"""
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
def get_files(base_url):
    response = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"})
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        logger.info(f"Baixando arquivos do mês: {base_url}")

        # Identificar arquivos ZIP disponíveis para download
        files = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".zip")]

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


# Função para extrair arquivos ZIP
def extract_files(zip_path, extract_to):
    logger.info(f"Extraindo {zip_path} para {extract_to}...")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


# Cria os indices nas tabelas, exceto da info_dados
def criar_indices():
    try:
        conn, _ = connect_db(autocommit=True)  # <- agora com autocommit
        cur = conn.cursor()

        logger.info("Iniciando criação dos índices...")

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
                logger.info(f"Índice {nome_indice} criado com sucesso.")
            except Exception as e:
                logger.error(f"Erro ao criar o índice {nome_indice}: {e}", exc_info=True)

        cur.close()
        conn.close()
        gc.collect()
        logger.info("Criação dos índices finalizada.")

    except Exception as e:
        logger.error(f"Erro geral ao criar índices: {e}", exc_info=True)
        logger.critical("Não foi possível iniciar o aplicativo")
        sys.exit(1)


def move_file_error(extracted_file_path, arquivo):
    try:
        shutil.move(extracted_file_path, ERRO_FILES_PATH / arquivo)
        logger.info(f"Arquivo {arquivo} movido para a pasta de erro: {ERRO_FILES_PATH}")
    except Exception as move_err:
        logger.error(f"Erro ao mover o arquivo {arquivo} para a pasta erro: {move_err}")


# Função principal do ETL
def etl_process():
    try:
        # Criar os diretórios caso não existam
        OUTPUT_FILES_PATH.mkdir(parents=True, exist_ok=True)
        EXTRACTED_FILES_PATH.mkdir(parents=True, exist_ok=True)
        ERRO_FILES_PATH.mkdir(parents=True, exist_ok=True)

        logger.info(f"Diretórios definidos:\n - Output: {OUTPUT_FILES_PATH}\n - Extraídos: {EXTRACTED_FILES_PATH}\n - Arquivos com erro: {ERRO_FILES_PATH}")

        start_time = datetime.now()

        info = verificar_nova_atualizacao()
        if not info:
            logger.info("Nenhuma atualização nova encontrada. Encerrando.")
            return
        
        logger.info(f"Nova atualização encontrada: {info['ano']}-{info['mes']:02d} em {info['data_atualizacao']}")
        logger.info(f"URL base para download: {info['url']}")
        
        files = get_files(info['url'])
        if not files:
            logger.info("Nenhum arquivo .zip para processar. Encerrando.")
            return

        logger.info(f"Arquivos encontrados ({len(files)}): {sorted(files)}")

        # Baixar arquivos
        zip_files = [download_file(info['url'] + file, OUTPUT_FILES_PATH) for file in files]

        # Extrair arquivos
        for zip_file in zip_files:
            extract_files(zip_file, EXTRACTED_FILES_PATH)

        logger.info("Todos os arquivos foram baixados e extraídos. Iniciando processamento dos dados.")
        
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
        
        # Criar tabelas antes de inserir dados
        create_tables()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Começa arquivos_empresa
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "empresa" ;')
        conn.commit()
        for arquivo in arquivos_empresa:
            logger.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logger.warning(f"Arquivo não encontrado: {extracted_file_path}")
                continue

            try:
                empresa_dtypes = {0: object, 1: object, 2: 'Int32', 3: 'Int32', 4: object, 5: 'Int32', 6: object}

                for i, chunk in enumerate(pd.read_csv(extracted_file_path,
                                                    sep=';',
                                                    header=None,
                                                    dtype=empresa_dtypes,
                                                    encoding='latin-1',
                                                    chunksize=CHUNK_ROWS,
                                                    )):

                    chunk.columns = ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_federativo_responsavel']

                    def tratar_capital(x):
                        try:
                            return float(str(x).replace(',', '.'))
                        except:
                            return 0.0

                    chunk['capital_social'] = chunk['capital_social'].apply(tratar_capital)
                    
                    try:
                        chunk.to_sql(
                            name='empresa', 
                            con=engine, 
                            if_exists='append', 
                            index=False, 
                            chunksize=CHUNK_TO_SQL,
                            method='multi'
                            )
                        logger.info(f"Arquivo {arquivo} / parte {i} inserido com sucesso!")
                    except Exception as e:
                        logger.error(f"Erro ao inserir chunk {i} do arquivo {arquivo}: {e}")
                        try:
                            conn.rollback()
                            logger.warning("Rollback realizado. Tentando inserir novamente o chunk...")

                            # Tenta de novo
                            chunk.to_sql(
                                name='empresa',
                                con=engine,
                                if_exists='append',
                                index=False,
                                chunksize=CHUNK_TO_SQL,
                                method='multi'
                            )
                            logger.info(f"Chunk {i} reprocessado com sucesso!")
                        except Exception as segunda_falha:
                            logger.error(f"Falha novamente ao reprocessar chunk {i}: {segunda_falha}")
                            break  # desiste desse arquivo

            except Exception as e:
                logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)

            finally:
                if 'chunk' in locals():
                    del chunk
                gc.collect()

        logger.info("Arquivos de empresa finalizados!")
        # Fecha cursor
        try:
            cur.close()
        except:
            pass

        # Fecha engine e libera RAM
        try:
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "estabelecimento" ;')
        conn.commit()
        for arquivo in arquivos_estabelecimento:
            logger.info(f"Trabalhando no arquivo: {arquivo}")
            
            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logger.warning(f"Arquivo não encontrado: {extracted_file_path}")
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

                    chunk.to_sql(
                        name='estabelecimento',
                        con=engine,
                        if_exists='append',
                        index=False,
                        chunksize=CHUNK_TO_SQL,
                        method='multi'
                    )

                    logger.info(f"Arquivo {arquivo} / parte {i} inserido com sucesso no banco de dados!")

            except Exception as e:
                logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)

            finally:
                if 'chunk' in locals():
                    del chunk
                gc.collect()

        logger.info("Arquivos de estabelecimento finalizados!")
        # Fecha cursor
        try:
            cur.close()
        except:
            pass

        # Fecha engine e libera RAM
        try:
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "socios" ;')
        conn.commit()
        for arquivo in arquivos_socios:
            logger.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logger.warning(f"Arquivo não encontrado: {extracted_file_path}")
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

                    # Gravar dados no banco:
                    chunk.to_sql(
                        name='socios',
                        con=engine,
                        if_exists='append',
                        index=False,
                        chunksize=CHUNK_TO_SQL,
                        method='multi'
                    )

                    logger.info(f"Arquivo {arquivo} / parte {i} inserido com sucesso no banco de dados!")

            except Exception as e:
                logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)

            finally:
                if 'chunk' in locals():
                    del chunk
                gc.collect()

        logger.info("Arquivos de socios finalizados!")
        # Fecha cursor
        try:
            cur.close()
        except:
            pass

        # Fecha engine e libera RAM
        try:
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "simples" ;')
        conn.commit()
        for arquivo in arquivos_simples:
            logger.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logger.warning(f"Arquivo não encontrado: {extracted_file_path}")
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

                    # Gravar dados no banco
                    chunk.to_sql(
                        name='simples',
                        con=engine,
                        if_exists='append',
                        index=False,
                        chunksize=CHUNK_TO_SQL,
                        method='multi'
                    )

                    logger.info(f"Arquivo {arquivo} / parte {i} inserido com sucesso no banco de dados!")

            except Exception as e:
                logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)

            finally:
                if 'chunk' in locals():
                    del chunk
                gc.collect()

        logger.info("Arquivos do simples finalizados!")
        # Fecha cursor
        try:
            cur.close()
        except:
            pass

        # Fecha engine e libera RAM
        try:
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "cnae" ;')
        conn.commit()
        for arquivo in arquivos_cnae:
            logger.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logger.warning(f"Arquivo não encontrado: {extracted_file_path}")
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
                    chunksize=CHUNK_TO_SQL,
                    method='multi'
                )

                logger.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)

            finally:
                if 'cnae' in locals():
                    del cnae
                gc.collect()

        logger.info("Arquivos de cnae finalizados!")
        # Fecha cursor
        try:
            cur.close()
        except:
            pass

        # Fecha engine e libera RAM
        try:
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "moti" ;')
        conn.commit()
        for arquivo in arquivos_moti:
            logger.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logger.warning(f"Arquivo não encontrado: {extracted_file_path}")
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
                    chunksize=CHUNK_TO_SQL,
                    method='multi'
                )

                logger.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)

            finally:
                if 'moti' in locals():
                    del moti
                gc.collect()

        logger.info("Arquivos de moti finalizados!")
        # Fecha cursor
        try:
            cur.close()
        except:
            pass

        # Fecha engine e libera RAM
        try:
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Arquivos de munic:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "munic" ;')
        conn.commit()
        for arquivo in arquivos_munic:
            logger.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logger.warning(f"Arquivo não encontrado: {extracted_file_path}")
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
                    chunksize=CHUNK_TO_SQL,
                    method='multi'
                )

                logger.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)

            finally:
                if 'munic' in locals():
                    del munic
                gc.collect()

        logger.info("Arquivos de munic finalizados!")
        # Fecha cursor
        try:
            cur.close()
        except:
            pass

        # Fecha engine e libera RAM
        try:
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Arquivos de natju:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "natju" ;')
        conn.commit()
        for arquivo in arquivos_natju:
            logger.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logger.warning(f"Arquivo não encontrado: {extracted_file_path}")
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
                    chunksize=CHUNK_TO_SQL,
                    method='multi'
                )

                logger.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)

            finally:
                if 'natju' in locals():
                    del natju
                gc.collect()

        logger.info("Arquivos de natju finalizados!")
        # Fecha cursor
        try:
            cur.close()
        except:
            pass

        # Fecha engine e libera RAM
        try:
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Arquivos de pais:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "pais" ;')
        conn.commit()
        for arquivo in arquivos_pais:
            logger.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logger.warning(f"Arquivo não encontrado: {extracted_file_path}")
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
                    chunksize=CHUNK_TO_SQL,
                    method='multi'
                )

                logger.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)

            finally:
                if 'pais' in locals():
                    del pais
                gc.collect()

        logger.info("Arquivos de pais finalizados!")
        # Fecha cursor
        try:
            cur.close()
        except:
            pass

        # Fecha engine e libera RAM
        try:
            engine.dispose()
        except:
            pass

        gc.collect()

        # Reabre conexão para próximo bloco
        conn, engine = connect_db()
        cur = conn.cursor()

        # Arquivos de qualificação de sócios:
        # Drop table antes do insert
        cur.execute('DROP TABLE IF EXISTS "quals" ;')
        conn.commit()
        for arquivo in arquivos_quals:
            logger.info(f"Trabalhando no arquivo: {arquivo}")

            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, arquivo)
            if not os.path.exists(extracted_file_path):
                logger.warning(f"Arquivo não encontrado: {extracted_file_path}")
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
                    chunksize=CHUNK_TO_SQL,
                    method='multi'
                )

                logger.info(f"Arquivo {arquivo} inserido com sucesso no banco de dados!")

            except Exception as e:
                logger.error(f"Erro ao processar o arquivo {arquivo}: {e}")
                arquivos_com_erro.append(arquivo)
                move_file_error(extracted_file_path, arquivo)

            finally:
                if 'quals' in locals():
                    del quals
                gc.collect()
        
        logger.info("Arquivos de quals finalizados!")
        # Fecha cursor
        try:
            cur.close()
        except:
            pass

        # Fecha engine e libera RAM
        try:
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

        # Criação dos índices
        criar_indices()

        # Remover arquivos após a inserção no banco
        shutil.rmtree(OUTPUT_FILES_PATH)
        shutil.rmtree(EXTRACTED_FILES_PATH)
        logger.info("Arquivos removidos após a carga no banco.")

        logger.info(f"ETL concluído com sucesso em {converter_segundos(start_time, datetime.now())}")

    except Exception as e:
        logger.error(f"Erro no processo ETL: {e}", exc_info=True)
        logger.critical("Não foi possível iniciar o aplicativo")
        sys.exit(1)


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
