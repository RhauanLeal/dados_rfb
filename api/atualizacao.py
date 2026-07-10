"""
Módulo de atualização - funções para verificar novas versões no site da RFB
Adaptado de: snfisc/modulos/rfb_dados/functions.py
"""

import requests
import base64
import re
import xml.etree.ElementTree as ET
from urllib.parse import unquote
from bs4 import BeautifulSoup
from api.db import get_connection, return_connection


def obter_registro_mais_recente_db():
    """
    Consulta a tabela info_dados e retorna o registro mais recente (ano, mes, data_atualizacao).
    Retorna None se a tabela não existir ou estiver vazia.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT ano, mes, data_atualizacao
            FROM info_dados
            ORDER BY ano DESC, mes DESC
            LIMIT 1
        """)

        resultado = cur.fetchone()
        cur.close()
        return_connection(conn)

        if resultado:
            ano, mes, data_atualizacao = resultado
            return {
                'ano': ano,
                'mes': mes,
                'data_atualizacao': data_atualizacao
            }
        else:
            raise RuntimeError("Nenhum registro encontrado no banco.")

    except Exception as e:
        return_connection(conn)
        raise RuntimeError(f"Erro ao consultar info_dados: {e}")


def obter_share_token(URL_RAIZ):
    """
    Descobre o token do share público acessando a raiz do site.
    Tenta duas fontes presentes no HTML (SPA Nextcloud):

    1. <meta property="og:url" content=".../index.php/s/{TOKEN}">
    2. <input id="initial-state-files_sharing-sharingToken" value="{base64_json}">
       onde base64_json decodifica para '"TOKEN"' (string JSON com aspas).
    """
    try:
        resp = requests.get(URL_RAIZ, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Erro ao acessar {URL_RAIZ}: {e}")

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Fonte 1: meta og:url
    og = soup.find('meta', property='og:url')
    if og:
        match = re.search(r'/index\.php/s/([A-Za-z0-9]+)', og.get('content', ''))
        if match:
            return match.group(1)

    # Fonte 2: hidden input (valor em base64 → JSON string com aspas)
    inp = soup.find('input', {'id': 'initial-state-files_sharing-sharingToken'})
    if inp:
        try:
            decoded = base64.b64decode(inp.get('value', '')).decode('utf-8')
            token = decoded.strip('"')   # remove aspas do JSON string
            if token:
                return token
        except Exception:
            pass

    return None


def propfind_listar(WEBDAV_BASE, path: str, SHARE_TOKEN) -> list:
    """
    Lista o conteúdo de um diretório no share público via WebDAV PROPFIND.
    Retorna lista de dicts: {'nome': str, 'tipo': 'dir'|'file'}
    """
    url = f"{WEBDAV_BASE}{path}"
    credentials = base64.b64encode(f"{SHARE_TOKEN}:".encode()).decode()
    headers = {
        "Authorization": f"Basic {credentials}",
        "Depth": "1",
        "Content-Type": "application/xml",
    }
    body = ('<?xml version="1.0"?>'
            '<d:propfind xmlns:d="DAV:">'
            '<d:prop><d:displayname/><d:resourcetype/></d:prop>'
            '</d:propfind>')
    try:
        resp = requests.request("PROPFIND", url, headers=headers, data=body, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Erro PROPFIND {url}: {e}")

    ns = {'d': 'DAV:'}
    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as e:
        raise RuntimeError(f"Erro ao parsear XML de {url}: {e}")

    itens = []
    for response in root.findall('d:response', ns):
        href_elem = response.find('d:href', ns)
        if href_elem is None:
            continue
        href = href_elem.text or ''
        prop = response.find('.//d:prop', ns)
        resourcetype = prop.find('d:resourcetype', ns) if prop is not None else None
        is_collection = (resourcetype is not None and
                         resourcetype.find('d:collection', ns) is not None)
        tipo = 'dir' if is_collection else 'file'
        nome = unquote(href.rstrip('/').split('/')[-1])
        if not nome:  # entrada do próprio diretório
            continue
        itens.append({'nome': nome, 'tipo': tipo})

    return itens


def obter_links_ano_mes(WEBDAV_BASE, cnpj_path, SHARE_TOKEN) -> list:
    """
    Lista as pastas AAAA-MM dentro do caminho CNPJ via WebDAV.
    Retorna lista ordenada de dicts: {'ano_mes', 'ano', 'mes', 'path'}
    """
    itens = propfind_listar(WEBDAV_BASE, cnpj_path, SHARE_TOKEN)
    resultado = []

    for item in itens:
        if item['tipo'] != 'dir':
            continue
        nome = item['nome']
        if len(nome) != 7 or nome[4] != '-':
            continue
        try:
            ano = int(nome[:4])
            mes = int(nome[5:])
            if not (1 <= mes <= 12):
                continue
            resultado.append({'ano_mes': nome, 'ano': ano, 'mes': mes,
                               'path': f"{cnpj_path}/{nome}"})
        except ValueError:
            continue

    resultado.sort(key=lambda x: (x['ano'], x['mes']))
    return resultado


def navegar_ate_cnpj(WEBDAV_BASE, CNPJ_PATH, URL_RAIZ, SHARE_TOKEN):
    """
    Verifica a pasta CNPJ via WebDAV.
    O site usa Nextcloud; a navegação é feita por PROPFIND em vez de scraping HTML.
    """
    itens = propfind_listar(WEBDAV_BASE, CNPJ_PATH, SHARE_TOKEN)

    if not itens:
        raise RuntimeError(f"Pasta CNPJ inacessível ou vazia: {CNPJ_PATH}")
    return CNPJ_PATH


def selecionar_pasta_mais_recente(lista_ano_mes, info_db):
    """
    Retorna a pasta mais recente do site se for mais nova que o registro do banco.
    Se não houver histórico no banco, retorna a pasta mais recente do site.
    Retorna None se não houver novidade.
    """
    if not lista_ano_mes:
        return None

    mais_recente_site = lista_ano_mes[-1]

    if info_db is None:
        return mais_recente_site

    tupla_site = (mais_recente_site['ano'], mais_recente_site['mes'])
    tupla_db   = (info_db['ano'], info_db['mes'])

    if tupla_site > tupla_db:
        return mais_recente_site

    return None


def verificar_nova_atualizacao():
    """
    Verifica se há uma nova versão do banco CNPJ disponível no site da RFB.
    Usa WebDAV PROPFIND para navegar na estrutura do site (Nextcloud).

    Retorna dict com:
    - msg: mensagem descritiva
    - update: True se há atualização, False caso contrário
    - ano, mes: versão disponível (se houver atualização)
    - data_atualizacao: data da última atualização no banco (se aplicável)
    """
    # Configuração
    URL_RAIZ    = "https://arquivos.receitafederal.gov.br/"
    WEBDAV_BASE = "https://arquivos.receitafederal.gov.br/public.php/webdav"
    CNPJ_PATH   = "/Dados/Cadastros/CNPJ"

    # dicionário de retorno padrão
    result = {
        "msg": "",
        "update": False,
    }

    # Obter share token
    try:
        SHARE_TOKEN = obter_share_token(URL_RAIZ)
        if not SHARE_TOKEN:
            result["msg"] = "❗ Não foi possível descobrir o token do share público."
            return result
    except Exception as e:
        result["msg"] = f"❗ Erro ao obter share token: {e}"
        return result

    # Consulta o registro mais recente do banco
    try:
        info_db = obter_registro_mais_recente_db()
    except Exception as e:
        result["msg"] = f"❗ Erro ao consultar o banco de dados: {e}"
        # Continua mesmo sem histórico no banco (primeira carga)
        info_db = None

    # Verifica a pasta CNPJ via WebDAV
    try:
        cnpj_path = navegar_ate_cnpj(WEBDAV_BASE, CNPJ_PATH, URL_RAIZ, SHARE_TOKEN)
    except RuntimeError as e:
        result["msg"] = f"❗ Erro ao navegar até a pasta CNPJ: {e}"
        return result

    # Lista pastas AAAA-MM disponíveis
    try:
        lista_ano_mes = obter_links_ano_mes(WEBDAV_BASE, cnpj_path, SHARE_TOKEN)
    except RuntimeError as e:
        result["msg"] = f"❗ Erro ao obter links de pastas AAAA-MM: {e}"
        return result

    if not lista_ano_mes:
        result["msg"] = "❗ Nenhuma pasta AAAA-MM encontrada no site da RFB."
        result["update"] = False
        return result

    # Seleciona a pasta mais recente comparando com o banco
    pasta = selecionar_pasta_mais_recente(lista_ano_mes, info_db)
    if not pasta:
        if info_db:
            result["msg"] = f"✅ Banco está atualizado. Versão atual: {info_db['mes']:02d}/{info_db['ano']}"
            result["update"] = False
            result["ano"] = info_db['ano']
            result["mes"] = info_db['mes']
            result["data_atualizacao"] = str(info_db['data_atualizacao'])
        else:
            result["msg"] = "⚠️  Nenhuma versão carregada no banco. Execute o ETL para carregar os dados."
            result["update"] = False
        return result

    # Há novidade
    result.update({
        "msg": f"🆕 Nova versão disponível no site da RFB: {pasta['mes']:02d}/{pasta['ano']}",
        "update": True,
        "ano": pasta["ano"],
        "mes": pasta["mes"],
        "versao_disponivel": f"{pasta['mes']:02d}/{pasta['ano']}",
    })

    # Adicionar versão atual se existir no banco
    if info_db:
        result["versao_atual"] = f"{info_db['mes']:02d}/{info_db['ano']}"
        result["data_ultima_atualizacao"] = str(info_db['data_atualizacao'])

    return result
