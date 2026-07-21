from fastapi import FastAPI, HTTPException, Query, Depends, BackgroundTasks
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from functools import lru_cache
import json
import os
import csv
import io
import subprocess
import sys
from datetime import datetime
from api.db import get_connection, return_connection, close_all_connections
from api.auth import create_token, verify_token
from api.atualizacao import (
    verificar_nova_atualizacao,
    verificar_atualizacao_local,
    registrar_versao_disponivel,
    garantir_tabela_versao_disponivel,
)
from typing import Optional

app = FastAPI(title="dados_rfb API", version="1.0.0")


class TokenRequest(BaseModel):
    client_id: str
    client_secret: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class AtualizacaoResponse(BaseModel):
    msg: str
    atualizado: bool
    nova_versao_disponivel: bool
    ano: Optional[int] = None
    mes: Optional[int] = None
    versao_disponivel: Optional[str] = None
    versao_atual: Optional[str] = None
    data_ultima_atualizacao: Optional[str] = None


class VersaoDisponivelRequest(BaseModel):
    ano: int
    mes: int
    origem: str = "manual"
    observacao: Optional[str] = None


CACHE_TTL = 3600  # 1 hora


@lru_cache(maxsize=1024)
def _fetch_lookup_table(table_name: str) -> dict:
    """Cache lookup tables (cnae, munic, empresa_natureza_juridica, etc.)"""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f'SELECT codigo, descricao FROM "{table_name}" ORDER BY codigo')
        result = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        return result
    finally:
        return_connection(conn)


def _get_lookup_desc(table_name: str, codigo) -> str:
    """Get description from cached lookup table"""
    if codigo is None:
        return None
    lookup = _fetch_lookup_table(table_name)
    return lookup.get(codigo)


def _to_int(value):
    """Convert value to int, handling None and empty strings"""
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@app.get("/health")
def health_check():
    """Health check endpoint (sem autenticação)"""
    return {"status": "ok"}


@app.get("/help")
def help_endpoints():
    """
    Lista todos os endpoints disponíveis da API dados_rfb.
    Sem autenticação requerida.
    """
    return {
        "api_info": {
            "nome": "API dados_rfb",
            "versao": "1.0.0",
            "descricao": "API de busca e atualização de dados CNPJ da Receita Federal",
            "base_url": "http://localhost:8001"
        },
        "endpoints": {
            "Autenticação": [
                {
                    "metodo": "POST",
                    "rota": "/auth/token",
                    "descricao": "Obter JWT token para acessar endpoints protegidos",
                    "requer_auth": False,
                    "parametros": {
                        "client_id": "snfisc",
                        "client_secret": "snfisc-secret-change-in-production"
                    },
                    "exemplo": "curl -X POST http://localhost:8001/auth/token -H 'Content-Type: application/json' -d '{\"client_id\":\"snfisc\", \"client_secret\":\"snfisc-secret-change-in-production\"}'"
                }
            ],
            "Saúde": [
                {
                    "metodo": "GET",
                    "rota": "/health",
                    "descricao": "Verificar se a API está operacional",
                    "requer_auth": False,
                    "exemplo": "curl http://localhost:8001/health"
                },
                {
                    "metodo": "GET",
                    "rota": "/help",
                    "descricao": "Listar todos os endpoints disponíveis",
                    "requer_auth": False,
                    "exemplo": "curl http://localhost:8001/help"
                }
            ],
            "Busca - Empresas": [
                {
                    "metodo": "GET",
                    "rota": "/empresas/buscar",
                    "descricao": "Buscar empresas com filtros avançados",
                    "requer_auth": True,
                    "parametros": {
                        "obrigatorios": ["uf", "municipio"],
                        "opcionais": ["cnpj_basico", "razao_nome", "natureza_juridica", "porte_empresa", "page", "limit"]
                    },
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/empresas/buscar?uf=SP&municipio=7121&limit=10'"
                },
                {
                    "metodo": "GET",
                    "rota": "/empresas/exportar-csv",
                    "descricao": "Exportar empresas em CSV (completo, sem paginação)",
                    "requer_auth": True,
                    "parametros": {
                        "obrigatorios": ["uf", "municipio"],
                        "opcionais": ["cnpj_basico", "razao_nome", "natureza_juridica", "porte_empresa", "max_records"]
                    },
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/empresas/exportar-csv?uf=SP&municipio=7121' -o empresas.csv"
                }
            ],
            "Busca - Estabelecimentos": [
                {
                    "metodo": "GET",
                    "rota": "/estabelecimentos/buscar",
                    "descricao": "Buscar estabelecimentos com filtros avançados",
                    "requer_auth": True,
                    "parametros": {
                        "obrigatorios": ["uf", "municipio"],
                        "opcionais": ["cnpj_basico", "razao_nome", "natureza_juridica", "porte_empresa", "situacao_cadastral", "motivo_situacao_cadastral", "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria", "page", "limit"]
                    },
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/estabelecimentos/buscar?uf=SP&municipio=7121&porte_empresa=1'"
                },
                {
                    "metodo": "GET",
                    "rota": "/estabelecimentos/exportar-csv",
                    "descricao": "Exportar estabelecimentos em CSV",
                    "requer_auth": True,
                    "parametros": {
                        "obrigatorios": ["uf", "municipio"],
                        "opcionais": ["cnpj_basico", "razao_nome", "natureza_juridica", "porte_empresa", "situacao_cadastral", "motivo_situacao_cadastral", "data_inicio_atividade", "cnae_fiscal_principal", "cnae_fiscal_secundaria", "max_records"]
                    },
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/estabelecimentos/exportar-csv?uf=SP&municipio=7121' -o estabelecimentos.csv"
                }
            ],
            "Busca - Sócios": [
                {
                    "metodo": "GET",
                    "rota": "/socios-buscar",
                    "descricao": "Buscar sócios com filtros avançados",
                    "requer_auth": True,
                    "parametros": {
                        "obrigatorios": ["uf", "municipio"],
                        "opcionais": ["cnpj_basico", "nome_socio_razao_social", "cpf_cnpj_socio", "page", "limit"]
                    },
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/socios-buscar?uf=SP&municipio=7121'"
                },
                {
                    "metodo": "GET",
                    "rota": "/socios/exportar-csv",
                    "descricao": "Exportar sócios em CSV",
                    "requer_auth": True,
                    "parametros": {
                        "obrigatorios": ["uf", "municipio"],
                        "opcionais": ["cnpj_basico", "nome_socio_razao_social", "cpf_cnpj_socio", "max_records"]
                    },
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/socios/exportar-csv?uf=SP&municipio=7121' -o socios.csv"
                }
            ],
            "Dados Detalhados (por CNPJ)": [
                {
                    "metodo": "GET",
                    "rota": "/empresa/{cnpj_basico}",
                    "descricao": "Obter dados completos de uma empresa e seu estabelecimento principal",
                    "requer_auth": True,
                    "parametros": {
                        "path": ["cnpj_basico"],
                        "query": ["cnpj_ordem"]
                    },
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/empresa/12345678'"
                },
                {
                    "metodo": "GET",
                    "rota": "/socios/{cnpj_basico}",
                    "descricao": "Obter todos os sócios de um CNPJ específico",
                    "requer_auth": True,
                    "parametros": {
                        "path": ["cnpj_basico"]
                    },
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/socios/12345678'"
                },
                {
                    "metodo": "GET",
                    "rota": "/filiais/{cnpj_basico}",
                    "descricao": "Obter todas as filiais de um CNPJ",
                    "requer_auth": True,
                    "parametros": {
                        "path": ["cnpj_basico"]
                    },
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/filiais/12345678'"
                },
                {
                    "metodo": "GET",
                    "rota": "/tudo/{cnpj_basico}",
                    "descricao": "Obter dados agregados: empresa, estabelecimento, sócios e filiais",
                    "requer_auth": True,
                    "parametros": {
                        "path": ["cnpj_basico"],
                        "query": ["cnpj_ordem"]
                    },
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/tudo/12345678'"
                }
            ],
            "Dados de Referência (Lookup)": [
                {
                    "metodo": "GET",
                    "rota": "/referencia/portes",
                    "descricao": "Obter lista de portes de empresa disponíveis",
                    "requer_auth": True,
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/referencia/portes'"
                },
                {
                    "metodo": "GET",
                    "rota": "/referencia/naturezas-juridicas",
                    "descricao": "Obter lista de naturezas jurídicas disponíveis",
                    "requer_auth": True,
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/referencia/naturezas-juridicas'"
                },
                {
                    "metodo": "GET",
                    "rota": "/referencia/situacoes-cadastrais",
                    "descricao": "Obter lista de situações cadastrais disponíveis",
                    "requer_auth": True,
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/referencia/situacoes-cadastrais'"
                },
                {
                    "metodo": "GET",
                    "rota": "/referencia/motivos-situacao",
                    "descricao": "Obter lista de motivos de situação cadastral disponíveis",
                    "requer_auth": True,
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/referencia/motivos-situacao'"
                }
            ],
            "Atualização": [
                {
                    "metodo": "GET",
                    "rota": "/atualizar/verificar",
                    "descricao": "Verificar se há novas atualizações disponíveis no site da RFB",
                    "requer_auth": True,
                    "observacao": "Consulta o site da RFB em tempo real - timeout recomendado 10-15s",
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/atualizar/verificar'"
                },
                {
                    "metodo": "GET",
                    "rota": "/atualizar/status",
                    "descricao": "Obter status da última atualização realizada",
                    "requer_auth": True,
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/atualizar/status'"
                },
                {
                    "metodo": "POST",
                    "rota": "/atualizar/iniciar",
                    "descricao": "Iniciar o processo de atualização do banco de dados (executa em background)",
                    "requer_auth": True,
                    "observacao": "Pode demorar várias horas - executa em background",
                    "exemplo": "curl -X POST -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/atualizar/iniciar'"
                },
                {
                    "metodo": "GET",
                    "rota": "/atualizar/verificar-local",
                    "descricao": "Verificar se há nova versão pendente usando somente dados locais (sem acessar o site da RFB)",
                    "requer_auth": True,
                    "observacao": "Rápido - compara info_dados com rfb_versao_disponivel. Ideal para outros sistemas consultarem com frequência.",
                    "exemplo": "curl -H 'Authorization: Bearer TOKEN' 'http://localhost:8001/atualizar/verificar-local'"
                },
                {
                    "metodo": "POST",
                    "rota": "/atualizar/versao-disponivel",
                    "descricao": "Registrar manualmente a versão mais recente conhecida como disponível no site da RFB",
                    "requer_auth": True,
                    "parametros": {
                        "obrigatorios": ["ano", "mes"],
                        "opcionais": ["origem", "observacao"]
                    },
                    "observacao": "Popula a tabela usada por /atualizar/verificar-local. Upsert por ano/mes.",
                    "exemplo": "curl -X POST -H 'Authorization: Bearer TOKEN' -H 'Content-Type: application/json' -d '{\"ano\":2026,\"mes\":6}' 'http://localhost:8001/atualizar/versao-disponivel'"
                }
            ]
        },
        "notas_importantes": [
            "Todos os endpoints, exceto /auth/token e /health, requerem autenticação JWT",
            "O token é obtido via /auth/token e dura 24 horas",
            "Passar token no header: Authorization: Bearer <seu_token>",
            "Busca com paginação retorna JSON estruturado (max 1000 registros por página)",
            "Export em CSV retorna arquivo completo (max 500k registros)",
            "UF e município são obrigatórios em todos os endpoints de busca",
            "Indices no banco recomendados para performance (executados no ETL)"
        ],
        "filtros_comuns": {
            "uf": "Estado (ex: SP, RJ, MG)",
            "municipio": "Código do município (integer)",
            "cnpj_basico": "CNPJ base 8 dígitos (opcional)",
            "page": "Número da página (padrão: 1)",
            "limit": "Registros por página (padrão: 100, máx: 1000)",
            "max_records": "Limite de registros CSV (padrão: 100000, máx: 500000)"
        },
        "codigos_comuns": {
            "natureza_juridica": "Código numérico (ex: 2062 = Sociedade Limitada)",
            "porte_empresa": "Código numérico (1=Microempresa, 2=Pequena, 3=Média, 4=Grande)",
            "situacao_cadastral": "Código numérico (8=Baixada, 1=Ativa)",
            "cnae": "Código de atividade (ex: 4330404 = Atividades de apoio administrativo)",
            "qualificacao_socio": "Código numérico da qualificação"
        }
    }


@app.post("/auth/token", response_model=TokenResponse)
def get_token(request: TokenRequest):
    """
    Obter JWT token para acessar outros endpoints.

    Exemplo:
    ```
    POST /auth/token
    {
      "client_id": "snfisc",
      "client_secret": "your-secret"
    }
    ```

    Retorna:
    ```
    {
      "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
      "token_type": "bearer",
      "expires_in": 86400
    }
    ```
    """
    JWT_EXPIRATION_HOURS = int(os.getenv("JWT_EXPIRATION_HOURS", 24))
    token = create_token(request.client_id, request.client_secret)

    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=JWT_EXPIRATION_HOURS * 3600
    )


@app.get("/empresa/{cnpj_basico}")
def get_empresa(
    cnpj_basico: str,
    cnpj_ordem: str = Query("0001"),
    _: str = Depends(verify_token)
):
    """
    Get aggregated empresa + estabelecimento data by CNPJ.
    Consolidates ~8 queries into 1-2 SQL queries with JOINs.

    **Requer autenticação:** Bearer token obtido via POST /auth/token

    Returns the same structure as snfisc's buscar_empresa_por_cnpj for compatibility.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Fetch empresa with lookup joins
        cur.execute("""
            SELECT
                e.cnpj_basico,
                e.razao_social,
                e.natureza_juridica,
                e.qualificacao_responsavel,
                e.capital_social,
                e.porte_empresa,
                e.ente_federativo_responsavel
            FROM empresa e
            WHERE e.cnpj_basico = %s
        """, (cnpj_basico,))

        empresa_row = cur.fetchone()
        if not empresa_row:
            cur.close()
            raise HTTPException(
                status_code=404, detail=f"Empresa com CNPJ {cnpj_basico} não encontrada")

        empresa = {
            "razao_social": empresa_row[1],
            "natureza_juridica": _to_int(empresa_row[2]),
            "qualificacao_responsavel": _to_int(empresa_row[3]),
            "capital_social": empresa_row[4],
            "porte_empresa": _to_int(empresa_row[5]),
            "ente_federativo_responsavel": empresa_row[6],
        }

        # Resolve lookup descriptions (using cache)
        empresa["natureza_juridica_desc"] = _get_lookup_desc(
            "empresa_natureza_juridica", empresa["natureza_juridica"])
        empresa["porte_empresa_desc"] = _get_lookup_desc(
            "empresa_porte", empresa["porte_empresa"])
        empresa["qualificacao_responsavel_desc"] = _get_lookup_desc(
            "socios_qualificacao", empresa["qualificacao_responsavel"])

        # Fetch estabelecimento
        cur.execute("""
            SELECT
                cnpj_basico, cnpj_ordem, cnpj_dv,
                identificador_matriz_filial, nome_fantasia, situacao_cadastral,
                data_situacao_cadastral, motivo_situacao_cadastral,
                data_inicio_atividade, cnae_fiscal_principal, cnae_fiscal_secundaria,
                tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio,
                ddd_1, telefone_1, ddd_2, telefone_2, correio_eletronico, situacao_especial
            FROM estabelecimento
            WHERE cnpj_basico = %s AND cnpj_ordem = %s
        """, (cnpj_basico, cnpj_ordem))

        estab_row = cur.fetchone()
        if not estab_row:
            cur.close()
            raise HTTPException(
                status_code=404, detail=f"Estabelecimento {cnpj_basico}/{cnpj_ordem} não encontrado")

        estabelecimento = {
            "cnpj_basico": estab_row[0],
            "cnpj_ordem": estab_row[1],
            "cnpj_dv": estab_row[2],
            "identificador_matriz_filial": _to_int(estab_row[3]),
            "nome_fantasia": estab_row[4],
            "situacao_cadastral": _to_int(estab_row[5]),
            "data_situacao_cadastral": estab_row[6],
            "motivo_situacao_cadastral": _to_int(estab_row[7]),
            "data_inicio_atividade": estab_row[8],
            "cnae_fiscal_principal": _to_int(estab_row[9]),
            "cnae_fiscal_secundaria": estab_row[10],
            "tipo_logradouro": estab_row[11],
            "logradouro": estab_row[12],
            "numero": estab_row[13],
            "complemento": estab_row[14],
            "bairro": estab_row[15],
            "cep": estab_row[16],
            "uf": estab_row[17],
            "municipio": _to_int(estab_row[18]),
            "ddd_1": estab_row[19],
            "telefone_1": estab_row[20],
            "ddd_2": estab_row[21],
            "telefone_2": estab_row[22],
            "correio_eletronico": estab_row[23],
            "situacao_especial": estab_row[24],
        }

        # Resolve lookup descriptions (using cache)
        estabelecimento["situacao_cadastral_desc"] = _get_lookup_desc(
            "estabelecimento_situacao_cadastral", estabelecimento["situacao_cadastral"])
        estabelecimento["motivo_situacao_cadastral_desc"] = _get_lookup_desc(
            "estabelecimento_motivo", estabelecimento["motivo_situacao_cadastral"])
        estabelecimento["cnae_fiscal_principal_desc"] = _get_lookup_desc(
            "cnae", estabelecimento["cnae_fiscal_principal"])
        estabelecimento["cnae_fiscal_secundaria_desc"] = _get_lookup_desc(
            "cnae", estabelecimento["cnae_fiscal_secundaria"])
        estabelecimento["municipio_desc"] = _get_lookup_desc(
            "munic", estabelecimento["municipio"])

        cur.close()

        return {
            "empresa": empresa,
            "estabelecimento": estabelecimento,
        }

    finally:
        return_connection(conn)


@app.get("/socios/{cnpj_basico}")
def get_socios(cnpj_basico: str, _: str = Depends(verify_token)):
    """
    Get sócios/shareholders data for a CNPJ.
    Consolidates multiple queries via cache for lookups.

    **Requer autenticação:** Bearer token obtido via POST /auth/token
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                cnpj_basico, identificador_socio, nome_socio_razao_social,
                cpf_cnpj_socio, qualificacao_socio, data_entrada_sociedade,
                pais, faixa_etaria, representante_legal, nome_do_representante,
                qualificacao_representante_legal
            FROM socios
            WHERE cnpj_basico = %s
            ORDER BY identificador_socio
        """, (cnpj_basico,))

        rows = cur.fetchall()
        if not rows:
            cur.close()
            return {"socios": [], "mensagem": "sócio não encontrado"}

        socios_list = []
        for row in rows:
            socio = {
                "cnpj_basico": row[0],
                "identificador_socio": _to_int(row[1]),
                "nome_socio_razao_social": row[2],
                "cpf_cnpj_socio": row[3],
                "qualificacao_socio": _to_int(row[4]),
                "data_entrada_sociedade": row[5],
                "pais": _to_int(row[6]),
                "faixa_etaria": _to_int(row[7]),
                "representante_legal": row[8],
                "nome_do_representante": row[9],
                "qualificacao_representante_legal": _to_int(row[10]),
            }
            socio["identificador_socio_desc"] = _get_lookup_desc(
                "socios_identificador", socio["identificador_socio"])
            socio["qualificacao_socio_desc"] = _get_lookup_desc(
                "socios_qualificacao", socio["qualificacao_socio"])
            socio["pais_desc"] = _get_lookup_desc("pais", socio["pais"])
            socio["qualificacao_representante_legal_desc"] = _get_lookup_desc(
                "socios_qualificacao", socio["qualificacao_representante_legal"])
            socios_list.append(socio)

        cur.close()
        return {"socios": socios_list}

    finally:
        return_connection(conn)


@app.get("/filiais/{cnpj_basico}")
def get_filiais(cnpj_basico: str, _: str = Depends(verify_token)):
    """
    Get all estabelecimentos (matriz + filiais) for a CNPJ.

    **Requer autenticação:** Bearer token obtido via POST /auth/token
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                cnpj_basico, cnpj_ordem, cnpj_dv,
                identificador_matriz_filial, nome_fantasia, situacao_cadastral,
                data_situacao_cadastral, motivo_situacao_cadastral,
                data_inicio_atividade, cnae_fiscal_principal,
                uf, municipio
            FROM estabelecimento
            WHERE cnpj_basico = %s
            ORDER BY cnpj_ordem
        """, (cnpj_basico,))

        rows = cur.fetchall()
        if not rows:
            cur.close()
            raise HTTPException(
                status_code=404, detail=f"Nenhum estabelecimento encontrado para {cnpj_basico}")

        filiais = []
        for row in rows:
            filial = {
                "cnpj_basico": row[0],
                "cnpj_ordem": row[1],
                "cnpj_dv": row[2],
                "identificador_matriz_filial": _to_int(row[3]),
                "nome_fantasia": row[4],
                "situacao_cadastral": _to_int(row[5]),
                "data_situacao_cadastral": row[6],
                "motivo_situacao_cadastral": _to_int(row[7]),
                "data_inicio_atividade": row[8],
                "cnae_fiscal_principal": _to_int(row[9]),
                "uf": row[10],
                "municipio": _to_int(row[11]),
            }
            filial["situacao_cadastral_desc"] = _get_lookup_desc(
                "estabelecimento_situacao_cadastral", filial["situacao_cadastral"])
            filial["motivo_situacao_cadastral_desc"] = _get_lookup_desc(
                "estabelecimento_motivo", filial["motivo_situacao_cadastral"])
            filial["cnae_fiscal_principal_desc"] = _get_lookup_desc(
                "cnae", filial["cnae_fiscal_principal"])
            filial["municipio_desc"] = _get_lookup_desc(
                "munic", filial["municipio"])
            filiais.append(filial)

        cur.close()
        return {"filiais": filiais}

    finally:
        return_connection(conn)


@app.get("/tudo/{cnpj_basico}")
def get_tudo(cnpj_basico: str, cnpj_ordem: str = Query("0001"), _: str = Depends(verify_token)):
    """
    Get everything about a CNPJ: empresa + estabelecimento + socios.
    Combines endpoints above for convenience.

    **Requer autenticação:** Bearer token obtido via POST /auth/token
    """
    empresa_estab = get_empresa(cnpj_basico, cnpj_ordem)
    socios = get_socios(cnpj_basico)
    filiais = get_filiais(cnpj_basico)

    return {
        "empresa": empresa_estab["empresa"],
        "estabelecimento": empresa_estab["estabelecimento"],
        "socios": socios["socios"],
        "filiais": filiais["filiais"],
    }


@app.get("/empresas/buscar")
def buscar_empresa(
    uf: str = Query(...),
    municipio: int = Query(...),
    cnpj_basico: Optional[str] = Query(None),
    razao_nome: Optional[str] = Query(None),
    natureza_juridica: Optional[int] = Query(None),
    porte_empresa: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=1000),
    _: str = Depends(verify_token)
):
    """
    Buscar empresas por UF, município e outros critérios.
    UF e município são obrigatórios.

    Parâmetros:
    - razao_nome: busca parcial em razao_social (case-insensitive)
    - natureza_juridica: código da natureza jurídica
    - porte_empresa: código do porte da empresa

    Retorna dados básicos: cnpj_basico, razao_social, natureza_juridica, porte_empresa, municipio, uf
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        offset = (page - 1) * limit

        # Build WHERE clause for estabelecimento table
        est_where_clauses = ["uf = %s", "municipio = %s"]
        params = [uf, municipio]

        if cnpj_basico:
            est_where_clauses.append("cnpj_basico = %s")
            params.append(cnpj_basico)

        est_where_sql = " AND ".join(est_where_clauses)

        # First, get unique CNPJs from estabelecimento table
        cur.execute(f"""
            SELECT DISTINCT cnpj_basico
            FROM estabelecimento
            WHERE {est_where_sql}
            ORDER BY cnpj_basico
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        cnpj_rows = cur.fetchall()
        if not cnpj_rows:
            cur.close()
            return {"empresas": [], "total": 0, "page": page, "limit": limit}

        cnpj_list = [row[0] for row in cnpj_rows]
        cnpj_placeholders = ", ".join(["%s"] * len(cnpj_list))

        # Then, fetch empresa details with optional filters
        emp_where_clauses = [f"e.cnpj_basico IN ({cnpj_placeholders})"]
        emp_params = cnpj_list[:]

        if razao_nome:
            emp_where_clauses.append("e.razao_social ILIKE %s")
            emp_params.append(f"%{razao_nome}%")
        if natureza_juridica is not None:
            emp_where_clauses.append("e.natureza_juridica = %s")
            emp_params.append(natureza_juridica)
        if porte_empresa is not None:
            emp_where_clauses.append("e.porte_empresa = %s")
            emp_params.append(porte_empresa)

        emp_where_sql = " AND ".join(
            emp_where_clauses) if emp_where_clauses else "TRUE"

        # Get the first estabelecimento for each CNPJ to get municipio and uf
        cur.execute(f"""
            SELECT
                e.cnpj_basico,
                e.razao_social,
                e.natureza_juridica,
                e.porte_empresa,
                (SELECT DISTINCT municipio FROM estabelecimento WHERE cnpj_basico = e.cnpj_basico LIMIT 1),
                (SELECT DISTINCT uf FROM estabelecimento WHERE cnpj_basico = e.cnpj_basico LIMIT 1)
            FROM empresa e
            WHERE {emp_where_sql}
            ORDER BY e.razao_social
        """, emp_params)

        rows = cur.fetchall()
        empresas = []
        for row in rows:
            empresa = {
                "cnpj_basico": row[0],
                "razao_social": row[1],
                "natureza_juridica": row[2],
                "natureza_juridica_desc": _get_lookup_desc("empresa_natureza_juridica", row[2]),
                "porte_empresa": row[3],
                "porte_empresa_desc": _get_lookup_desc("empresa_porte", row[3]),
                "municipio": _to_int(row[4]),
                "municipio_desc": _get_lookup_desc("munic", _to_int(row[4])),
                "uf": row[5]
            }
            empresas.append(empresa)

        # Count total distinct CNPJs in estabelecimento
        cur.execute(f"""
            SELECT COUNT(DISTINCT cnpj_basico)
            FROM estabelecimento
            WHERE {est_where_sql}
        """, params)

        total = cur.fetchone()[0]
        cur.close()

        return {
            "empresas": empresas,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": (total + limit - 1) // limit
        }

    finally:
        return_connection(conn)


@app.get("/estabelecimentos/buscar")
def buscar_estabelecimento(
    uf: str = Query(...),
    municipio: int = Query(...),
    cnpj_basico: Optional[str] = Query(None),
    razao_nome: Optional[str] = Query(None),
    natureza_juridica: Optional[int] = Query(None),
    porte_empresa: Optional[int] = Query(None),
    situacao_cadastral: Optional[int] = Query(None),
    motivo_situacao_cadastral: Optional[int] = Query(None),
    data_inicio_atividade: Optional[str] = Query(None),
    cnae_fiscal_principal: Optional[int] = Query(None),
    cnae_fiscal_secundaria: Optional[int] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=1000),
    _: str = Depends(verify_token)
):
    """
    Buscar estabelecimentos por UF, município e outros critérios.
    UF e município são obrigatórios.

    Parâmetros:
    - razao_nome: busca parcial em nome_fantasia (case-insensitive)
    - natureza_juridica: código da natureza jurídica (da empresa)
    - porte_empresa: código do porte da empresa
    - motivo_situacao_cadastral: código do motivo da situação cadastral
    - data_inicio_atividade: data exata (formato YYYY-MM-DD)

    Retorna dados básicos de estabelecimentos.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        offset = (page - 1) * limit

        where_clauses = ["est.uf = %s", "est.municipio = %s"]
        params = [uf, municipio]

        if cnpj_basico:
            where_clauses.append("est.cnpj_basico = %s")
            params.append(cnpj_basico)
        if razao_nome:
            where_clauses.append("est.nome_fantasia ILIKE %s")
            params.append(f"%{razao_nome}%")
        if natureza_juridica is not None:
            where_clauses.append("e.natureza_juridica = %s")
            params.append(natureza_juridica)
        if porte_empresa is not None:
            where_clauses.append("e.porte_empresa = %s")
            params.append(porte_empresa)
        if situacao_cadastral is not None:
            where_clauses.append("est.situacao_cadastral = %s")
            params.append(situacao_cadastral)
        if motivo_situacao_cadastral is not None:
            where_clauses.append("est.motivo_situacao_cadastral = %s")
            params.append(motivo_situacao_cadastral)
        if data_inicio_atividade:
            where_clauses.append("est.data_inicio_atividade = %s")
            params.append(data_inicio_atividade)
        if cnae_fiscal_principal is not None:
            where_clauses.append("est.cnae_fiscal_principal = %s")
            params.append(cnae_fiscal_principal)
        if cnae_fiscal_secundaria is not None:
            where_clauses.append("est.cnae_fiscal_secundaria = %s")
            params.append(cnae_fiscal_secundaria)

        where_sql = " AND ".join(where_clauses)

        cur.execute(f"""
            SELECT
                est.cnpj_basico,
                est.cnpj_ordem,
                est.cnpj_dv,
                est.nome_fantasia,
                est.situacao_cadastral,
                est.data_inicio_atividade,
                est.cnae_fiscal_principal,
                est.municipio,
                est.uf,
                est.bairro,
                e.razao_social,
                e.natureza_juridica,
                e.porte_empresa
            FROM estabelecimento est
            LEFT JOIN empresa e ON est.cnpj_basico = e.cnpj_basico
            WHERE {where_sql}
            ORDER BY est.cnpj_basico, est.cnpj_ordem
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        rows = cur.fetchall()
        if not rows:
            cur.close()
            return {"estabelecimentos": [], "total": 0, "page": page, "limit": limit}

        estabelecimentos = []
        for row in rows:
            estab = {
                "cnpj_basico": row[0],
                "cnpj_ordem": row[1],
                "cnpj_dv": row[2],
                "cnpj_completo": f"{row[0]}{row[1]}{row[2]}",
                "nome_fantasia": row[3],
                "situacao_cadastral": row[4],
                "situacao_cadastral_desc": _get_lookup_desc("estabelecimento_situacao_cadastral", row[4]),
                "data_inicio_atividade": row[5],
                "cnae_fiscal_principal": row[6],
                "cnae_fiscal_principal_desc": _get_lookup_desc("cnae", row[6]),
                "municipio": row[7],
                "municipio_desc": _get_lookup_desc("munic", row[7]),
                "uf": row[8],
                "bairro": row[9],
                "razao_social": row[10],
                "natureza_juridica": _to_int(row[11]),
                "natureza_juridica_desc": _get_lookup_desc("empresa_natureza_juridica", _to_int(row[11])),
                "porte_empresa": _to_int(row[12]),
                "porte_empresa_desc": _get_lookup_desc("empresa_porte", _to_int(row[12]))
            }
            estabelecimentos.append(estab)

        cur.execute(f"""
            SELECT COUNT(*)
            FROM estabelecimento est
            LEFT JOIN empresa e ON est.cnpj_basico = e.cnpj_basico
            WHERE {where_sql}
        """, params)

        total = cur.fetchone()[0]
        cur.close()

        return {
            "estabelecimentos": estabelecimentos,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": (total + limit - 1) // limit
        }

    finally:
        return_connection(conn)


@app.get("/socios-buscar")
def buscar_socios(
    uf: str = Query(...),
    municipio: int = Query(...),
    cnpj_basico: Optional[str] = Query(None),
    nome_socio_razao_social: Optional[str] = Query(None),
    cpf_cnpj_socio: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=1000),
    _: str = Depends(verify_token)
):
    """
    Buscar sócios por UF, município e outros critérios.
    UF e município são obrigatórios.

    Parâmetros:
    - nome_socio_razao_social: busca parcial pelo nome (case-insensitive)
    - cpf_cnpj_socio: busca exata por CPF ou CNPJ

    Retorna dados básicos de sócios, incluindo informações de representante legal.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        offset = (page - 1) * limit

        where_clauses = ["est.uf = %s", "est.municipio = %s"]
        params = [uf, municipio]

        if cnpj_basico:
            where_clauses.append("s.cnpj_basico = %s")
            params.append(cnpj_basico)
        if nome_socio_razao_social:
            where_clauses.append("s.nome_socio_razao_social ILIKE %s")
            params.append(f"%{nome_socio_razao_social}%")
        if cpf_cnpj_socio:
            where_clauses.append("s.cpf_cnpj_socio ILIKE %s")
            params.append(f"%{cpf_cnpj_socio}%")

        where_sql = " AND ".join(where_clauses)

        cur.execute(f"""
            SELECT
                s.cnpj_basico,
                s.identificador_socio,
                s.nome_socio_razao_social,
                s.qualificacao_socio,
                s.representante_legal,
                s.nome_do_representante,
                s.qualificacao_representante_legal,
                est.municipio,
                est.uf
            FROM socios s
            JOIN estabelecimento est ON s.cnpj_basico = est.cnpj_basico
            WHERE {where_sql}
            ORDER BY s.cnpj_basico, s.identificador_socio
            LIMIT %s OFFSET %s
        """, params + [limit, offset])

        rows = cur.fetchall()
        if not rows:
            cur.close()
            return {"socios": [], "total": 0, "page": page, "limit": limit}

        socios = []
        for row in rows:
            socio = {
                "cnpj_basico": row[0],
                "identificador_socio": row[1],
                "identificador_socio_desc": _get_lookup_desc("socios_identificador", row[1]),
                "nome_socio_razao_social": row[2],
                "qualificacao_socio": row[3],
                "qualificacao_socio_desc": _get_lookup_desc("socios_qualificacao", row[3]),
                "representante_legal": row[4],
                "nome_do_representante": row[5],
                "qualificacao_representante_legal": _to_int(row[6]),
                "qualificacao_representante_legal_desc": _get_lookup_desc("socios_qualificacao", _to_int(row[6])),
                "municipio": row[7],
                "municipio_desc": _get_lookup_desc("munic", row[7]),
                "uf": row[8]
            }
            socios.append(socio)

        cur.execute(f"""
            SELECT COUNT(*)
            FROM socios s
            JOIN estabelecimento est ON s.cnpj_basico = est.cnpj_basico
            WHERE {where_sql}
        """, params)

        total = cur.fetchone()[0]
        cur.close()

        return {
            "socios": socios,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": (total + limit - 1) // limit
        }

    finally:
        return_connection(conn)


@app.get("/empresas/exportar-csv")
def exportar_empresas_csv(
    uf: str = Query(...),
    municipio: int = Query(...),
    cnpj_basico: Optional[str] = Query(None),
    razao_nome: Optional[str] = Query(None),
    natureza_juridica: Optional[int] = Query(None),
    porte_empresa: Optional[int] = Query(None),
    max_records: int = Query(100000, ge=1, le=500000),
    _: str = Depends(verify_token)
):
    """
    Exportar empresas em CSV (completo, sem paginação)

    Parâmetros:
    - uf, municipio: obrigatórios
    - max_records: limite máximo de registros (padrão: 100000, máx: 500000)

    Retorna: arquivo CSV com todas as empresas correspondentes aos filtros
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        est_where_clauses = ["uf = %s", "municipio = %s"]
        params = [uf, municipio]

        if cnpj_basico:
            est_where_clauses.append("cnpj_basico = %s")
            params.append(cnpj_basico)

        est_where_sql = " AND ".join(est_where_clauses)

        # Buscar CNPJs do estabelecimento
        cur.execute(f"""
            SELECT DISTINCT cnpj_basico
            FROM estabelecimento
            WHERE {est_where_sql}
            ORDER BY cnpj_basico
            LIMIT %s
        """, params + [max_records])

        cnpj_rows = cur.fetchall()
        if not cnpj_rows:
            cur.close()
            return_connection(conn)
            return StreamingResponse(
                iter(
                    [b"cnpj_basico,razao_social,natureza_juridica,porte_empresa,municipio,uf\n"]),
                media_type="text/csv",
                headers={
                    "Content-Disposition": "attachment; filename=empresas.csv"}
            )

        cnpj_list = [row[0] for row in cnpj_rows]
        cnpj_placeholders = ", ".join(["%s"] * len(cnpj_list))

        emp_where_clauses = [f"e.cnpj_basico IN ({cnpj_placeholders})"]
        emp_params = cnpj_list[:]

        if razao_nome:
            emp_where_clauses.append("e.razao_social ILIKE %s")
            emp_params.append(f"%{razao_nome}%")
        if natureza_juridica is not None:
            emp_where_clauses.append("e.natureza_juridica = %s")
            emp_params.append(natureza_juridica)
        if porte_empresa is not None:
            emp_where_clauses.append("e.porte_empresa = %s")
            emp_params.append(porte_empresa)

        emp_where_sql = " AND ".join(
            emp_where_clauses) if emp_where_clauses else "TRUE"

        # Buscar dados das empresas
        cur.execute(f"""
            SELECT
                e.cnpj_basico,
                e.razao_social,
                e.natureza_juridica,
                e.porte_empresa,
                (SELECT DISTINCT municipio FROM estabelecimento WHERE cnpj_basico = e.cnpj_basico LIMIT 1),
                (SELECT DISTINCT uf FROM estabelecimento WHERE cnpj_basico = e.cnpj_basico LIMIT 1)
            FROM empresa e
            WHERE {emp_where_sql}
            ORDER BY e.razao_social
        """, emp_params)

        rows = cur.fetchall()

        # Gerar CSV em memória
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["cnpj_basico", "razao_social",
                        "natureza_juridica", "porte_empresa", "municipio", "uf"])

        for row in rows:
            writer.writerow(row)

        cur.close()
        return_connection(conn)

        # Retornar como streaming response
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=empresas.csv"}
        )

    finally:
        if conn:
            return_connection(conn)


@app.get("/estabelecimentos/exportar-csv")
def exportar_estabelecimentos_csv(
    uf: str = Query(...),
    municipio: int = Query(...),
    cnpj_basico: Optional[str] = Query(None),
    razao_nome: Optional[str] = Query(None),
    natureza_juridica: Optional[int] = Query(None),
    porte_empresa: Optional[int] = Query(None),
    situacao_cadastral: Optional[int] = Query(None),
    motivo_situacao_cadastral: Optional[int] = Query(None),
    data_inicio_atividade: Optional[str] = Query(None),
    cnae_fiscal_principal: Optional[int] = Query(None),
    cnae_fiscal_secundaria: Optional[int] = Query(None),
    max_records: int = Query(100000, ge=1, le=500000),
    _: str = Depends(verify_token)
):
    """
    Exportar estabelecimentos em CSV (completo, sem paginação)

    Parâmetros:
    - uf, municipio: obrigatórios
    - max_records: limite máximo de registros (padrão: 100000, máx: 500000)

    Retorna: arquivo CSV com todos os estabelecimentos correspondentes aos filtros
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        where_clauses = ["est.uf = %s", "est.municipio = %s"]
        params = [uf, municipio]

        if cnpj_basico:
            where_clauses.append("est.cnpj_basico = %s")
            params.append(cnpj_basico)
        if razao_nome:
            where_clauses.append("est.nome_fantasia ILIKE %s")
            params.append(f"%{razao_nome}%")
        if natureza_juridica is not None:
            where_clauses.append("e.natureza_juridica = %s")
            params.append(natureza_juridica)
        if porte_empresa is not None:
            where_clauses.append("e.porte_empresa = %s")
            params.append(porte_empresa)
        if situacao_cadastral is not None:
            where_clauses.append("est.situacao_cadastral = %s")
            params.append(situacao_cadastral)
        if motivo_situacao_cadastral is not None:
            where_clauses.append("est.motivo_situacao_cadastral = %s")
            params.append(motivo_situacao_cadastral)
        if data_inicio_atividade:
            where_clauses.append("est.data_inicio_atividade = %s")
            params.append(data_inicio_atividade)
        if cnae_fiscal_principal is not None:
            where_clauses.append("est.cnae_fiscal_principal = %s")
            params.append(cnae_fiscal_principal)
        if cnae_fiscal_secundaria is not None:
            where_clauses.append("est.cnae_fiscal_secundaria = %s")
            params.append(cnae_fiscal_secundaria)

        where_sql = " AND ".join(where_clauses)

        cur.execute(f"""
            SELECT
                est.cnpj_basico,
                est.cnpj_ordem,
                est.cnpj_dv,
                est.nome_fantasia,
                est.situacao_cadastral,
                est.data_inicio_atividade,
                est.cnae_fiscal_principal,
                est.municipio,
                est.uf,
                e.razao_social,
                e.natureza_juridica,
                e.porte_empresa
            FROM estabelecimento est
            LEFT JOIN empresa e ON est.cnpj_basico = e.cnpj_basico
            WHERE {where_sql}
            ORDER BY est.cnpj_basico, est.cnpj_ordem
            LIMIT %s
        """, params + [max_records])

        rows = cur.fetchall()

        # Gerar CSV em memória
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["cnpj_basico", "cnpj_ordem", "cnpj_dv", "nome_fantasia", "razao_social",
                        "situacao_cadastral", "data_inicio_atividade", "cnae_fiscal_principal",
                        "natureza_juridica", "porte_empresa", "municipio", "uf"])

        for row in rows:
            writer.writerow(row)

        cur.close()
        return_connection(conn)

        # Retornar como streaming response
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=estabelecimentos.csv"}
        )

    finally:
        if conn:
            return_connection(conn)


@app.get("/socios/exportar-csv")
def exportar_socios_csv(
    uf: str = Query(...),
    municipio: int = Query(...),
    cnpj_basico: Optional[str] = Query(None),
    nome_socio_razao_social: Optional[str] = Query(None),
    cpf_cnpj_socio: Optional[str] = Query(None),
    max_records: int = Query(100000, ge=1, le=500000),
    _: str = Depends(verify_token)
):
    """
    Exportar sócios em CSV (completo, sem paginação)

    Parâmetros:
    - uf, municipio: obrigatórios
    - max_records: limite máximo de registros (padrão: 100000, máx: 500000)

    Retorna: arquivo CSV com todos os sócios correspondentes aos filtros, incluindo representante legal
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        where_clauses = ["est.uf = %s", "est.municipio = %s"]
        params = [uf, municipio]

        if cnpj_basico:
            where_clauses.append("s.cnpj_basico = %s")
            params.append(cnpj_basico)
        if nome_socio_razao_social:
            where_clauses.append("s.nome_socio_razao_social ILIKE %s")
            params.append(f"%{nome_socio_razao_social}%")
        if cpf_cnpj_socio:
            where_clauses.append("s.cpf_cnpj_socio ILIKE %s")
            params.append(f"%{cpf_cnpj_socio}%")

        where_sql = " AND ".join(where_clauses)

        cur.execute(f"""
            SELECT
                s.cnpj_basico,
                s.identificador_socio,
                s.nome_socio_razao_social,
                s.qualificacao_socio,
                s.representante_legal,
                s.nome_do_representante,
                s.qualificacao_representante_legal,
                est.municipio,
                est.uf
            FROM socios s
            JOIN estabelecimento est ON s.cnpj_basico = est.cnpj_basico
            WHERE {where_sql}
            ORDER BY s.cnpj_basico, s.identificador_socio
            LIMIT %s
        """, params + [max_records])

        rows = cur.fetchall()

        # Gerar CSV em memória
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["cnpj_basico", "identificador_socio", "nome_socio_razao_social",
                        "qualificacao_socio", "representante_legal", "nome_do_representante",
                        "qualificacao_representante_legal", "municipio", "uf"])

        for row in rows:
            writer.writerow(row)

        cur.close()
        return_connection(conn)

        # Retornar como streaming response
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=socios.csv"}
        )

    finally:
        if conn:
            return_connection(conn)


@app.get("/atualizar/verificar")
def verificar_atualizacao(_: str = Depends(verify_token)):
    """
    Verifica se há nova versão do banco de dados CNPJ disponível no site da RFB.

    Faz requisições WebDAV ao servidor da Receita Federal para descobrir:
    - Última versão disponível (AAAA-MM)
    - Comparação com a versão atual do banco local
    - Se há atualizações pendentes

    Resposta:
    - msg: mensagem descritiva (pode conter emoji)
    - update: true se há nova versão, false se está atualizado
    - ano, mes: versão disponível (se houver)
    - versao_disponivel: formato MM/YYYY (se houver)
    - versao_atual: versão do banco local (se existir)
    - data_ultima_atualizacao: data da última atualização (se existir)

    Exemplos:
    - Atualizado: {"msg": "✅ Banco está atualizado...", "update": false, ...}
    - Há novidade: {"msg": "🆕 Nova versão disponível: 01/2025", "update": true, ...}
    """
    try:
        # Chamada à função completa do snfisc
        result = verificar_nova_atualizacao()

        # Mapear para o formato esperado
        return {
            "msg": result.get("msg", ""),
            "atualizado": not result.get("update", False),
            "nova_versao_disponivel": result.get("update", False),
            "ano": result.get("ano"),
            "mes": result.get("mes"),
            "versao_disponivel": result.get("versao_disponivel"),
            "versao_atual": result.get("versao_atual"),
            "data_ultima_atualizacao": result.get("data_ultima_atualizacao"),
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao verificar atualização: {str(e)}"
        )


@app.post("/atualizar/versao-disponivel")
def popular_versao_disponivel(request: VersaoDisponivelRequest, _: str = Depends(verify_token)):
    """
    Registra manualmente a versão mais recente conhecida como disponível no site da RFB.

    Use este endpoint para popular a checagem local rápida (GET /atualizar/verificar-local)
    sem que ela precise sair para a internet a cada consulta. Você mesmo decide quando
    checar o site da RFB (manualmente ou via /atualizar/verificar) e grava o resultado aqui.

    Corpo (JSON):
    - ano, mes: versão detectada como disponível
    - origem: identifica quem populou (padrão: "manual")
    - observacao: texto livre opcional

    Chamadas repetidas com o mesmo ano/mes apenas atualizam data_verificacao (upsert).
    """
    if not (1 <= request.mes <= 12):
        raise HTTPException(status_code=400, detail="mes deve estar entre 1 e 12")

    try:
        resultado = registrar_versao_disponivel(
            request.ano, request.mes, request.origem, request.observacao)
        return {
            "status": "ok",
            "mensagem": f"Versão disponível registrada: {resultado['mes']:02d}/{resultado['ano']}",
            "ano": resultado["ano"],
            "mes": resultado["mes"],
            "data_verificacao": str(resultado["data_verificacao"]),
            "origem": resultado["origem"],
            "observacao": resultado["observacao"],
        }
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/atualizar/verificar-local")
def verificar_atualizacao_local_endpoint(_: str = Depends(verify_token)):
    """
    Verifica se há nova versão pendente usando SOMENTE dados locais do banco.

    Compara info_dados (versão atualmente carregada) com rfb_versao_disponivel
    (última versão conhecida como disponível, populada via POST /atualizar/versao-disponivel).
    Não faz nenhuma requisição ao site da RFB — ideal para outros sistemas consultarem
    com frequência sem o custo/latência do scraping online.

    Resposta: mesmo formato de /atualizar/verificar, mais versao_disponivel_conhecida
    e data_verificacao_disponivel.
    """
    try:
        result = verificar_atualizacao_local()
        return {
            "msg": result.get("msg", ""),
            "atualizado": not result.get("update", False),
            "nova_versao_disponivel": result.get("update", False),
            "ano": result.get("ano"),
            "mes": result.get("mes"),
            "versao_atual": result.get("versao_atual"),
            "data_ultima_atualizacao": result.get("data_ultima_atualizacao"),
            "versao_disponivel_conhecida": result.get("versao_disponivel_conhecida"),
            "data_verificacao_disponivel": result.get("data_verificacao_disponivel"),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao verificar atualização local: {str(e)}"
        )


def _executar_etl_background():
    """
    Executa o ETL em background
    """
    try:
        # Caminho do script ETL
        script_path = os.path.join(
            os.path.dirname(__file__), "..", "etl_rfb2.py")

        # Executar o script
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=3600  # 1 hora de timeout
        )

        if result.returncode != 0:
            with open("logs/etl_error.txt", "a") as f:
                f.write(
                    f"\n[{datetime.now()}] Erro no ETL:\n{result.stderr}\n")
    except Exception as e:
        with open("logs/etl_error.txt", "a") as f:
            f.write(
                f"\n[{datetime.now()}] Exceção ao executar ETL: {str(e)}\n")


@app.post("/atualizar/iniciar")
def iniciar_atualizacao(background_tasks: BackgroundTasks, _: str = Depends(verify_token)):
    """
    Inicia o processo de atualização do banco de dados RFB.

    **Atenção:**
    - Este endpoint executa o ETL completo (pode demorar várias horas)
    - Requer autenticação JWT
    - Retorna imediatamente (executa em background)
    - Monitore o arquivo de log: `logs/etl_rfb_dados_log.txt`

    Resposta:
    - status: "iniciado" ou "erro"
    - mensagem: descrição do status
    """

    # Verificar se o arquivo ETL existe
    script_path = os.path.join(os.path.dirname(__file__), "..", "etl_rfb2.py")
    if not os.path.exists(script_path):
        raise HTTPException(
            status_code=404,
            detail=f"Script ETL não encontrado: {script_path}"
        )

    # Verificar se já existe uma atualização em andamento (simples check)
    # Em um caso real, você usaria um lock file ou banco de dados
    log_file = "logs/etl_rfb_dados_log.txt"

    # Adicionar a tarefa ao background
    background_tasks.add_task(_executar_etl_background)

    return {
        "status": "iniciado",
        "mensagem": "Atualização iniciada em background. Verifique o arquivo de log: logs/etl_rfb_dados_log.txt",
        "log_file": log_file,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/atualizar/status")
def obter_status_atualizacao(_: str = Depends(verify_token)):
    """
    Retorna o status da última atualização realizada no banco.

    Resposta:
    - ano, mes: versão do banco
    - data_atualizacao: quando foi feita a última atualização
    - dias_desde_atualizacao: quantos dias passaram desde a última atualização
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

        if not resultado:
            return {
                "status": "vazio",
                "mensagem": "Nenhum registro de atualização. Execute o ETL para carregar os dados."
            }

        ano, mes, data_atualizacao = resultado
        data_obj = data_atualizacao if hasattr(
            data_atualizacao, 'date') else datetime.strptime(str(data_atualizacao), '%Y-%m-%d')
        dias = (datetime.now().date() - data_obj.date()
                ).days if hasattr(data_obj, 'date') else 0

        return {
            "status": "atualizado",
            "versao": f"{mes:02d}/{ano}",
            "ano": ano,
            "mes": mes,
            "data_atualizacao": str(data_atualizacao),
            "dias_desde_atualizacao": dias
        }

    except Exception as e:
        return_connection(conn)
        raise HTTPException(
            status_code=500, detail=f"Erro ao obter status: {str(e)}")


@app.get("/referencia/portes")
def get_portes_empresa(_: str = Depends(verify_token)):
    """
    Retorna lista de portes de empresa disponíveis.

    **Requer autenticação:** Bearer token obtido via POST /auth/token
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT codigo, descricao FROM empresa_porte ORDER BY codigo")
        rows = cur.fetchall()
        cur.close()

        portes = [
            {"codigo": row[0], "descricao": row[1]}
            for row in rows
        ]
        return {"portes": portes}
    finally:
        return_connection(conn)


@app.get("/referencia/naturezas-juridicas")
def get_naturezas_juridicas(_: str = Depends(verify_token)):
    """
    Retorna lista de naturezas jurídicas disponíveis.

    **Requer autenticação:** Bearer token obtido via POST /auth/token
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT codigo, descricao FROM empresa_natureza_juridica ORDER BY codigo")
        rows = cur.fetchall()
        cur.close()

        naturezas = [
            {"codigo": row[0], "descricao": row[1]}
            for row in rows
        ]
        return {"naturezas_juridicas": naturezas}
    finally:
        return_connection(conn)


@app.get("/referencia/situacoes-cadastrais")
def get_situacoes_cadastrais(_: str = Depends(verify_token)):
    """
    Retorna lista de situações cadastrais disponíveis.

    **Requer autenticação:** Bearer token obtido via POST /auth/token
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT codigo, descricao FROM estabelecimento_situacao_cadastral ORDER BY codigo")
        rows = cur.fetchall()
        cur.close()

        situacoes = [
            {"codigo": row[0], "descricao": row[1]}
            for row in rows
        ]
        return {"situacoes_cadastrais": situacoes}
    finally:
        return_connection(conn)


@app.get("/referencia/motivos-situacao")
def get_motivos_situacao(_: str = Depends(verify_token)):
    """
    Retorna lista de motivos de situação cadastral disponíveis.

    **Requer autenticação:** Bearer token obtido via POST /auth/token
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT codigo, descricao FROM estabelecimento_motivo ORDER BY codigo")
        rows = cur.fetchall()
        cur.close()

        motivos = [
            {"codigo": row[0], "descricao": row[1]}
            for row in rows
        ]
        return {"motivos": motivos}
    finally:
        return_connection(conn)


@app.get("/referencia/portes/{codigo}")
def get_porte_por_codigo(codigo: int, _: str = Depends(verify_token)):
    """Retorna descrição de um porte de empresa pelo código."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT codigo, descricao FROM empresa_porte WHERE codigo = %s", (codigo,))
        row = cur.fetchone()
        cur.close()
        if row:
            return {"codigo": row[0], "descricao": row[1]}
        raise HTTPException(status_code=404, detail="Porte não encontrado")
    finally:
        return_connection(conn)


@app.get("/referencia/naturezas-juridicas/{codigo}")
def get_natureza_juridica_por_codigo(codigo: int, _: str = Depends(verify_token)):
    """Retorna descrição de uma natureza jurídica pelo código."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT codigo, descricao FROM empresa_natureza_juridica WHERE codigo = %s", (codigo,))
        row = cur.fetchone()
        cur.close()
        if row:
            return {"codigo": row[0], "descricao": row[1]}
        raise HTTPException(status_code=404, detail="Natureza jurídica não encontrada")
    finally:
        return_connection(conn)


@app.get("/referencia/situacoes-cadastrais/{codigo}")
def get_situacao_cadastral_por_codigo(codigo: int, _: str = Depends(verify_token)):
    """Retorna descrição de uma situação cadastral pelo código."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT codigo, descricao FROM estabelecimento_situacao_cadastral WHERE codigo = %s", (codigo,))
        row = cur.fetchone()
        cur.close()
        if row:
            return {"codigo": row[0], "descricao": row[1]}
        raise HTTPException(status_code=404, detail="Situação cadastral não encontrada")
    finally:
        return_connection(conn)


@app.get("/referencia/motivos-situacao/{codigo}")
def get_motivo_situacao_por_codigo(codigo: int, _: str = Depends(verify_token)):
    """Retorna descrição de um motivo de situação cadastral pelo código."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT codigo, descricao FROM estabelecimento_motivo WHERE codigo = %s", (codigo,))
        row = cur.fetchone()
        cur.close()
        if row:
            return {"codigo": row[0], "descricao": row[1]}
        raise HTTPException(status_code=404, detail="Motivo não encontrado")
    finally:
        return_connection(conn)


@app.on_event("startup")
def startup_event():
    garantir_tabela_versao_disponivel()


@app.on_event("shutdown")
def shutdown_event():
    close_all_connections()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
