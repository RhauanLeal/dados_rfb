"""
Módulo para enviar notificações por e-mail usando Gmail.
Utiliza App Passwords para autenticação segura.
"""

import os
import smtplib
import logging
from email.mime.text import MIMEText
from datetime import datetime
from dotenv import load_dotenv
import pathlib

logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
# Procurar em /dados_rfb_env/.env (produção) ou .env local (desenvolvimento)
_base_dir = pathlib.Path(__file__).parent
_env_path_parent = _base_dir.parent / "dados_rfb_env" / ".env"
_env_path_local = _base_dir / ".env"

if _env_path_parent.exists():
    load_dotenv(str(_env_path_parent))
elif _env_path_local.exists():
    load_dotenv(str(_env_path_local))

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_PASSWORD = os.getenv("GMAIL_PASSWORD")
GMAIL_FROM = os.getenv("GMAIL_FROM", GMAIL_USER)
GMAIL_TO = os.getenv("GMAIL_TO", GMAIL_USER)
GMAIL_ENABLED = GMAIL_USER and GMAIL_PASSWORD


def enviar_email_erro(titulo: str, mensagem: str, detalhes: str = "", rastreamento: str = ""):
    """
    Envia e-mail de erro para o usuário.

    Args:
        titulo: Título do e-mail (ex: "ETL Falhou - Erro no TRUNCATE")
        mensagem: Mensagem principal do erro
        detalhes: Detalhes adicionais do erro
        rastreamento: Stack trace completo (opcional)
    """
    if not GMAIL_ENABLED:
        logger.warning("AVISO: E-mail nao configurado. Pulando notificacao.")
        return False

    try:
        # Criar mensagem simples
        corpo = f"""
ERRO NO ETL
{'='*60}
Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Titulo: {titulo}

Mensagem:
{mensagem}

Detalhes:
{detalhes if detalhes else '(Sem detalhes adicionais)'}

{f'Stack Trace:{chr(10)}{rastreamento}' if rastreamento else ''}

{'='*60}
Por favor, verifique o servidor e os logs do ETL.
        """

        msg = MIMEText(corpo)
        msg["Subject"] = f"ERRO - {titulo}"
        msg["From"] = GMAIL_FROM
        msg["To"] = GMAIL_TO

        # Enviar e-mail
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.send_message(msg)

        logger.info(f"OK: E-mail enviado para {GMAIL_TO}")
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error("ERRO: Falha na autenticacao Gmail. Verifique GMAIL_USER e GMAIL_PASSWORD")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"ERRO SMTP: {e}")
        return False
    except Exception as e:
        logger.error(f"ERRO: {e}")
        return False


def enviar_email_sucesso(titulo: str, mensagem: str = "", detalhes: str = ""):
    """
    Envia e-mail de sucesso (opcional, para alertas de conclusão).

    Args:
        titulo: Título do e-mail (ex: "ETL Completado com Sucesso")
        mensagem: Mensagem adicional
        detalhes: Detalhes da execução
    """
    if not GMAIL_ENABLED:
        return False

    try:
        msg = MIMEText(
            f"SUCESSO - {titulo}\n"
            f"Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"{mensagem}\n\n"
            f"Detalhes:\n{detalhes}\n\n"
            f"---\n"
            f"E-mail automático do sistema ETL"
        )
        msg["Subject"] = f"SUCESSO - {titulo}"
        msg["From"] = GMAIL_FROM
        msg["To"] = GMAIL_TO

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.send_message(msg)

        logger.info(f"✅ E-mail de sucesso enviado para {GMAIL_TO}")
        return True

    except Exception as e:
        logger.error(f"❌ Erro ao enviar e-mail de sucesso: {e}", exc_info=True)
        return False


def decorador_com_notificacao(func):
    """
    Decorator que envolve uma função e envia e-mail se houver exceção.
    Útil para envolver etl_process() ou outras funções críticas.
    """
    def wrapper(*args, **kwargs):
        try:
            resultado = func(*args, **kwargs)
            return resultado
        except Exception as e:
            import traceback

            titulo = f"Erro em {func.__name__}"
            mensagem = str(e)
            rastreamento = traceback.format_exc()

            logger.error(f"🚨 Erro capturado em {func.__name__}: {e}")
            enviar_email_erro(titulo, mensagem, rastreamento=rastreamento)

            # Re-lançar a exceção para que o ETL trate normalmente
            raise

    return wrapper
