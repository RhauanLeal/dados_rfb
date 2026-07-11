"""
Gerenciador de locks PostgreSQL para evitar travamentos em operações críticas.
Implementa timeout, retry automático, monitoramento e diagnóstico de locks.
"""

import time
import logging
import psycopg2
from functools import wraps
from typing import Optional

logger = logging.getLogger(__name__)


class LockManager:
    """Gerenciador de locks PostgreSQL para operações críticas (TRUNCATE, etc)."""

    def __init__(self, conn, timeout_seconds=30):
        self.conn = conn
        self.timeout_seconds = timeout_seconds

    def _safe_rollback(self) -> None:
        """Executa rollback de forma segura, capturando qualquer erro."""
        try:
            self.conn.rollback()
            logger.debug("✓ Rollback executado com sucesso")
        except Exception as e:
            logger.warning(f"⚠️  Erro ao fazer rollback (continuando): {type(e).__name__}: {e}")

    def kill_blocking_sessions(self, table_name: str) -> int:
        """
        Sem privilégio SUPERUSER, apenas faz diagnóstico de sessões bloqueadoras.
        O timeout (30s) vai cuidar da falha automática.
        Retry automático (3x) vai tentar novamente.
        """
        logger.info(f"ℹ️  Sem privilégio SUPERUSER, realizando diagnóstico de locks em {table_name}")
        logger.info(f"    Timeout configurado em {self.timeout_seconds}s + retry automático (3x)")

        # Apenas diagnosticar, não tentar matar
        self.diagnose_locks(table_name)
        return 0

    def check_locks(self, table_name: str) -> bool:
        """Verifica se há locks ativos na tabela."""
        query = """
        SELECT COUNT(*) FROM pg_stat_activity
        WHERE query ILIKE %s AND state != 'idle'
        """

        try:
            # Limpar transação abortada se houver
            self._safe_rollback()

            with self.conn.cursor() as cursor:
                cursor.execute(query, (f'%{table_name}%',))
                count = cursor.fetchone()[0]
                return count > 0
        except Exception as e:
            logger.warning(f"Erro ao verificar locks na tabela {table_name}: {e}")
            return False

    def truncate_with_timeout(self, table_name: str) -> None:
        """TRUNCATE com timeout e retry automático (3 tentativas)."""
        max_attempts = 3
        wait_time = 2

        # Limpar qualquer transação abortada anterior
        try:
            self.conn.rollback()
            logger.info(f"✓ Transação anterior limpa")
        except Exception as e:
            logger.warning(f"⚠️  Erro ao fazer rollback: {e}. Reconectando...")
            # Se rollback falhar, a conexão pode estar em estado inválido
            # Fechar e obter uma nova conexão
            try:
                self.conn.close()
            except:
                pass
            raise psycopg2.OperationalError("Conexão corrompida, necessário reconectar")

        for attempt in range(1, max_attempts + 1):
            try:
                # Reabrir transação e limpar o estado de erro
                self.conn.reset()

                with self.conn.cursor() as cursor:
                    # Definir timeout em uma transação separada/limpa
                    cursor.execute(f"SET statement_timeout = '{self.timeout_seconds}s'")

                    # Executar TRUNCATE
                    cursor.execute(f'TRUNCATE TABLE "{table_name}" CASCADE')
                    self.conn.commit()
                    logger.info(f"✓ {table_name} truncada com sucesso")
                    return

            except psycopg2.errors.LockNotAvailable as e:
                logger.warning(f"🔒 Lock não disponível na tentativa {attempt}/{max_attempts}")
                self._safe_rollback()
                if attempt < max_attempts:
                    logger.info(f"   Aguardando {wait_time}s antes de retry...")
                    time.sleep(wait_time)
                    wait_time *= 2
                else:
                    raise

            except (psycopg2.ProgrammingError, psycopg2.errors.AdminShutdown) as e:
                error_str = str(e).lower()
                if "timeout" in error_str or "lock" in error_str:
                    logger.warning(f"⏱️  Timeout/Lock na tentativa {attempt}/{max_attempts}")
                    self._safe_rollback()
                    if attempt < max_attempts:
                        logger.info(f"   Aguardando {wait_time}s antes de retry...")
                        time.sleep(wait_time)
                        wait_time *= 2
                    else:
                        raise
                else:
                    self._safe_rollback()
                    raise
            except psycopg2.errors.InFailedSqlTransaction as e:
                logger.warning(f"❌ Transação abortada na tentativa {attempt}/{max_attempts}: {e}")
                self._safe_rollback()
                if attempt < max_attempts:
                    logger.info(f"   Aguardando {wait_time}s antes de retry...")
                    time.sleep(wait_time)
                    wait_time *= 2
                else:
                    raise
            except Exception as e:
                logger.error(f"❌ Erro inesperado na tentativa {attempt}/{max_attempts}: {e}")
                self._safe_rollback()
                raise

    def diagnose_locks(self, table_name: str) -> dict:
        """
        Função de diagnóstico para locks em caso de erro.
        Retorna dict com PIDs e informações das sessões bloqueadoras.
        """
        logger.info(f"\n📊 === DIAGNÓSTICO DE LOCKS: {table_name} ===")

        query = """
        SELECT
            pid,
            usename,
            application_name,
            state,
            query,
            EXTRACT(EPOCH FROM (NOW() - query_start)) as duration_sec
        FROM pg_stat_activity
        WHERE query ILIKE %s
        ORDER BY query_start DESC
        LIMIT 10
        """

        resultado = {
            "pids": [],
            "sessoes": []
        }

        try:
            # Limpar transação abortada se houver
            self._safe_rollback()

            with self.conn.cursor() as cursor:
                cursor.execute(query, (f'%{table_name}%',))
                rows = cursor.fetchall()

                if rows:
                    logger.info(f"Sessões ativas relacionadas a {table_name}:")
                    for row in rows:
                        pid, user, app, state, query_text, duration = row
                        logger.info(f"  PID: {pid} | User: {user} | State: {state} | "
                                   f"Duration: {duration:.1f}s | App: {app}")

                        resultado["pids"].append(pid)
                        resultado["sessoes"].append({
                            "pid": pid,
                            "user": user,
                            "state": state,
                            "duration": duration
                        })
                else:
                    logger.info(f"Nenhuma sessão ativa encontrada para {table_name}")
        except Exception as e:
            logger.warning(f"Erro ao diagnosticar locks: {e}")

        logger.info("🔒 Para matar manualmente uma sessão:")
        logger.info("   SELECT pg_terminate_backend(PID);")
        logger.info("=" * 60 + "\n")

        return resultado


def configure_connection_timeouts(conn):
    """Configura timeouts para evitar travamentos indefinidos."""
    try:
        # Limpar transação anterior se houver
        try:
            conn.rollback()
            logger.debug("Rollback anterior executado")
        except Exception as e:
            logger.debug(f"Nenhuma transação anterior para limpar: {e}")

        with conn.cursor() as cursor:
            # Timeout geral para statements (30 segundos)
            cursor.execute("SET statement_timeout = '30s'")

            # Timeout para lock (5 segundos)
            cursor.execute("SET lock_timeout = '5s'")

            conn.commit()

        logger.info("✓ Timeouts configurados: statement=30s, lock=5s")
    except Exception as e:
        logger.warning(f"⚠️  Erro ao configurar timeouts: {e}")
        try:
            conn.rollback()
        except Exception as rb_err:
            logger.debug(f"Erro ao fazer rollback após falha: {rb_err}")


def retry_on_lock(max_attempts=3, initial_wait=2):
    """Decorator para retry automático em operações de lock."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            wait_time = initial_wait
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.ProgrammingError) as e:
                    error_msg = str(e).lower()
                    if "lock" not in error_msg and "timeout" not in error_msg:
                        raise

                    if attempt == max_attempts:
                        logger.error(f"✗ Falha após {max_attempts} tentativas: {e}")
                        raise

                    logger.warning(f"🔄 Tentativa {attempt}/{max_attempts} falhou. "
                                  f"Aguardando {wait_time}s...")
                    time.sleep(wait_time)
                    wait_time *= 2

        return wrapper
    return decorator
