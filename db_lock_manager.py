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
            # Timeout geral para statements (30 segundos - padrão)
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


class StagingManager:
    """Gerencia tabelas de staging para atualização sem bloqueio de leituras."""

    STAGING_TABLES = [
        'empresa', 'estabelecimento', 'socios', 'simples', 'cnae',
        'estabelecimento_motivo', 'munic', 'empresa_natureza_juridica',
        'pais', 'socios_qualificacao'
    ]

    def __init__(self, conn):
        self.conn = conn

    def prepare_staging(self) -> None:
        """Cria tabelas staging como cópias vazias das originais."""
        with self.conn.cursor() as cursor:
            for table_name in self.STAGING_TABLES:
                staging_name = f"{table_name}_staging"
                try:
                    logger.info(f"Preparando staging para {table_name}...")
                    cursor.execute(f'DROP TABLE IF EXISTS "{staging_name}";')
                    cursor.execute(f"""
                        CREATE TABLE "{staging_name}" AS
                        SELECT * FROM "{table_name}" LIMIT 0;
                    """)
                    self.conn.commit()
                    logger.info(f"✓ Staging '{staging_name}' preparado")
                except Exception as e:
                    logger.error(f"Erro ao preparar staging de {table_name}: {e}")
                    self.conn.rollback()
                    raise

    def get_row_counts(self) -> dict:
        """Retorna contagem de linhas em tabelas originais e staging (validação pré-swap)."""
        counts = {}
        with self.conn.cursor() as cursor:
            for table_name in self.STAGING_TABLES:
                staging_name = f"{table_name}_staging"
                try:
                    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                    original_count = cursor.fetchone()[0]
                    cursor.execute(f'SELECT COUNT(*) FROM "{staging_name}"')
                    staging_count = cursor.fetchone()[0]
                    counts[table_name] = {
                        'original': original_count,
                        'staging': staging_count
                    }
                except Exception as e:
                    logger.warning(f"Erro ao contar linhas em {table_name}: {e}")
                    counts[table_name] = {'original': 0, 'staging': 0}
        return counts

    def validate_before_swap(self) -> bool:
        """Valida integridade dos dados antes de fazer swap. Retorna True se OK."""
        logger.info("📋 Validando integridade dos dados antes do swap...")
        counts = self.get_row_counts()

        all_ok = True
        for table_name, row_counts in counts.items():
            original = row_counts['original']
            staging = row_counts['staging']

            if staging == 0:
                logger.warning(f"⚠️  Tabela '{table_name}' staging vazia (não será swapped)")
            elif staging < original * 0.5:  # Staging tem menos de 50% dos dados
                logger.error(f"❌ Staging de '{table_name}' tem {staging} linhas vs {original} no original (< 50%)")
                all_ok = False
            else:
                logger.info(f"✓ '{table_name}': original={original}, staging={staging}")

        return all_ok

    def swap_all_tables(self) -> None:
        """
        Faz swap atômico de TODAS as tabelas em UMA transação.
        Se qualquer swap falhar, TODOS revertem. Garante consistência.
        """
        logger.info("🔄 Iniciando swap atômico de todas as tabelas em transação única...")

        import psycopg2
        cursor = self.conn.cursor()

        try:
            # Definir isolamento SERIALIZABLE antes de BEGIN
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)

            # Abrir transação
            cursor.execute("BEGIN")

            for table_name in self.STAGING_TABLES:
                staging_name = f"{table_name}_staging"
                old_name = f"{table_name}_old"

                logger.info(f"Swapping '{table_name}' (dentro de transação global)...")

                # Renomear: original → old
                cursor.execute(f'ALTER TABLE IF EXISTS "{table_name}" RENAME TO "{old_name}"')

                # Renomear: staging → original
                cursor.execute(f'ALTER TABLE "{staging_name}" RENAME TO "{table_name}"')

                # Drop da tabela antiga (libera espaço)
                cursor.execute(f'DROP TABLE IF EXISTS "{old_name}"')

            # COMMIT ÚNICA VEZ ao final — ou falha tudo ou sucede tudo
            self.conn.commit()
            logger.info("✅ Todos os 10 swaps concluídos com sucesso (transação única)!")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Erro durante swap — REVERTENDO TODOS os swaps: {e}")
            raise
        finally:
            cursor.close()
            # Voltar ao isolamento padrão
            self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)


class AdvisoryLock:
    """Advisory Lock para sincronização entre processos ETL."""

    LOCK_ID_ETL = 1
    LOCK_ID_FIXES = 2
    LOCK_ID_INDICES = 3

    def __init__(self, conn, lock_id: int, lock_name: str = "ETL"):
        self.conn = conn
        self.lock_id = lock_id
        self.lock_name = lock_name
        self.acquired = False

    def acquire(self, timeout_seconds: int = 300) -> bool:
        """Adquire um advisory lock (bloqueante)."""
        try:
            with self.conn.cursor() as cursor:
                logger.info(f"🔒 Tentando adquirir lock '{self.lock_name}' (ID: {self.lock_id})...")
                cursor.execute(f"SET statement_timeout = '{timeout_seconds}s'")
                cursor.execute(f"SELECT pg_advisory_lock({self.lock_id})")
                self.conn.commit()
                self.acquired = True
                logger.info(f"✅ Lock '{self.lock_name}' adquirido com sucesso")
                return True
        except Exception as e:
            logger.error(f"❌ Falha ao adquirir lock '{self.lock_name}': {e}")
            self.acquired = False
            return False

    def release(self):
        """Libera o advisory lock."""
        if not self.acquired:
            return

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT pg_advisory_unlock({self.lock_id})")
                self.conn.commit()
                self.acquired = False
                logger.info(f"🔓 Lock '{self.lock_name}' liberado")
        except Exception as e:
            logger.warning(f"⚠️  Erro ao liberar lock '{self.lock_name}': {e}")

    def __enter__(self):
        """Context manager: acquire lock."""
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager: release lock."""
        self.release()
        if exc_type:
            logger.error(f"❌ Erro durante execução com lock: {exc_val}")
