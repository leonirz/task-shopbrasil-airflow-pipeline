import logging
from collections import defaultdict
from typing import Any

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from validar_produtos_operator import ValidarProdutosOperator

logger = logging.getLogger(__name__)

FAKESTORE_PRODUCTS_URL = "https://fakestoreapi.com/products"
POSTGRES_CONN_ID = "postgres_shopbrasil"
ECOMMERCE_POOL = "ecommerce_pool"


# ---------------------------------------------------------------------------
# Lifecycle Callbacks (Observability & Simulated Alerting)
# ---------------------------------------------------------------------------
def on_failure_alert_callback(context: dict[str, Any]) -> None:
    """Simulates sending a critical alert when a task fails permanently."""
    ti = context.get("task_instance")
    exception = context.get("exception")
    logger.error(
        "[ALERTA CRÍTICO - SHOPBRASIL] Falha definitiva na task '%s' (DAG: %s | Run: %s). Erro: %s",
        ti.task_id if ti else "unknown",
        ti.dag_id if ti else "unknown",
        context.get("run_id"),
        exception,
    )


def on_retry_log_callback(context: dict[str, Any]) -> None:
    """Logs retry attempts caused by transient API or network instability."""
    ti = context.get("task_instance")
    logger.warning(
        "[RETRY - SHOPBRASIL] Tentativa #%s falhou na task '%s'. Aguardando backoff exponencial...",
        ti.try_number if ti else "?",
        ti.task_id if ti else "unknown",
    )


def on_success_log_callback(context: dict[str, Any]) -> None:
    """Logs successful completion of critical ingestion task."""
    ti = context.get("task_instance")
    logger.info(
        "[SUCESSO - SHOPBRASIL] Task '%s' concluída com êxito na execução %s.",
        ti.task_id if ti else "unknown",
        context.get("run_id"),
    )


# ---------------------------------------------------------------------------
# Definições da nossa querida DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="shopbrasil_pricing_pipeline",
    description="Pipeline diário de inteligência de preços por categoria da ShopBrasil.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    tags=["shopbrasil", "pricing", "etl", "postgres"],
)
def shopbrasil_pricing_pipeline() -> None:
    # =======================================================================
    # TaskGroup 1: Ingestão Resiliente & Validação (Topologia Linear)
    # =======================================================================
    with TaskGroup(
        group_id="ingestao",
        tooltip="Coleta resiliente na FakeStore API e validação de schema",
    ) as tg_ingestao:

        @task(
            task_id="buscar_produtos",
            retries=3,
            retry_delay=pendulum.duration(seconds=10),
            retry_exponential_backoff=True,
            max_retry_delay=pendulum.duration(minutes=5),
            on_failure_callback=on_failure_alert_callback,
            on_retry_callback=on_retry_log_callback,
            on_success_callback=on_success_log_callback,
        )
        def buscar_produtos() -> list[dict[str, Any]]:
            """Fetches product catalog from FakeStore API with explicit error handling."""
            try:
                logger.info("Requesting product catalog from %s", FAKESTORE_PRODUCTS_URL)
                response = requests.get(FAKESTORE_PRODUCTS_URL, timeout=15)
                response.raise_for_status()
                produtos = response.json()
                logger.info("Successfully fetched %d raw products.", len(produtos))
                return produtos
            except requests.RequestException as exc:
                logger.error("HTTP/Network error while fetching FakeStore API: %s", exc)
                raise
            except ValueError as exc:
                logger.error("Invalid JSON payload returned by FakeStore API: %s", exc)
                raise

        produtos_brutos = buscar_produtos()

        validar_produtos = ValidarProdutosOperator(
            task_id="validar_produtos_schema",
            task_id_origem="ingestao.buscar_produtos",
        )

        produtos_brutos >> validar_produtos

    # =======================================================================
    # TaskGroup 2: Análise Paralela & Persistência Idempotente (Fan-Out / Fan-In)
    # =======================================================================
    with TaskGroup(
        group_id="analise",
        tooltip="Agrupamento dinâmico por categoria, cálculo paralelo e carga no PostgreSQL",
    ) as tg_analise:

        @task(task_id="agrupar_por_categoria")
        def agrupar_por_categoria(
            produtos_validados: list[dict[str, Any]],
        ) -> list[dict[str, Any]]:
            """Groups validated prices dynamically by category to support fan-out mapping."""
            grouped: dict[str, list[float]] = defaultdict(list)
            for item in produtos_validados:
                grouped[item["category"]].append(item["price"])

            payload_categorias = [
                {"categoria": categoria, "precos": precos}
                for categoria, precos in sorted(grouped.items())
            ]
            logger.info(
                "Discovered %d categories dynamically: %s",
                len(payload_categorias),
                [c["categoria"] for c in payload_categorias],
            )
            return payload_categorias

        @task(
            task_id="calcular_metricas_categoria",
            pool=ECOMMERCE_POOL,
        )
        def calcular_metricas_categoria(
            dados_categoria: dict[str, Any],
        ) -> dict[str, Any]:
            """Calculates min, max, average price, and item count for a single category."""
            categoria = dados_categoria["categoria"]
            precos = dados_categoria["precos"]

            qtd_produtos = len(precos)
            preco_medio = round(sum(precos) / qtd_produtos, 2)
            preco_minimo = round(min(precos), 2)
            preco_maximo = round(max(precos), 2)

            metrica = {
                "categoria": categoria,
                "preco_medio": preco_medio,
                "preco_minimo": preco_minimo,
                "preco_maximo": preco_maximo,
                "qtd_produtos": qtd_produtos,
            }
            logger.info("Computed metrics for category '%s': %s", categoria, metrica)
            return metrica

        @task(task_id="salvar_postgresql")
        def salvar_postgresql(
            metricas_consolidadas: list[dict[str, Any]],
            **context: Any,
        ) -> None:
            """
            Persists category metrics idempotently into snapshot table (UPSERT)
            and appends execution record into historical table.
            """
            data_referencia = context["ds"]
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

            upsert_snapshot_sql = """
                INSERT INTO metricas_categorias_snapshot (
                    data_referencia,
                    categoria,
                    preco_medio,
                    preco_minimo,
                    preco_maximo,
                    qtd_produtos,
                    atualizado_em
                )
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (data_referencia, categoria)
                DO UPDATE SET
                    preco_medio = EXCLUDED.preco_medio,
                    preco_minimo = EXCLUDED.preco_minimo,
                    preco_maximo = EXCLUDED.preco_maximo,
                    qtd_produtos = EXCLUDED.qtd_produtos,
                    atualizado_em = CURRENT_TIMESTAMP;
            """

            insert_historico_sql = """
                INSERT INTO metricas_categorias_historico (
                    data_execucao,
                    data_referencia,
                    categoria,
                    preco_medio,
                    preco_minimo,
                    preco_maximo,
                    qtd_produtos
                )
                VALUES (CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s);
            """

            conn = hook.get_conn()
            try:
                with conn.cursor() as cursor:
                    for item in metricas_consolidadas:
                        params = (
                            data_referencia,
                            item["categoria"],
                            item["preco_medio"],
                            item["preco_minimo"],
                            item["preco_maximo"],
                            item["qtd_produtos"],
                        )
                        cursor.execute(upsert_snapshot_sql, params)
                        cursor.execute(insert_historico_sql, params)
                conn.commit()
                logger.info(
                    "Successfully persisted %d category metrics for reference date %s.",
                    len(metricas_consolidadas),
                    data_referencia,
                )
            except Exception as exc:
                conn.rollback()
                logger.error("Database transaction failed; rolled back changes: %s", exc)
                raise
            finally:
                conn.close()

        # Fan-out preparation -> Parallel Mapped Execution -> Fan-in Consolidation
        categorias_agrupadas = agrupar_por_categoria(validar_produtos.output)
        metricas_mapeadas = calcular_metricas_categoria.expand(
            dados_categoria=categorias_agrupadas
        )
        salvar_postgresql(metricas_mapeadas)

    tg_ingestao >> tg_analise


shopbrasil_pricing_pipeline()