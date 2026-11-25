from typing import Any, Sequence
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class ValidarProdutosOperator(BaseOperator):
    """
    Operador customizado para validar o schema e a integridade dos payloads retornados.
    """

    template_fields: Sequence[str] = ("task_id_origem",)

    def __init__(self, task_id_origem: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.task_id_origem = task_id_origem

    def execute(self, context: dict[str, Any]) -> list[dict[str, Any]]:
        ti = context["ti"]
        produtos: list[dict[str, Any]] | None = ti.xcom_pull(
            task_ids=self.task_id_origem
        )

        if not produtos or not isinstance(produtos, list):
            raise AirflowException(
                "Schema validation failed: product list is empty or invalid."
            )

        required_keys = {"id", "title", "price", "category"}
        validated_products: list[dict[str, Any]] = []

        for index, item in enumerate(produtos):
            if not isinstance(item, dict):
                raise AirflowException(
                    f"Schema validation failed at index {index}: expected dict, got {type(item).__name__}."
                )

            missing_keys = required_keys - set(item.keys())
            if missing_keys:
                raise AirflowException(
                    f"Schema validation failed at index {index}: missing keys {missing_keys}."
                )

            if not isinstance(item["price"], (int, float)) or item["price"] < 0:
                raise AirflowException(
                    f"Schema validation failed for product id={item.get('id')}: invalid price {item['price']}."
                )

            if not isinstance(item["category"], str) or not item["category"].strip():
                raise AirflowException(
                    f"Schema validation failed for product id={item.get('id')}: invalid category."
                )

            # Otimização básica.
            # Manter apenas os campos leves necessários para as etapas posteriores,
            # minimizando o volume de dados trafegados via XCom.
            validated_products.append(
                {
                    "category": item["category"].strip(),
                    "price": float(item["price"]),
                }
            )

        self.log.info(
            "Schema validation succeeded for %d products.", len(validated_products)
        )
        return validated_products