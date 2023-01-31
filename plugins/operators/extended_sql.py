import json
import logging
from typing import Any, Sequence

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


class SQLQueryToLocalOperator(SQLExecuteQueryOperator):
    def __init__(self, file_path: str, *args, **kwargs) -> None:
        self.file_path = file_path
        super().__init__(*args, **kwargs)

    def _process_output(
        self, results: list[Any], descriptions: list[Sequence[Sequence] | None]
    ):

        logging.info("Writing to file")
        with open(self.file_path, "w") as f:
            json.dump(results, f)
        return results
