from contextlib import closing
from typing import Iterable, Sequence

from pyodbc import ProgrammingError
from sqlalchemy.orm.decl_api import DeclarativeMeta

from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.utils.context import Context

# WE NEED TO OPT BETWEEN TEMPORARY TABLES AND NON TEMPORARY TABLES
    
class TemporaryTableSQLOperator(BaseSQLOperator):

    template_fields: Sequence[str] = ()
    template_ext: Sequence[str] = ()

    def __init__(
        self,
        *,
        table: str | DeclarativeMeta,
        target_fields: Iterable[str] | None = None,
        temporary_table: str | None = None,
        empty: bool = True,
        conn_id: str | None = None,
        database: str | None = None,
        **kwargs,
    ) -> None:

        if database is not None:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {"database": database, **hook_params}

        super().__init__(
            conn_id=conn_id,
            **kwargs,
        )

        self.table = table
        self.target_fields = target_fields or ()
        self.temporary_table = temporary_table
        self.empty = empty

    @classmethod
    def _generate_statement_sql(
        cls,
        table: str | DeclarativeMeta,
        target_fields: Iterable[str],
        temporary_table: str | None,
        empty: bool,
        **kwargs,
    ) -> str:

        if target_fields:
            if not isinstance(target_fields, Iterable):
                raise Exception(
                    "Trailing commas required for a unique element : e.g. (EmployeeId,)"
                )
            target_fields = ", ".join(target_fields)
        else:
            target_fields = "*"

        if isinstance(table, DeclarativeMeta):
            table = table.__name__

        if not temporary_table:
            temporary_table = table + "_temp"

        sql = f"SELECT {target_fields} INTO {temporary_table} FROM {table}"

        if not empty:
            end = ";"
        else:
            end = " WHERE 0=1;"

        sql += end
        return sql

    def execute(self, context: Context):

        extra_kwargs = {}

        hook = self.get_db_hook()

        with closing(hook.get_conn()) as conn:

            cur = conn.cursor()

            sql = self._generate_statement_sql(
                self.table,
                self.target_fields,
                self.temporary_table,
                self.empty,
                **extra_kwargs,
            )

            self.log.debug("Generated sql: %s", sql)

            try:
                cur.execute(sql)
            except ProgrammingError as ex:
                if ex.args[0] == "42S01":
                    self.log.error("Table already exists")
                    raise

            finally:
                cur.close()

            conn.commit()

        self.log.info("Done. Created temporary table.")


class InsertSQLOperator(BaseSQLOperator):
    pass


class MergeSQLOperator(BaseSQLOperator):
    pass
