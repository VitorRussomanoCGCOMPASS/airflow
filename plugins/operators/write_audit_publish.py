from typing import Sequence
from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from sqlalchemy.orm.decl_api import DeclarativeMeta
from airflow.utils.context import Context
from typing import Iterable

from contextlib import closing

class TemporaryTableSQLOperator(BaseSQLOperator):

    template_fields: Sequence[str] = ("table", "target_fields")
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
    ):
        super().__init__(
            conn_id=conn_id,
            database=database,
            hook_params={"schema": "dbo", "database": "DB_Brasil"},
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
        # TODO : SCHEMA AND DB.
        # TODO :  WE NEED TO OPT BETWEEN TEMPORARY TABLES AND NON TEMPORARY TABLES

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

        sql = (
            f"USE DB_Brasil SELECT {target_fields} INTO {temporary_table} FROM {table}"
        )

        if not empty:
            end = ";"
        else:
            end = " WHERE 0=1;"

        sql += end
        return sql

    def execute(self, context: Context):

        extra_kwargs = {}

        hook = self.get_db_hook()
        engine = hook.get_sqlalchemy_engine()


        with engine.connect() as conn:
            engine.execute()

        with closing(conn) as conn:
            with closing(conn.cursor()) as cur:
                sql = self._generate_statement_sql(
                    self.table,
                    self.target_fields,
                    self.temporary_table,
                    self.empty,
                    **extra_kwargs,
                )
                self.log.info("Generated sql: %s", sql)
                try:
                    cur.execute(sql)
                except Exception as ex:
                    conn.rollback()
                    raise ex

            conn.commit()

            self.log.info("Done. Created temporary table.")
        return None


class InsertSQLOperator(BaseSQLOperator):
    pass


class MergeSQLOperator(BaseSQLOperator):
    pass
