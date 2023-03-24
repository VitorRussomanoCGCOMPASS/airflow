from contextlib import closing
from typing import Any, Iterable, Sequence

from pyodbc import ProgrammingError
from sqlalchemy import MetaData, Table, insert
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
            # WHY DO WE OPT FOR CUR.EXECUTE INSTEAD OF HOOK RUN?
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
    def __init__(
        self,
        *,
        conn_id: str | None = None,
        database: str,
        table: str | DeclarativeMeta,
        values: dict[str, Any],
        **kwargs,
    ) -> None:
        self.table = table
        self.values = values

        hook_params = kwargs.pop("hook_params", {})
        kwargs["hook_params"] = {"database": database, **hook_params}

        super().__init__(
            conn_id=conn_id,
            **kwargs,
        )

    def execute(self, context: Context) -> None:
        hook = self.get_db_hook()
        engine = hook.get_sqlalchemy_engine()

        if isinstance(self.table, DeclarativeMeta):
            # Transform into Table Object for consistency
            self.table = getattr(self.table, "__table__")

        if not isinstance(self.table, Table):
            metadata = MetaData()
            metadata.reflect(bind=engine)
            self.table = Table(self.table, metadata, autoload_with=engine)

        with engine.connect() as conn:
            conn.execute(insert(self.table), self.values)

        self.log.info("Sucessfully inserted values into table :%s", self.table.name)


def on_conflict_do_nothing():
    pass


def on_conflict_do_update():
    pass


from typing import Literal


class MergeSQLOperator(BaseSQLOperator):
    def __init__(
        self,
        *,
        source_table: str | None = None,
        target_table: DeclarativeMeta | Table | str,
        on_conflict=Literal["do_nothing", "do_update"],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_table = source_table
        self.target_table = target_table

    @classmethod
    def generate_merge_sql(cls, target_table: Table, source_table: str):
        pass

    def execute(self, context: Context):

        hook = self.get_db_hook()
        engine = hook.get_sqlalchemy_engine()

        if isinstance(self.target_table, DeclarativeMeta):
            # Transform into Table Object for consistency
            self.target_table = getattr(self.target_table, "__table__")

        if not isinstance(self.target_table, Table):
            # if target_table is str, we generate the model
            metadata = MetaData()
            metadata.reflect(bind=engine)
            self.target_table = Table(self.target_table, metadata, autoload_with=engine)

        if not self.source_table:
            self.source_table = self.target_table.name + "_temp"
        # Now we have source_table as string and target_table as TABLE.
