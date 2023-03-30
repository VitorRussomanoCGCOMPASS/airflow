from contextlib import closing
from typing import Any, Iterable, Literal, Sequence

from pyodbc import ProgrammingError
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.orm.decl_api import DeclarativeMeta

from airflow.models.xcom_arg import XComArg
from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.utils.context import Context


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

    template_fields: Sequence[str] = "values"
    template_ext: Sequence[str] = ".json"
    template_fields_renderers = {"values": "json"}

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        database: str,
        table: str | DeclarativeMeta,
        values: dict[str, Any] | XComArg,
        **kwargs,
    ) -> None:
        self.table = table
        self.values = values or {}

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

        # This wont be needed anymore.
        if not isinstance(self.table, Table):
            metadata = MetaData()
            metadata.reflect(bind=engine)
            self.table = Table(self.table, metadata, autoload_with=engine)

        with engine.connect() as conn:
            conn.execute(self.table.insert().values(self.values))

        self.log.info("Sucessfully inserted values into table :%s", self.table.name)


# TODO : IF ON CONFLICT DO UPDATE
""" 
WHEN MATCHED THEN
UPDATE SET ....
"""


class MergeSQLOperator(BaseSQLOperator):
    """
    Operator that performs a SQL merge operation on two tables.

    :param conn_id: The connection ID to use for the operator.
    :type conn_id: str or None
    :param database: The name of the database to use.
    :type database: str
    :param source_table: The source table to merge.
    :type source_table: sqlalchemy.ext.declarative.api.DeclarativeMeta or sqlalchemy.schema.Table
    :param target_table: The target table to merge into.
    :type target_table: sqlalchemy.ext.declarative.api.DeclarativeMeta or sqlalchemy.schema.Table
    :param index_elements: The index elements to use for the merge operation.
    :type index_elements: list[sqlalchemy.schema.Column] or sqlalchemy.schema.Column or None
    :param index_where: The where clause to use for the index.
    :type index_where: None
    :param set_: The columns to update in the target table. If None, only inserts will be performed.
    :type set_: Iterable[sqlalchemy.schema.Column] or Iterable[str] or None
    :param holdlock: Whether to hold a lock during the merge operation.
    :type holdlock: bool
    """

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        database: str,
        source_table: DeclarativeMeta | Table,
        target_table: DeclarativeMeta | Table,
        index_elements: list[Column] | Column | None = None,
        index_where: None = None,
        set_: Iterable[Column] | Iterable[str] | None = None,
        holdlock: bool = True,
        **kwargs,
    ) -> None:

        hook_params = kwargs.pop("hook_params", {})
        kwargs["hook_params"] = {"database": database, **hook_params}

        super().__init__(
            conn_id=conn_id,
            **kwargs,
        )

        self.source_table = source_table
        self.target_table = target_table
        self.holdlock = holdlock
        self.set_ = set_

    @classmethod
    def generate_on_clause(cls, primary_keys: list) -> str:
        """
        Generate the ON clause for the MERGE statement using the primary key of the target table.

        Parameters
        ----------
        primary_key : list
            List of primary key columns.

        Returns
        -------
        str
            The ON clause for the MERGE statement.
        """
        stmt = f"target.{primary_keys[0]} = source.{primary_keys[0]}"

        if len(primary_keys) > 1:
            for pk in primary_keys[1:]:
                stmt += f" AND target.{pk} = source.{pk}"

        return stmt

    @classmethod
    def generate_update_clause(
        cls, source_table: Table, columns: Iterable[str] | Iterable[Column]
    ) -> str:
        """
        Generate the UPDATE clause for the MERGE statement using the columns to be updated.

        Parameters
        ----------
        source_table : Table
            The source table object.
        columns : Iterable
            Iterable of column names or column objects.

        Returns
        -------
        str
            The UPDATE clause for the MERGE statement.
        """
        stmt = """ 
                WHEN MATCHED THEN
                UPDATE SET 
                """
            
        for col in columns:
            if isinstance(col, str):
                # We make sure that there is a column with that name for safety
                try:
                    col =   getattr((getattr(source_table, "columns")), col)
                except AttributeError as exc:
                    raise exc
            
            if isinstance(col, Column):
                stmt += f"target.{col.name} = source.{col.name}"
                

        return stmt

    @classmethod
    def _generate_merge_sql(
        cls, target_table: Table, source_table: Table, holdlock: bool, set_
    ) -> str:
        """
        Generate SQL statement for merging data from `source_table` to `target_table`.

        Parameters
        ----------
        target_table : Table
            The table to merge data into.
        source_table : Table
            The table to merge data from.
        holdlock : bool
            Whether to use the HOLDLOCK hint in the SQL statement or not.
        set_ : Iterable[Column] | Iterable[str] | None
            The columns to update in case of a match between the `source_table` and
            `target_table`.

        Returns
        -------
        str
            The SQL statement to merge the data.

        Raises
        ------
        TypeError
            If either `target_table` or `source_table` is not of type `Table` or
            `DeclarativeMeta`.
        """
        if not isinstance(source_table, Table) or not isinstance(target_table, Table):
            raise TypeError("Tables must be of type <Table> or <DeclarativeMeta>")

        target_table_pks = [pk.name for pk in target_table.primary_key]

        cols = [i.name for i in source_table.columns]
        source_prefixed_non_pks = ["source" + "." + col for col in cols]

        sql = f""" 
                MERGE {target_table.name} {"WITH (HOLDLOCK) as target" if holdlock else "as target"}
                USING {source_table.name} as source
                ON {cls.generate_on_clause(primary_keys=target_table_pks)}

                WHEN NOT MATCHED  BY TARGET THEN
                    INSERT ({', '.join(cols)})
                    VALUES ({', '.join(source_prefixed_non_pks)})
                """

        if set_:
            update_stmt = cls.generate_update_clause(
                source_table=source_table, columns=set_
            )
            sql += update_stmt

        sql +=';'

        return sql

    def execute(self, context: Context):

        hook = self.get_db_hook()

        if isinstance(self.target_table, DeclarativeMeta):
            # Transform into Table Object for consistency
            self.target_table = getattr(self.target_table, "__table__")

        if isinstance(self.source_table, DeclarativeMeta):
            self.source_table = getattr(self.source_table, "__table__")

        sql = self._generate_merge_sql(
            source_table=self.source_table,
            target_table=self.target_table,
            holdlock=self.holdlock,
            set_=self.set_,
        )
        
        self.log.debug("Generated sql: %s", sql)
       
        hook.run(sql)
       
        self.log.info("Sucessfuly merged.")


