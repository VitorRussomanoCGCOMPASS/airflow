from typing import Any, Iterable, Sequence, Generator

from sqlalchemy import Column, MetaData, Table
from sqlalchemy.orm.decl_api import DeclarativeMeta

from airflow.models.xcom_arg import XComArg
from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.utils.context import Context


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
    def validate_columns(
        cls, table: Table, columns: Iterable[str] | Iterable[Column]
    ) -> Generator:

        for col in columns:
            if isinstance(col, str):
                # We make sure that there is a column with that name for safety
                try:
                    col = getattr((getattr(table, "columns")), col)
                except AttributeError:
                    raise Exception(
                        "There is no column named %s in table %s", col, table
                    )
            if isinstance(col, Column):
                yield col

    @classmethod
    def generate_update_clause(
        cls, table: Table, columns: Iterable[str] | Iterable[Column]
    ) -> str:
        """
        Generate the UPDATE clause for the MERGE statement using the columns to be updated.

        Parameters
        ----------
        source_table : Table
            The table object.
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

        cols  =cls.validate_columns(table, columns)

        f_col = next(cols)
        stmt += f"target.{f_col.name} =  source.{f_col.name}"
        
        for col in cols:
            stmt+= f", target.{col.name} =  source.{col.name}"

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
                table=source_table, columns=set_
            )
            sql += update_stmt

        return sql + " ;"

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
