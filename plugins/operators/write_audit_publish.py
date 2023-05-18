from typing import Any, Generator, Iterable, Sequence

from sqlalchemy import Column, Table
from sqlalchemy.orm.decl_api import DeclarativeMeta

from airflow.models.xcom_arg import XComArg
from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.utils.context import Context

from typing import Iterable


class InsertSQLOperator(BaseSQLOperator):

    """
    A custom operator that inserts data into a specific table in an SQL database.

    Parameters
    ----------
    conn_id : str or None, optional
        The connection ID to use to connect to the database. If None, the default connection
        ID will be used. Default is None.
    database : str or None, optional
        The name of the database to use. If None, the default database for the connection
        will be used. Default is None.
    table : DeclarativeMeta or Table
        The table object or metadata for the table to insert data into.
    values : dict or XComArg, optional
        The data to insert. Can be a single dictionary or a list of dictionaries.
    normalize : bool, optional
        If True, the dictionaries in the `values` argument will be normalized to include
        any expected keys by the SQLALCHEMY model that are not present. If False, the
        dictionaries will be inserted as is. Default is True.
    engine_kwargs : dict or None, optional
        Additional keyword arguments to pass to the SQLAlchemy engine when creating a
        connection. If None, the default behaviour is to set `fast_executemany=True`.
        Default is None.

    Raises
    ------
    ValueError
        If the `table` argument is not a `DeclarativeMeta` or `Table` object.
    TypeError
        If the `values` argument is not a dictionary or a list of dictionaries.

    Notes
    -----
    If `fast_executemany` is set to True in `engine_kwargs`, the operator will use the
    `executemany` method of the SQLAlchemy connection object to insert multiple rows
    at once. This can significantly improve performance when inserting large amounts of
    data. However, this feature is not supported by all database backends and can cause
    errors in some cases. If you encounter errors when using `fast_executemany`, try
    setting it to False.

    """

    template_fields: Sequence[str] = "values"
    template_ext: Sequence[str] = ".json"
    template_fields_renderers = {"values": "json"}

    def __init__(
        self,
        *,
        conn_id: str | None = None,
        database: str | None = None,
        table: DeclarativeMeta | Table,
        values: dict[str, Any] | XComArg,
        engine_kwargs: dict | None = None,
        normalize: bool = True,
        data_keys: dict | Iterable[dict] | None = None,
        **kwargs,
    ) -> None:
        self.table = table
        self.values = values or {}
        self.normalize = normalize
        self.engine_kwargs = engine_kwargs or {}
        self.data_keys = data_keys

        hook_params = kwargs.pop("hook_params", {})
        kwargs["hook_params"] = {"database": database, **hook_params}

        super().__init__(
            conn_id=conn_id,
            **kwargs,
        )

    def _invoke_pre_processing(
        self, table: Table, normalize: bool, data_keys, many=False
    ) -> None:

        if isinstance(self.values, list):
            many = True

        columns = set([col for col in table.columns.keys()])
        if data_keys:
            keys = [
                data_keys.get(col) if data_keys.get(col) is not None else col
                for col in columns
            ]

            if any(data_keys[key] not in keys for key in data_keys):
                raise ValueError(
                    "The data key argument for one or more fields are "
                    "not present in the Table model."
                    "Check the following field names: {}".format(
                        set(data_keys) - columns
                    )
                )

            if len(keys) != len(set(keys)):
                data_keys_duplicates = {x for x in keys if keys.count(x) > 1}
                raise ValueError(
                    "The data_key argument for one or more fields collides "
                    "with another field's name or data_key argument. "
                    "Check the following field names and "
                    "data_key arguments: {}".format(list(data_keys_duplicates))
                )

            self.log.info("Replacing data keys %s", data_keys)
            self.values = self._replace_keys(self.values, data_keys, many)

        if normalize:
            # Deletes extra keys and create key value pair with NULL in the missing keys
            self.log.info("Normalizing values")

            self.out = set()
            self.values = self._normalize_dict(self.values, columns, many)

            if self.out:
                self.log.info("Removed extra keys %s", list(self.out))

    @staticmethod
    def _replace_keys(value, data_keys, many: bool = False):
        if many and value is not None:
            return [
                InsertSQLOperator._replace_keys(val, data_keys, many=False)
                for val in value
            ]

        for key, new_key in data_keys.items():
            value[key] = value.pop(new_key)

        return value

    def _normalize_dict(self, value, columns, many: bool = False):
        """
        Normalize all dictionaries in value to ensure that it contains all expected keys for the SQL model.

        If any expected key is missing from a dictionary, it will be added with a value of `None`.

        Parameters
        ----------
        value : dict | List[dict]
            value to be normalized

        table : Table
            SQLALCHEMY model of the table

        Returns
        -------
        value
            The normalized dictionary or dictionaries with all expected keys.

        """
        if many and value is not None:
            return [self._normalize_dict(val, columns, many=False) for val in value]

        to_remove = columns - value.keys()
        value.update((col, None) for col in to_remove)
        self.out.update(to_remove)

        return value

    def execute(self, context: Context) -> None:
        hook = self.get_db_hook()

        if "fast_executemany" not in self.engine_kwargs:
            self.engine_kwargs.update({"fast_executemany": True})

        engine = hook.get_sqlalchemy_engine(engine_kwargs=self.engine_kwargs)

        if isinstance(self.table, DeclarativeMeta):
            # Transform into Table Object for consistency
            self.table = getattr(self.table, "__table__")

        with engine.connect() as conn:

            self._invoke_pre_processing(
                data_keys=self.data_keys, table=self.table, normalize=self.normalize
            )

            conn.execute(self.table.insert(), self.values)

        self.log.info(
            "Sucessfully inserted values into table: %s", self.table.name.upper()
        )


from sqlalchemy.sql.selectable import TableClause


class MergeSQLOperator(BaseSQLOperator):
    """
    Operator that performs a SQL merge operation on tables or views.

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
    :param index_where: The where clause to use for the index
        (Rows that do not match the condition will not be affect at all, neither in the update or insert statement.)
    :type index_where: None
    :param set_: The columns to update in the target table. If None, only inserts will be performed.
    :type set_: Iterable[sqlalchemy.schema.Column] or Iterable[str] or None
    :param holdlock: Whether to hold a lock during the merge operation.
    :type holdlock: bool
    """
    


    template_fields: Sequence[str] = ("index_where",)
    



    def __init__(
        self,
        *,
        conn_id: str | None = None,
        database: str | None = None,
        source_table: DeclarativeMeta | Table | TableClause,
        target_table: DeclarativeMeta | Table | TableClause,
        index_elements: Iterable[str] | str | None = None,
        index_where: str | None = None,
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
        self.index_elements = index_elements
        self.index_where = index_where

    @staticmethod
    def validate_columns(
        table: Table,
        columns: Iterable[str] | Iterable[Column] | Column | str,
        index_elements: Iterable[Column] | None = None,
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
                if index_elements and col.name not in index_elements:
                    raise Exception(
                        "Update statement includes an element that does not compose the index_elements parameters."
                    )

                yield col

    @staticmethod
    def generate_on_clause(primary_keys: list) -> str:
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
        stmt = f'target."{primary_keys[0]}" = source."{primary_keys[0]}"'

        if len(primary_keys) > 1:
            for pk in primary_keys[1:]:
                stmt += f' AND target."{pk}" = source."{pk}"'

        return stmt

    def generate_update_clause(
        self, table: Table, columns: Iterable[str] | Iterable[Column]
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

        cols = self.validate_columns(table, columns)

        f_col = next(cols)
        stmt += f'target."{f_col.name}" =  source."{f_col.name}"'

        for col in cols:
            stmt += f', target."{col.name}" =  source."{col.name}"'

        return stmt

    def _generate_merge_sql(
        self,
        target_table,
        source_table,
        holdlock: bool,
        set_,
        index_elements: None | Iterable[str] = None,
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

        target_table_pks = [pk.name for pk in target_table.primary_key]
        cols = [
            f'"{i.name}"'
            for i in source_table.columns
            if not index_elements or i.name in index_elements
        ]

        sql = f""" 
                MERGE {target_table.name} {"WITH (HOLDLOCK) as target" if holdlock else "as target"}
                USING {self._genenerate_partition_stmt(source_table, self.index_where)} AS source
                ON {self.generate_on_clause(primary_keys=target_table_pks)}

                WHEN NOT MATCHED  BY TARGET THEN
                    INSERT ({', '.join(cols)})
                    VALUES ({', '.join(["source" + "." + col for col in cols])})
                """

        if set_:
            update_stmt = self.generate_update_clause(table=source_table, columns=set_)
            sql += update_stmt

        return sql + " ;"

    @staticmethod
    def _genenerate_partition_stmt(
        source_table,
        index_where
    ) -> str:
        if index_where:
            return f""" 
            ( 
            SELECT * FROM {source_table.name}
            WHERE {index_where}
            ) 
            """
        return source_table.name

    def execute(self, context: Context):

        hook = self.get_db_hook()

        # Transform into Table Object for consistency
        if isinstance(self.source_table, (DeclarativeMeta, TableClause)):
            self.source_table = getattr(self.source_table, "__table__")

        if isinstance(self.target_table, (DeclarativeMeta, TableClause)):
            self.target_table = getattr(self.target_table, "__table__")

        sql = self._generate_merge_sql(
            source_table=self.source_table,
            target_table=self.target_table,
            holdlock=self.holdlock,
            set_=self.set_,
            index_elements=self.index_elements,
        )

        self.log.info("Generated sql: %s", sql)
        hook.run(sql, handler=None)

        self.log.info(
            f""" 
            Sucessfuly merged tables with source : {self.source_table.name} and target : {self.target_table.name}
            { "With partition: {}".format(self.index_where) if self.index_where else ""}"""
        )
