import json
import logging
from typing import Any, Sequence

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.providers.common.sql.hooks.sql import  fetch_all_handler, return_single_query_results
from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from typing import Mapping, Iterable, Callable
import ast


class SQLQueryToLocalOperator(BaseSQLOperator):
    """
    Executes SQL code in a specific database
    :param sql: the SQL code or string pointing to a template file to be executed (templated).
    File must have a '.sql' extensions.
    When implementing a specific Operator, you can also implement `_process_output` method in the
    hook to perform additional processing of values returned by the DB Hook of yours. For example, you
    can join description retrieved from the cursors of your statements with returned values, or save
    the output of your operator to a file.
    :param autocommit: (optional) if True, each command is automatically committed (default: False).
    :param parameters: (optional) the parameters to render the SQL query with.
    :param handler: (optional) the function that will be applied to the cursor (default: fetch_all_handler).
    :param split_statements: (optional) if split single SQL string into statements. By default, defers
        to the default value in the ``run`` method of the configured hook.
    :param return_last: (optional) return the result of only last statement (default: True).
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SQLExecuteQueryOperator`
    """

    template_fields: Sequence[str] = ("conn_id", "sql", "parameters")
    template_ext: Sequence[str] = (".sql", ".json")
    template_fields_renderers = {"sql": "sql", "parameters": "json"}
    ui_color = "#cdaaed"

    def __init__(
        self,
        *,
        sql: str | list[str],
        autocommit: bool = False,
        parameters: Mapping | Iterable | None = None,
        handler: Callable[[Any], Any] = fetch_all_handler,
        split_statements: bool | None = None,
        return_last: bool = True,
        file_path : str| None =None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.handler = handler
        self.split_statements = split_statements
        self.return_last = return_last
        self.file_path = file_path
    
    #FIXME  : IT IS NOT ALWAYS THAT WE WANT THIS BEHAVIOUR.
    #FIXME: MAYBE A HANDLER?
    def _process_output(self, results: list[Any], descriptions: list[Sequence[Sequence] | None]) -> list[Any]:
        """
        Processes output before it is returned by the operator.
        It can be overridden by the subclass in case some extra processing is needed. Note that unlike
        DBApiHook return values returned - the results passed and returned by ``_process_output`` should
        always be lists of results - each element of the list is a result from a single SQL statement
        (typically this will be list of Rows). You have to make sure that this is the same for returned
        values = there should be one element in the list for each statement executed by the hook..
        The "process_output" method can override the returned output - augmenting or processing the
        output as needed - the output returned will be returned as execute return value and if
        do_xcom_push is set to True, it will be set as XCom returned.
        :param results: results in the form of list of rows.
        :param descriptions: list of descriptions returned by ``cur.description`` in the Python DBAPI
        """
        logging.info("Writing to file")
        logging.info(results)
        logging.info(descriptions)
        

        keys = list(map(lambda x: x.name,  descriptions[0]))
        results = [ dict(zip(keys,i)) for i in results[0]]

        with open(self.file_path, "w+") as f:
            json.dump(results, f)
            
        return results

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        hook = self.get_db_hook()
        if self.split_statements is not None:
            extra_kwargs = {"split_statements": self.split_statements}
        else:
            extra_kwargs = {}
        output = hook.run(
            sql=self.sql,
            autocommit=self.autocommit,
            parameters=self.parameters,
            handler=self.handler,
            return_last=self.return_last,
            **extra_kwargs,
        )
        if return_single_query_results(self.sql, self.return_last, self.split_statements):
            # For simplicity, we pass always list as input to _process_output, regardless if
            # single query results are going to be returned, and we return the first element
            # of the list in this case from the (always) list returned by _process_output
            return self._process_output([output], hook.descriptions)[-1]
        return self._process_output(output, hook.descriptions)

    def prepare_template(self) -> None:
        """Parse template file for attribute parameters."""
        if isinstance(self.parameters, str):
            self.parameters = ast.literal_eval(self.parameters)
