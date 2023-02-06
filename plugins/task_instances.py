from typing import Any, Dict, List, Optional

from sqlalchemy.orm.session import Session

from airflow.models import taskinstance
from airflow.models.dagrun import DagRun
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState
from airflow.plugins_manager import AirflowPlugin


@provide_session
def get_previous_ti_by_state(
    task_instance: taskinstance.TaskInstance,
    task_state: TaskInstanceState,
    session: Session = NEW_SESSION,
) -> Optional[taskinstance.TaskInstance]:
    """Gets the latest taskinstance.TaskInstance whose state corresponds to task_state
    Args:
        task_instance (taskinstance.TaskInstance): The target taskinstance.TaskInstance
        task_state (TaskInstanceState): The TaskInstanceState to use as a filter
        session (Session, optional): The DB Session. Defaults to NEW_SESSION.
    Returns:
        Optional[taskinstance.TaskInstance]: The fetched taskinstance.TaskInstance, if any
    """
    # retrieve the latest Task Instance model whose TaskInstanceState corresponds to the specified task instance state
    return (
        session.query(taskinstance.TaskInstance)
        .filter_by(state=task_state, task_id=task_instance.task_id)
        .order_by(taskinstance.TaskInstance.start_date.desc())  # type: ignore
        .first()
    )


@provide_session
def get_previous_ti_success(
    task_instance: taskinstance.TaskInstance, session: Session = NEW_SESSION
) -> Optional[taskinstance.TaskInstance]:
    """Fetches the latest TraskInstance whose state is TaskInstanceState.SUCCESS
    Args:
        task_instance (taskinstance.TaskInstance):  The target taskinstance.TaskInstance
        session (Session, optional): The DB Session. Defaults to NEW_SESSION.
    Returns:
        Optional[taskinstance.TaskInstance]: The fetched taskinstance.TaskInstance, if any
    """
    return get_previous_ti_by_state(
        task_instance=task_instance,
        task_state=TaskInstanceState.SUCCESS,
        session=session,
    )


@provide_session
def get_previous_ti_dagrun_by_state(
    task_instance: taskinstance.TaskInstance,
    task_state: TaskInstanceState,
    session: Session = NEW_SESSION,
) -> Optional[DagRun]:
    """Gets the DAGRun associated with the latest taskinstance.TaskInstance whose state corresponds to task_state
    Args:
        task_instance (taskinstance.TaskInstance): The target taskinstance.TaskInstance
        task_state (TaskInstanceState): The TaskInstanceState to use as a filter
        session (Session, optional): The DB Session. Defaults to NEW_SESSION.
    Returns:
        Optional[DagRun]: The retrieved DAGRun instance, if any
    """

    # the previously ran task according to the specified state
    prev_task_instance: taskinstance.TaskInstance = get_previous_ti_by_state(
        task_instance=task_instance, task_state=task_state, session=session
    )

    if prev_task_instance:
        # the DAG Run instance associated with the previously ran task instance according to the specified state
        dag_run: DagRun = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == prev_task_instance.dag_id,
                DagRun.run_id == prev_task_instance.run_id,
            )
            .one()
        )

        return dag_run

    return None


@provide_session
def get_previous_ti_sucess_dagrun(
    task_instance: taskinstance.TaskInstance, session: Session = NEW_SESSION
) -> Optional[DagRun]:
    """Gets the DAGRun associated with the latest taskinstance.TaskInstance whose state is TaskInstanceState.SUCCESS
    Args:
        task_instance (taskinstance.TaskInstance): The target taskinstance.TaskInstance
        session (Session, optional): The DB Session. Defaults to NEW_SESSION.
    Returns:
        Optional[DagRun]: The retrieved DAGRun instance, if any
    """

    return get_previous_ti_dagrun_by_state(
        task_instance=task_instance,
        task_state=TaskInstanceState.SUCCESS,
        session=session,
    )


from airflow.utils.session import provide_session
from airflow.models import XCom


@provide_session
def _cleanup_xcom(context, session=None) -> None:
    import logging

    dag_id = context["ti"]["dag_id"]
    if not session:
        session = context["session"]

    session.query(XCom).filter(XCom.dag_id == dag_id).delete()
    logging.info("Cleaned up XCOM")


class TaskPlugin(AirflowPlugin):
    name = "task_plugin"
    macros = [
        get_previous_ti_by_state,
        get_previous_ti_dagrun_by_state,
        get_previous_ti_success,
        get_previous_ti_sucess_dagrun,
        _cleanup_xcom,
    ]
