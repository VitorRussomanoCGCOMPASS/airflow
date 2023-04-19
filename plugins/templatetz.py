from functools import lru_cache
from airflow.plugins_manager import AirflowPlugin

@lru_cache(maxsize=2)
def convert_ts(ts: str, timezone="America/Sao_Paulo") -> str:
    import pendulum
    try:
        pendulum_datetime = pendulum.from_format(ts, "YYYY-MM-DDTHH:mm:ss.SSSSSSZ")
    except ValueError:
        try:
            pendulum_datetime = pendulum.from_format(ts, "YYYY-MM-DD")
        except ValueError:
            pendulum_datetime = pendulum.parser.parse(ts)
                            
    return pendulum_datetime.in_tz(timezone).to_date_string()

class TemplateTz(AirflowPlugin):
    name = "template_tz"
    macros = [convert_ts]
