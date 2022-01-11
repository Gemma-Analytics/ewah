from ewah.constants import EWAHConstants as EC
from ewah.operators import operator_list as operators
from ewah.operators.base import EWAHBaseOperator as EBO

from .dag_factory_atomic import dag_factory_atomic
from .dag_factory_idempotent import dag_factory_idempotent
from .dag_factory_mixed import dag_factory_mixed

from airflow.utils.log.logging_mixin import LoggingMixin

from typing import Optional, Any
from copy import deepcopy
from jinja2 import Template

import yaml
from ewah.utils.yml_loader import Loader, Dumper


class EWAHDAGGenerator(LoggingMixin):
    """
    Generator class to yield all DAGs defined in the dag definition dict.
    """

    DAG_FACTORIES = {
        EC.DS_ATOMIC: dag_factory_atomic,
        "fr": dag_factory_atomic,
        "full-refresh": dag_factory_atomic,
        "full refresh": dag_factory_atomic,
        EC.DS_MIXED: dag_factory_mixed,
        "fullcremental": dag_factory_mixed,
        "periodic fr": dag_factory_mixed,
        EC.DS_IDEMPOTENT: dag_factory_idempotent,
        "idemnpotent": dag_factory_idempotent,
        "inc": dag_factory_idempotent,
        "incr": dag_factory_idempotent,
        "incremental": dag_factory_idempotent,
    }

    @staticmethod
    def _default(my_dict: dict, key: str, default_value: Any):
        "Set default value to a key if key does not exist in dict."
        my_dict[key] = my_dict.pop(key, default_value)

    def __init__(self, dag_definition_dict: dict) -> None:
        self.base_config = dag_definition_dict.pop("base_config", {})
        self.dags_dict = dag_definition_dict.pop("el_dags", {})
        _msg = "Invalid definitions:\n\n{0}".format(str(dag_definition_dict))
        assert not dag_definition_dict, _msg
        assert self.dags_dict, "No DAGs specified in config!"

        self._default(self.base_config, "additional_dag_args", {})
        self._default(self.base_config, "additional_task_args", {})
        self._default(self.base_config["additional_task_args"], "owner", "EWAH")
        self._default(
            self.base_config["additional_task_args"], "weight_rule", "upstream"
        )

    def __iter__(self):
        "Generator to yield individual DAGs as parsed from DAGs dictionary."
        self.log.info("EWAH is now loading DAGs from dict")

        def get_logging_func(dag_name):
            def logging_func(msg):
                self.log.info(
                    "EWAH LOG - DAG: {0} - Message: {1}".format(dag_name, msg)
                )

            return logging_func

        for dag_name, dag_config in deepcopy(self.dags_dict).items():
            self.log.info("EWAH is loading DAG(s): %s", dag_name)

            # Start with base config as default config, build up from there
            config = deepcopy(self.base_config)
            config["logging_func"] = get_logging_func(dag_name)
            config["additional_task_args"].update(
                dag_config.pop("additional_task_args", {})
            )
            config["additional_dag_args"].update(
                dag_config.pop("additional_dag_args", {})
            )
            config.update(dag_config)
            config["dag_name"] = dag_name

            dwh_engine = config.pop("dwh_engine", None)
            if (
                dwh_engine is None
                or not dwh_engine.lower() in EC.DWH_ENGINE_SPELLING_VARIANTS
            ):
                raise Exception(
                    "Invalid dwh_engine {0} for DAG {1}!".format(
                        dwh_engine,
                        dag_name,
                    )
                )
            config["dwh_engine"] = EC.DWH_ENGINE_SPELLING_VARIANTS[dwh_engine]

            el_strategy = config.pop("el_strategy", config.pop("dag_strategy", None))
            if (
                el_strategy is None
                or not el_strategy.lower() in self.DAG_FACTORIES.keys()
            ):
                raise Exception(
                    "Invalid el_strategy {0} for DAG {1}".format(
                        el_strategy,
                        dag_name,
                    )
                )
            dag_factory = self.DAG_FACTORIES[el_strategy.lower()]

            el_operator = config.pop("el_operator", None)
            if isinstance(el_operator, type) and issubclass(el_operator, EBO):
                config["el_operator"] = el_operator
            else:
                if not el_operator.lower() in operators:
                    _msg = "Invalid el_operator {0} for DAG {1}!".format(
                        el_operator,
                        dag_name,
                    )
                    raise Exception(_msg)
                config["el_operator"] = operators[el_operator.lower()]

            dags = dag_factory(**config)
            for dag in dags:
                # Iterate over individual DAGs!
                yield dag


def dags_from_yml_file(file_path: str) -> EWAHDAGGenerator:
    """Load DAGs from a YAML file.

    Returns an initialized generator class that iterates over all the individual
    DAGs as defined in the provided YAML file.

    This YAML file may contain Jinja2 templating.

    :param file_path: Complete path to the YAML file containing the DAGs
        definition.
    """

    return EWAHDAGGenerator(
        dag_definition_dict=yaml.load(open(file_path, "r"), Loader=Loader)
    )
