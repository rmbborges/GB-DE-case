from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers
import hooks

class AuxPlugins(AirflowPlugin):
    name = "aux_plugins"
    operators = [
        operators.TreatXLSXOperator
    ],
    hooks = [
        hooks.GetTopSoldProductHook
    ],
    helpers = [
        helpers.SqlQueries
    ]
