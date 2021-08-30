from argparse import Namespace
from databricks_cli.configure.provider import DatabricksConfig

from ..utils._profile import extract_profile


def update_scope(args: Namespace):
    extract_profile(args)
