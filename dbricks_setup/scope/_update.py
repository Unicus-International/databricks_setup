from argparse import Namespace
from databricks_cli.configure.provider import DatabricksConfig

from ..utils._groups import get_groups
from ..utils._profile import extract_profile


def update_scope(args: Namespace):
    """Updates the secret scope configuration of the databricks instance defined in the current profile

    :param Namespace args: The arguments from the cli
    :return:
    """

    # Get the base profile
    profile, base_config = extract_profile(args)

    # Get the workspace groups
    groups = get_groups(profile)

    print(groups)
