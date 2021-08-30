from argparse import Namespace
from typing import Tuple
from databricks_cli.configure.provider import DatabricksConfig

from databricks_cli.configure.provider import DEFAULT_SECTION, ProfileConfigProvider


def extract_profile(args: Namespace) -> Tuple[str, DatabricksConfig]:
    """Function gets the configuration from the databricks cli, defaults to DEFAULT

    :param Namespace args: The arguments from the cli

    :return: Profile name, and profile object
    :rtype: Tuple[str, DatabricksConfig]
    """

    # Set the profile
    profile = DEFAULT_SECTION

    # Update the cli call with the desired profile
    if args.profile is not None:
        profile = args.profile

    # Get the profile for extraneous requests
    base_cfg = ProfileConfigProvider(profile).get_config()
    if base_cfg is None:
        raise EnvironmentError(f'The profile {profile} has not been configured please add it to the databricks cli.')

    return profile, base_cfg
