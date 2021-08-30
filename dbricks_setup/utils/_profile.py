import datetime
import json
import os
from argparse import Namespace
from typing import Tuple

import logging
from databricks_cli.configure.provider import DatabricksConfig, update_and_persist_config

from databricks_cli.configure.provider import DEFAULT_SECTION, ProfileConfigProvider

logger = logging.getLogger(__name__)


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


def set_aad_scope(base_config: DatabricksConfig):
    """Function sets the azure ad scope for use when setting key vault backed scopes

    :param DatabricksConfig base_config: The config upon which the AAD profile is built
    """
    # Get the token from file
    token_file = os.path.join(os.path.expanduser('~'), '.databricks_token.json')

    # Ask the user to update the token file if needed
    if not os.path.exists(token_file):
        error_message = "Azure AD token for this cli has not been configured."
        error_message += f"\nPlease run:"
        error_message += f'\naz account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d>"{token_file}"'
        error_message += '\nand try again. You may need to run "az login" again.'
        raise EnvironmentError(error_message)

    # Get the token data
    with open(token_file, 'r') as f:
        token_data = json.load(f)

    # Get the expiration and enforce rollover
    expiration = datetime.datetime.strptime(token_data.get('expiresOn', '2000-01-01 00:00:00.000000'),
                                            '%Y-%m-%d %H:%M:%S.%f')
    if expiration < datetime.datetime.now() + datetime.timedelta(minutes=5):
        error_message = "Azure AD token for this cli has/is about to expire."
        error_message += f"\nPlease run:"
        error_message += f'\naz account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d>"{token_file}"'
        error_message += '\nand try again. You may need to run "az login" again.'
        raise EnvironmentError(error_message)

    # Get the up to date token
    with open(token_file, 'r') as f:
        logger.info('Updating AAD profile')
        token_data = json.load(f)
        update_and_persist_config(
            'AAD',
            DatabricksConfig(
                host=base_config.host,
                username=None,
                password=None,
                token=token_data['accessToken'],
                insecure=None)
        )

    # Update the AAD profile and validate
    cfg = ProfileConfigProvider('AAD').get_config()
    if cfg is None:
        raise EnvironmentError(
            f'The profile AAD has not been configured correctly, please try again.')
