from argparse import Namespace

import logging

from ..utils._groups import get_groups
from ..utils._profile import extract_profile, set_aad_scope
from ..utils.scope._extract import extract_scopes
from ..utils.scope._delete import delete_scope
from ..utils.scope._create import create_scope

logger = logging.getLogger(__name__)


def update_scope(args: Namespace):
    """Updates the secret scope configuration of the databricks instance defined in the current profile

    :param Namespace args: The arguments from the cli
    :return:
    """

    # Get the base profile
    profile, base_config = extract_profile(args)

    # Get the workspace groups
    groups = get_groups(profile)

    # Get the existing scopes
    scopes = extract_scopes(profile)

    # Check scope name
    scope_name = args.scope_name
    if not scope_name:
        scope_name = args.key_vault

    # Check scope existence
    if scope_name in scopes and not args.f:
        logger.warning(
            f'Scope {scope_name} already exists. Please remove if misconfigured, consider using the -f flag to force an update.')
    else:
        create = args.f or scope_name not in scopes

        # If the scope is missing or an update is forced, recreate it
        if create:
            # Update the azure ad profile if needed
            set_aad_scope(base_config)

            # Delete if exists
            if scope_name in scopes:
                # Delete the scope
                delete_scope(scope_name, profile)

            create_scope(
                scope=scope_name,
                resource_id=args.resource_id,
                key_vault_name=args.key_vault
            )
