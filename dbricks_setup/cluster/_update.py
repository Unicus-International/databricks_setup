from argparse import Namespace

import logging

from ..utils._groups import create_groups, get_groups
from ..utils._profile import extract_profile, set_aad_scope
from ..utils.scope._acl import get_acls, set_acls
from ..utils.scope._create import create_scope
from ..utils.scope._delete import delete_scope
from ..utils.cluster._config import create_config
from ..utils.cluster._create import create_cluster
from ..utils.cluster._delete import terminate_cluster
from ..utils.cluster._extract import extract_clusters

logger = logging.getLogger(__name__)


def update_cluster_cli(args: Namespace):
    """Updates the cluster configuration of the databricks instance defined in the current profile

    :param Namespace args: The arguments from the cli
    :return:
    """
    # Set the name
    cluster_name = args.name.lower()

    # Get the base profile
    profile, base_config = extract_profile(args)

    # Get the workspace groups
    groups = get_groups(profile)

    # Get the existing cluster
    clusters = extract_clusters(profile)

    # Get the clusters matching the desired name
    matching_clusters = [
        cluster
        for cluster
        in clusters
        if cluster['name'].lower() == cluster_name
    ]

    # Create the cluster configuration
    cluster_config = create_config(cluster_name, profile)

    # Create the cluster
    if not matching_clusters:
        cluster_id = create_cluster(profile, cluster_config)

        # Terminate the newly started cluster
        if not args.r:
            terminate_cluster(cluster_id, cluster_name, profile)
    return

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

    # Construct the access groups
    accesses = ['read', 'write', 'manage']
    access_groups = {
        f'scope-{scope_name}-{access}': access.upper()
        for access in accesses
    }

    # Filter and create the missing groups
    missing_groups = [group for group in access_groups if group not in groups]
    if missing_groups:
        create_groups(missing_groups, profile)

    # Get the existing acls for the secret scope
    acls = get_acls(scope_name, profile)

    # Update the acls
    set_acls(acls, access_groups, scope_name, profile)
