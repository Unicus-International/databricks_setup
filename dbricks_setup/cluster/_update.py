from argparse import Namespace

import logging

from ..utils._groups import create_groups, get_groups
from ..utils._profile import extract_profile
from ..utils.cluster._acl import set_acls
from ..utils.cluster._config import create_config
from ..utils.cluster._create import create_cluster, edit_cluster
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
        cluster_status = 'PENDIING'

        # Terminate the newly started cluster
        if not args.r:
            terminate_cluster(cluster_id, cluster_name, profile)
            cluster_status = 'TERMINATED'

        # Add the new cluster to the configuration
        matching_clusters.append(
            {
                'name': cluster_name,
                'cluster_id': cluster_id,
                'status': cluster_status
            }
        )

    access_groups = {
        f'cluster-{cluster_name}-manage': 'CAN_MANAGE',
        f'cluster-{cluster_name}-restart': 'CAN_RESTART',
        f'cluster-{cluster_name}-attach': 'CAN_ATTACH_TO',
    }

    # Filter and create the missing groups
    missing_groups = [group for group in access_groups if group not in groups]
    if missing_groups:
        create_groups(missing_groups, profile)

    # Update the clusters
    for cluster in matching_clusters:
        set_acls(access_groups, cluster['cluster_id'], base_config)

        # Update the cluster configuration
        if cluster['status'] == 'TERMINATED':
            cluster_config['cluster_id'] = cluster['cluster_id']
            edit_cluster(profile, cluster_config)

    return
