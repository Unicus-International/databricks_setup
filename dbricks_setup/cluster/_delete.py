from argparse import Namespace

import logging

from ..utils._groups import delete_group, get_groups
from ..utils._profile import extract_profile
from ..utils.cluster._acl import get_acls, set_acls
from ..utils.cluster._delete import terminate_cluster, delete_cluster
from ..utils.cluster._extract import extract_clusters

logger = logging.getLogger(__name__)


def delete_cluster_cli(args: Namespace):
    """Updates the secret scope configuration of the databricks instance defined in the current profile

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

    # Set access groups
    access_groups = {
        f'cluster-{cluster_name}-manage': 'CAN_MANAGE',
        f'cluster-{cluster_name}-restart': 'CAN_RESTART',
        f'cluster-{cluster_name}-attach': 'CAN_ATTACH_TO',
    }

    # Filter the existing groups
    existing_groups = [group for group in access_groups if group in groups]

    # Set the permission structure
    permissions = {
    }

    # Get the existing permissions
    for cluster in matching_clusters:
        cluster_id = cluster['cluster_id']
        permissions[cluster_id] = []

        # Get the permissions for the cluster
        cluster_permissions = get_acls(cluster_id, base_config)
        for acl in cluster_permissions.get('access_control_list', []):
            principal = acl.get('group_name', acl.get('user_name', 'UNKOWN'))
            acl_permissions = set(
                permission['permission_level']
                for permission
                in acl['all_permissions']
                if not permission['inherited']
            )
            if acl_permissions:
                permissions[cluster_id].append(
                    {
                        'principal': principal,
                        'permissions': sorted(acl_permissions)
                    }
                )
        # Remove empty lists
        if not permissions[cluster_id]:
            permissions.pop(cluster_id)

    # Set deletions
    to_delete = {
        'clusters': matching_clusters,
        'groups': existing_groups,
        'permissions': permissions
    }

    if (not args.a and not args.s) or not matching_clusters:
        to_delete.pop('clusters')
    if (not args.a and not args.g) or not existing_groups:
        to_delete.pop('groups')
    if (not args.a and not args.c) or not permissions:
        to_delete.pop('permissions')

    deletion_warning = ''
    if 'clusters' in to_delete:
        deletion_warning += '\nClusters:'
        for cluster in to_delete['clusters']:
            deletion_warning += f'\n\t{cluster["cluster_id"]}: {cluster["name"]}'
    if 'groups' in to_delete:
        deletion_warning += '\nGroups:'
        for group in to_delete['groups']:
            deletion_warning += f'\n\t{group}'
    if 'permissions' in to_delete:
        deletion_warning += '\nAcls:'
        for cluster_id, permission_list in to_delete['permissions'].items():
            deletion_warning += f'\n\t{cluster_id}:'
            for permission in permission_list:
                deletion_warning += f'\n\t\t{(permission["principal"]+":").ljust(30)}{permission["permissions"]}'
    deletion_warning = 'The following resources will be deleted:' + deletion_warning
    if args.d:
        print(deletion_warning)

    # Delete items
    elif to_delete and (args.q or input(deletion_warning + '\n(Y/N):').upper() == 'Y'):
        for cluster_id in to_delete.get('permissions', {}):
            # Remove permissions
            set_acls({}, cluster_id, base_config)
        for group in to_delete.get('groups', []):
            # Remove the existing group
            delete_group(group, profile)
        for cluster in to_delete.get('clusters', []):
            # Delete the cluster
            cluster_id = cluster['cluster_id']
            cluster_name = cluster['name']
            delete_cluster(cluster_id, cluster_name, profile)
