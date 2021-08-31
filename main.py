import argparse
import datetime
import os
import subprocess
import json
import logging
import requests
from databricks_cli.configure.provider import ProfileConfigProvider, DEFAULT_SECTION, update_and_persist_config, \
    DatabricksConfig

logging.basicConfig(level=logging.INFO)

# Top level
parser = argparse.ArgumentParser(description='CLI for helping set up databricks.')
subparsers = parser.add_subparsers(help='Sub commands')

# Optional arguments
parser.add_argument('--profile', type=str, help='The databricks cli profile to use')

# cluster level commands
cluster_parser = subparsers.add_parser(
    'cluster',
    help='Cluster commands',
    description='Cluster commands'
)
cluster_parser.set_defaults(which='cluster')
cluster_subparsers = cluster_parser.add_subparsers(help='Sub commands')

# Optional arguments
cluster_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')

# cluster create commands
cluster_update_parser = cluster_subparsers.add_parser(
    'update',
    help='cluster creation/update commands',
    description="Create and/or update configure a cluster, updates include rbac/acls for the cluster"
)
cluster_update_parser.set_defaults(which='cluster_update')

# Optional arguments
cluster_update_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
cluster_update_parser.add_argument('-r', action='store_true', help='Allow cluster to run after creation')

# Required arguments
required_args = cluster_update_parser.add_argument_group('required arguments')
required_args.add_argument('--name', type=str, help='The cluster name', required=True)

# cluster delete commands
cluster_delete_parser = cluster_subparsers.add_parser(
    'delete',
    help='Cluster deletion commands',
    description="Delete clusters and connected items"
)
cluster_delete_parser.set_defaults(which='cluster_delete')

# Optional arguments
cluster_delete_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
cluster_delete_parser.add_argument('-a', action='store_true', help='Delete all resources')
cluster_delete_parser.add_argument('-c', action='store_true', help='Delete control lists')
cluster_delete_parser.add_argument('-d', action='store_true', help='Debug, does not perform the deletes')
cluster_delete_parser.add_argument('-g', action='store_true', help='Delete groups')
cluster_delete_parser.add_argument('-q', action='store_true', help='Quiet')
cluster_delete_parser.add_argument('-s', action='store_true', help='Delete cluster')

# Required arguments
required_args = cluster_delete_parser.add_argument_group('required arguments')
required_args.add_argument('--name', type=str, help='Name override for cluster, case insensitive')

# scope level commands
scope_parser = subparsers.add_parser(
    'scope',
    help='Secret scope commands',
    description='Scope level commands'
)
scope_parser.set_defaults(which='scope')
scope_subparsers = scope_parser.add_subparsers(help='Sub commands')

# Optional arguments
scope_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')

# scope update commands
scope_update_parser = scope_subparsers.add_parser(
    'update',
    help='Secret scope configuration update commands',
    description="Update/create the configuration for a key vault backed secret scope"
)
scope_update_parser.set_defaults(which='scope_update')

# Optional arguments
scope_update_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
scope_update_parser.add_argument('--scope-name', type=str, help='Name override for the secret scope')
scope_update_parser.add_argument('-f', action='store_true', help='Force deletion of existing secret scope')

# Required arguments
required_args = scope_update_parser.add_argument_group('required arguments')
required_args.add_argument('--key-vault', type=str, help='The the key vault name', required=True)
required_args.add_argument('--resource-id', type=str, help='The the key vault resource id', required=True)

# scope delete commands
scope_delete_parser = scope_subparsers.add_parser(
    'delete',
    help='Secret scope deletion commands',
    description="Delete secret scopes and connected items"
)
scope_delete_parser.set_defaults(which='scope_delete')

# Optional arguments
scope_delete_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
scope_delete_parser.add_argument('-a', action='store_true', help='Delete all resources')
scope_delete_parser.add_argument('-c', action='store_true', help='Delete control lists')
scope_delete_parser.add_argument('-d', action='store_true', help='Debug, does not perform the deletes')
scope_delete_parser.add_argument('-g', action='store_true', help='Delete groups')
scope_delete_parser.add_argument('-q', action='store_true', help='Quiet')
scope_delete_parser.add_argument('-s', action='store_true', help='Delete scope')

# Required arguments
required_args = scope_delete_parser.add_argument_group('required arguments')
required_args.add_argument('--scope-name', type=str, help='Name override for the secret scope')

if __name__ == '__main__':
    args = parser.parse_args()
    print(args)

    # Query what groups are available
    group_query = 'databricks groups list'

    # Update the cli call with the appropriate profile
    if args.profile is not None:
        group_query += f' --profile {args.profile}'
        profile = args.profile
    else:
        profile = DEFAULT_SECTION

    # Get the profile for extraneous requests
    base_cfg = ProfileConfigProvider(profile).get_config()
    if base_cfg is None:
        raise EnvironmentError(f'The profile {profile} has not been configured please add it to the databricks cli.')

    # Run the group query
    sp = subprocess.run(group_query, capture_output=True)
    sp.check_returncode()
    groups = json.loads(sp.stdout).get('group_names', [])

    if args.which == 'cluster_delete':
        cluster_name = args.name.lower()

        # Query what clusters exists
        cluster_query = 'databricks clusters list'
        cluster_query += f' --profile {profile}'

        # Run and enforce success
        sp = subprocess.run(cluster_query, capture_output=True)
        sp.check_returncode()

        # Extract the existing scopes
        cluster_lines = [l.strip('\r') for l in sp.stdout.decode().split('\n')]
        cluster_lines = [l for l in cluster_lines if l.replace('-', '').strip()]
        cluster_lines = [[elem for elem in l.split(' ') if elem] for l in cluster_lines]
        existing_clusters = [{'cluster_id': cluster[0], 'name': cluster[1], 'status': cluster[2]} for cluster in
                             cluster_lines]

        # Get the clusters matching the desired name
        matching_clusters = [
            cluster
            for cluster
            in existing_clusters
            if cluster['name'].lower() == cluster_name
        ]

        # Construct the access groups
        access_groups = {
            f'cluster-{cluster_name}-manage': 'CAN_MANAGE',
            f'cluster-{cluster_name}-restart': 'CAN_RESTART',
            f'cluster-{cluster_name}-attach': 'CAN_ATTACH_TO',
        }

        # Filter the existing groups
        existing_groups = [group for group in access_groups if group in groups]

        # Access the permissions api
        api_version = '/api/2.0'
        headers = {'Authorization': f"Bearer {base_cfg.token}"}

        permissions = {
        }

        # Get the existing permissions
        for cluster in matching_clusters:
            cluster_id = cluster['cluster_id']
            permissions[cluster_id] = []
            api_command = f'/permissions/clusters/{cluster_id}'
            url = f"{base_cfg.host.rstrip('/')}{api_version}{api_command}"
            r = requests.get(
                url=url,
                headers=headers
            )

            # Get the permissions for the cluster
            cluster_permissions = r.json()
            for acl in cluster_permissions['access_control_list']:
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
        elif to_delete and (args.q or input(deletion_warning + '\n(Y/N):').upper() == 'Y'):
            for cluster_id in to_delete.get('permissions', []):
                # Remove permissions
                logging.warning(f'Removing acls or {cluster_id}')
                api_command = f'/permissions/clusters/{cluster_id}'
                url = f"{base_cfg.host.rstrip('/')}{api_version}{api_command}"
                r = requests.put(
                    url=url,
                    headers=headers,
                    json={'access_control_list': []}
                )
            for group in to_delete.get('groups', []):
                # Remove the existing group
                group_query = 'databricks groups delete'
                group_query += f' --profile {profile}'
                group_query += f' --group-name {group}'

                # Run and enforce success
                logging.warning(f'Removing group {group}')
                sp = subprocess.run(group_query, capture_output=True)
                sp.check_returncode()
            for cluster in to_delete.get('clusters', []):
                # Delete the scope
                cluster_id = cluster['cluster_id']
                logging.info(f'Deleting cluster: {cluster_id}')
                delete_query = f'databricks clusters permanent-delete --profile {profile}'
                delete_query += f' --cluster-id {cluster_id}'

                # Run and enforce success
                logging.warning(f'Deleting cluster {cluster_id}')
                sp = subprocess.run(delete_query, capture_output=True)
                sp.check_returncode()
