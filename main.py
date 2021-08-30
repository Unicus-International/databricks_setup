import argparse
import datetime
import os
import subprocess
import json

import logging
from databricks_cli.configure.provider import ProfileConfigProvider, DEFAULT_SECTION, update_and_persist_config, \
    DatabricksConfig

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description='CLI for helping set up databricks.')
parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
subparsers = parser.add_subparsers(help='Sub commands')

cluster_parser = subparsers.add_parser('cluster', help='Cluster commands')
cluster_parser.set_defaults(which='cluster')

required_args = cluster_parser.add_argument_group('required arguments')
cluster_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
required_args.add_argument('--name', type=str, help='The the cluster name', required=True)

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

    if args.which == 'scope_update':

        scope_query = 'databricks secrets list-scopes'

        # Update the cli call with the appropriate profile
        if args.profile is not None:
            scope_query += f' --profile {args.profile}'
        sp = subprocess.run(scope_query, capture_output=True)
        sp.check_returncode()

        # Extract the existing scopes
        scope_lines = [l.strip('\r') for l in sp.stdout.decode().split('\n')[1:]]
        scope_lines = [l for l in scope_lines if l.replace('-', '').strip()]
        scope_lines = [[elem for elem in l.split(' ') if elem] for l in scope_lines]
        existing_scopes = {scope[0]: {'backend': scope[1], 'url': scope[2]} for scope in scope_lines}

        # Check scope name
        scope_name = args.scope_name
        if not scope_name:
            scope_name = args.key_vault

        # Check scope existence
        if scope_name in existing_scopes and not args.f:
            logging.warning(
                f'Scope {scope_name} already exists. Please remove if misconfigured, consider using the -f flag to force an update.')
        else:
            create = args.f or scope_name not in existing_scopes
            if create:
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
                    token_data = json.load(f)
                    update_and_persist_config(
                        'AAD',
                        DatabricksConfig(
                            host=base_cfg.host,
                            username=None,
                            password=None,
                            token=token_data['accessToken'],
                            insecure=None)
                    )

                # Update the AAD profile and validate
                cfg = ProfileConfigProvider('AAD').get_config()
                if cfg is None:
                    raise EnvironmentError(
                        f'The profile AAD has not been configured please add it to the databricks cli.')

                # Delete in exists
                if scope_name in existing_scopes:
                    # Create the scope
                    logging.info(f'Deleting secret scope: {scope_name}')
                    delete_query = f'databricks secrets delete-scope --profile {profile}'
                    delete_query += f' --scope {scope_name}'

                    # Run and enforce success
                    sp = subprocess.run(delete_query, capture_output=True)
                    sp.check_returncode()

                # Create the scope
                logging.info(f'Creating secret scope: {scope_name}')
                create_query = 'databricks secrets create-scope --profile AAD'
                create_query += f' --scope {scope_name}'
                create_query += f' --scope-backend-type AZURE_KEYVAULT'
                create_query += f' --resource-id {args.resource_id}'
                create_query += f' --dns-name https://{args.key_vault}.vault.azure.net/'

                # Run and enforce success
                sp = subprocess.run(create_query, capture_output=True)
                sp.check_returncode()

        # Construct the access groups
        accesses = ['read', 'write', 'manage']
        access_groups = {
            f'scope-{scope_name}-{access}': access.upper()
            for access in accesses
        }

        # Filter the missing groups
        missing_groups = [group for group in access_groups if group not in groups]
        if missing_groups:
            logging.info(f'Creating groups: {missing_groups}')

        # Create groups
        for group in missing_groups:
            logging.info(f'Creating Group: {group}')
            create_query = f'databricks groups create --profile {profile}'
            create_query += f' --group-name {group}'

            # Run and enforce success
            sp = subprocess.run(create_query, capture_output=True)
            sp.check_returncode()

        # Get the acls for the scope
        acl_query = 'databricks secrets list-acls'
        acl_query += f' --profile {profile}'
        acl_query += f' --scope {scope_name}'

        # Run and enforce success
        sp = subprocess.run(acl_query, capture_output=True)
        sp.check_returncode()

        # Extract the existing scopes
        acl_lines = [l.strip('\r') for l in sp.stdout.decode().split('\n')[1:]]
        acl_lines = [l for l in acl_lines if l.replace('-', '').strip()]
        acl_lines = [[elem for elem in l.split(' ') if elem] for l in acl_lines]
        existing_acls = {acl[0]: acl[1] for acl in acl_lines}

        # Add the acls
        for group, permission in access_groups.items():
            # Add new groups
            if group not in existing_acls:
                # Add the acl
                acl_query = 'databricks secrets put-acl'
                acl_query += f' --profile {profile}'
                acl_query += f' --scope {scope_name}'
                acl_query += f' --principal {group}'
                acl_query += f' --permission {permission}'

                # Run and enforce success
                logging.info(f'Adding {permission} to {scope_name} for {group}')
                sp = subprocess.run(acl_query, capture_output=True)
                sp.check_returncode()

            # Update misconfigured acls
            elif existing_acls.get(group, None) != permission:
                # Remove the existing acl
                acl_query = 'databricks secrets delete-acl'
                acl_query += f' --profile {profile}'
                acl_query += f' --scope {scope_name}'
                acl_query += f' --principal {group}'

                # Run and enforce success
                logging.warning(f'Removing existing acl to {scope_name} for {group}')
                sp = subprocess.run(acl_query, capture_output=True)
                sp.check_returncode()

                # Add the acl
                acl_query = 'databricks secrets put-acl'
                acl_query += f' --profile {profile}'
                acl_query += f' --scope {scope_name}'
                acl_query += f' --principal {group}'
                acl_query += f' --permission {permission}'

                # Run and enforce success
                logging.info(f'Adding {permission} to {scope_name} for {group}')
                sp = subprocess.run(acl_query, capture_output=True)
                sp.check_returncode()

        # Clean up the access roles
        for principal in existing_acls:
            if principal not in access_groups:
                # Remove the acl
                acl_query = 'databricks secrets delete-acl'
                acl_query += f' --profile {profile}'
                acl_query += f' --scope {scope_name}'
                acl_query += f' --principal {principal}'

                # Run and enforce success
                logging.warning(f'Removing acl to {scope_name} for {principal}')
                sp = subprocess.run(acl_query, capture_output=True)
                sp.check_returncode()
