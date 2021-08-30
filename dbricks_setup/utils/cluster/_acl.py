import json
import subprocess
from typing import Dict

import requests
from databricks_cli.configure.provider import DatabricksConfig

import logging

logger = logging.getLogger(__name__)


def get_acls(scope: str, profile: str) -> Dict[str, str]:
    """Get the list of acls from the supplied secret scope

    :param str scope: The scope to extract from
    :param str profile: The profile configured for the workspace

    :return: The available groups
    :rtype: Dict[str, str]
    """

    # Get the acls for the scope
    acl_query = 'databricks secrets list-acls'
    acl_query += f' --profile {profile}'
    acl_query += f' --scope {scope}'

    # Run and enforce success
    sp = subprocess.run(acl_query, capture_output=True)
    sp.check_returncode()

    # Extract the existing scopes
    acl_lines = [l.strip('\r') for l in sp.stdout.decode().split('\n')[1:]]
    acl_lines = [l for l in acl_lines if l.replace('-', '').strip()]
    acl_lines = [[elem for elem in l.split(' ') if elem] for l in acl_lines]

    # Turn acls int a dictionary
    existing_acls = {acl[0]: acl[1] for acl in acl_lines}

    return existing_acls


def add_acl(group: str, permission: str, scope: str, profile: str):
    """Add an acl to the supplied secret scope

    :param str group: The group for which to add the permission
    :param str permission: The permission to add
    :param str scope: The scope to extract from
    :param str profile: The profile configured for the workspace
    """
    # Add the acl
    acl_query = 'databricks secrets put-acl'
    acl_query += f' --profile {profile}'
    acl_query += f' --scope {scope}'
    acl_query += f' --principal {group}'
    acl_query += f' --permission {permission}'

    # Run and enforce success
    logging.info(f'Adding {permission} to {scope} for {group}')
    sp = subprocess.run(acl_query, capture_output=True)
    sp.check_returncode()


def delete_acl(group: str, scope: str, profile: str):
    """Remove an acl to the supplied secret scope

    :param str group: The group for which to remove the permission
    :param str scope: The scope to extract from
    :param str profile: The profile configured for the workspace
    """
    # Remove the existing acl
    acl_query = 'databricks secrets delete-acl'
    acl_query += f' --profile {profile}'
    acl_query += f' --scope {scope}'
    acl_query += f' --principal {group}'

    # Run and enforce success
    logging.warning(f'Removing existing acl to {scope} for {group}')
    sp = subprocess.run(acl_query, capture_output=True)
    sp.check_returncode()


def set_acls(desired_acls: Dict[str, str], cluster_id: str, base_config: DatabricksConfig) -> Dict[str, str]:
    """Enforce the list of acls for the supplied secret scope

    :param Dict[str, str] desired_acls: The acls to add to the scope
    :param str cluster_id: The id of the cluster in question
    :param DatabricksConfig base_config: The profile configured for the workspace
    """

    # Access the permissions api
    api_version = '/api/2.0'
    headers = {'Authorization': f"Bearer {base_config.token}"}

    # Set the permissions
    permissions = {
        'access_control_list': [
            {
                'group_name': group,
                'permission_level': permission
            }
            for group, permission
            in desired_acls.items()
        ]
    }

    # Generate the url
    api_command = f'/permissions/clusters/{cluster_id}'
    url = f"{base_config.host.rstrip('/')}{api_version}{api_command}"

    # Update the acls
    r = requests.put(
        url=url,
        headers=headers,
        json=permissions
    )
    logger.info(f'Permissions updated to {json.dumps(r.json(), indent=2)}')
