import json
import subprocess
from typing import Dict

import requests
from databricks_cli.configure.provider import DatabricksConfig

import logging

logger = logging.getLogger(__name__)


def get_acls(cluster_id: str, base_config: DatabricksConfig) -> Dict[str, str]:
    """Get the list of acls for the supplied cluster

    :param str cluster_id: The id of the cluster in question
    :param DatabricksConfig base_config: The profile configured for the workspace
    """

    # Access the permissions api
    api_version = '/api/2.0'
    headers = {'Authorization': f"Bearer {base_config.token}"}

    # Generate the url
    api_command = f'/permissions/clusters/{cluster_id}'
    url = f"{base_config.host.rstrip('/')}{api_version}{api_command}"

    # Update the acls
    r = requests.get(
        url=url,
        headers=headers,
    )

    return r.json()


def set_acls(desired_acls: Dict[str, str], cluster_id: str, base_config: DatabricksConfig) -> Dict[str, str]:
    """Enforce the list of acls for the supplied secret scope

    :param Dict[str, str] desired_acls: The acls to add to the cluster
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
