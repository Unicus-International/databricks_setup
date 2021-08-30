import subprocess
from typing import Dict

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
