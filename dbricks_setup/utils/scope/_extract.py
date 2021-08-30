import subprocess
from typing import Dict

import logging

logger = logging.getLogger(__name__)


def extract_scopes(profile: str) -> Dict[str, Dict[str, str]]:
    """Get the list of secret scopes from the configured workspace

    :param str profile: The profile configured for the workspace

    :return: The available secret scopes with the names as the dictionary keys
    :rtype: Dict[str, Dict[str, str]]
    """

    # Construct the query
    logger.info('Extracting scope information')
    scope_query = 'databricks secrets list-scopes'
    scope_query += f' --profile {profile}'

    # Run and verify output
    sp = subprocess.run(scope_query, capture_output=True)
    sp.check_returncode()

    # Extract the existing scopes
    scope_lines = [l.strip('\r') for l in sp.stdout.decode().split('\n')[1:]]
    scope_lines = [l for l in scope_lines if l.replace('-', '').strip()]
    scope_lines = [[elem for elem in l.split(' ') if elem] for l in scope_lines]

    # Convert to a nested dictionary
    existing_scopes = {scope[0]: {'backend': scope[1], 'url': scope[2]} for scope in scope_lines}

    return existing_scopes
