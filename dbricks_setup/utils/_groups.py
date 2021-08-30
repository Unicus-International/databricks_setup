import json
import subprocess
from typing import List

import logging

logger = logging.getLogger(__name__)


def get_groups(profile: str) -> List[str]:
    """Get the list of groups from the configured workspace

    :param str profile: The profile configured for the workspace

    :return: The available groups
    :rtype: List[str]
    """

    # Query what groups are available
    logger.info('Extracting group information')
    group_query = f'databricks groups list --profile {profile}'

    # Run the group query
    sp = subprocess.run(group_query, capture_output=True)
    sp.check_returncode()
    groups = json.loads(sp.stdout).get('group_names', [])

    return groups


def create_groups(groups: List[str], profile: str):
    """Create a set of groups in the configured workspace

    :param List[str] groups: The list of groups to create
    :param str profile: The profile configured for the workspace
    """
    if groups:
        logger.info(f'Creating groups: {groups}')
    for group in groups:
        create_group(group, profile)


def create_group(group: str, profile: str):
    """Create a set of groups in the configured workspace

    :param str group: The group to create
    :param str profile: The profile configured for the workspace
    """
    logging.info(f'Creating Group: {group}')
    create_query = f'databricks groups create --profile {profile}'
    create_query += f' --group-name {group}'

    # Run and enforce success
    sp = subprocess.run(create_query, capture_output=True)
    sp.check_returncode()
