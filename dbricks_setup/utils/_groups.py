import json
import subprocess
from typing import List


def get_groups(profile: str) -> List[str]:
    """Get the list of groups from the configured workspace

    :param str profile: The profile configured for the workspace

    :return: The available groups
    :rtype: List[str]
    """

    # Query what groups are available
    group_query = f'databricks groups list --profile {profile}'

    # Run the group query
    sp = subprocess.run(group_query, capture_output=True)
    sp.check_returncode()
    groups = json.loads(sp.stdout).get('group_names', [])

    return groups
