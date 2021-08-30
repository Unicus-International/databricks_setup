import subprocess
from typing import Dict, List

import logging

logger = logging.getLogger(__name__)


def extract_clusters(profile: str) -> List[Dict[str, str]]:
    """Get the list of clusters from the configured workspace

    :param str profile: The profile configured for the workspace

    :return: The list of clusters in the workspace
    :rtype: List[Dict[str, str]]
    """

    # Construct the query
    logger.info('Extracting scope information')
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

    #
    existing_clusters = [{'cluster_id': cluster[0], 'name': cluster[1], 'status': cluster[2]} for cluster in
                         cluster_lines]

    return existing_clusters
