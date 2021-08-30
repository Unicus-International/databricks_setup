import json
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


def extract_spark(profile: str) -> Dict[str, str]:
    """Get the current spark version from the configured workspace

    :param str profile: The profile configured for the workspace

    :return: The current spark version
    :rtype: Dict[str, str]
    """

    # Get the current spark versions
    spark_query = 'databricks clusters spark-versions'
    spark_query += f' --profile {profile}'

    # Run and enforce success
    sp = subprocess.run(spark_query, capture_output=True)
    sp.check_returncode()

    # Filter the spark versions
    spark_versions = json.loads(sp.stdout)['versions']
    valid_versions = [
        v
        for v
        in spark_versions
        if (
                'gpu' not in v['key']
                and 'cpu' not in v['key']
                and 'photon' not in v['key']
                and 'apache' not in v['key']
        )
    ]
    valid_versions = sorted(valid_versions, key=lambda x: x['key'])

    # Get the last version
    last_version = valid_versions[-1]

    return last_version
