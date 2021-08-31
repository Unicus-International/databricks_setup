import json
import logging
import subprocess
from typing import Dict

logger = logging.getLogger(__name__)


def create_cluster(profile: str, cluster_config: Dict) -> str:
    """Function for creating a secret scope from databricks

    :param str profile: The profile configured for the workspace
    :param Dict cluster_config: The config of the cluster

    :returns: The cluster id of the new cluster
    :rtype: str
    """
    # Create the cluster
    cluster_str = json.dumps(cluster_config, ensure_ascii=False).replace('"', '\\"')
    create_query = 'databricks clusters create'
    create_query += f' --profile {profile}'
    create_query += f' --json "{cluster_str}"'

    # Run and enforce success
    logger.info(f'Creating cluster {cluster_config["cluster_name"]}')
    sp = subprocess.run(create_query, capture_output=True)
    sp.check_returncode()

    # Extract the cluster id
    cluster_id = json.loads(sp.stdout)['cluster_id']

    return cluster_id


def edit_cluster(profile: str, cluster_config: Dict):
    """Function for creating a secret scope from databricks

    :param str profile: The profile configured for the workspace
    :param Dict cluster_config: The config of the cluster

    :returns: The cluster id of the new cluster
    :rtype: str
    """
    # Create the cluster
    cluster_str = json.dumps(cluster_config, ensure_ascii=False).replace('"', '\\"')
    edit_query = 'databricks clusters edit'
    edit_query += f' --profile {profile}'
    edit_query += f' --json "{cluster_str}"'

    # Run and enforce success
    logger.info(f'Editing cluster {cluster_config["cluster_name"]}')
    sp = subprocess.run(edit_query, capture_output=True)
    sp.check_returncode()
