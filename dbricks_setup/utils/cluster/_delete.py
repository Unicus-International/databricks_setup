import logging
import subprocess

logger = logging.getLogger(__name__)


def terminate_cluster(cluster_id: str, cluster_name: str, profile: str):
    """Function for deleting a secret scope from databricks

    :param str cluster_id: The id of the cluster
    :param str cluster_name: The name of the cluster
    :param str profile: The profile configured for the workspace
    """
    # Terminate the cluster
    terminate_query = 'databricks clusters delete'
    terminate_query += f' --profile {profile}'
    terminate_query += f' --cluster-id {cluster_id}'

    # Run and enforce success
    logging.warning(f'Terminating cluster {cluster_name} with id {cluster_id}')
    sp = subprocess.run(terminate_query, capture_output=True)
    sp.check_returncode()
