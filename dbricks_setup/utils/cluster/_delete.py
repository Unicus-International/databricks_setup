import logging
import subprocess

logger = logging.getLogger(__name__)


def terminate_cluster(cluster_id: str, cluster_name: str, profile: str):
    """Function for terminating a cluster

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


def delete_cluster(cluster_id: str, cluster_name: str, profile: str):
    """Function for delete a cluster

    :param str cluster_id: The id of the cluster
    :param str cluster_name: The name of the cluster
    :param str profile: The profile configured for the workspace
    """
    # Terminate the cluster
    delete_query = 'databricks clusters permanent-delete'
    delete_query += f' --profile {profile}'
    delete_query += f' --cluster-id {cluster_id}'

    # Run and enforce success
    logging.warning(f'Deleting cluster {cluster_name} with id {cluster_id}')
    sp = subprocess.run(delete_query, capture_output=True)
    sp.check_returncode()
