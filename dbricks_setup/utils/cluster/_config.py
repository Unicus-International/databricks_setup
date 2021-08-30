from typing import Dict

from ._extract import extract_spark


def create_config(cluster_name: str, profile: str) -> Dict:
    """Get the current spark version from the configured workspace

    :param str cluster_name: The name of the cluster
    :param str profile: The profile configured for the workspace

    :return: The current spark version
    :rtype: Dict
    """

    last_version = extract_spark(profile)

    # Set the cluster json
    cluster_config = {
        "cluster_name": cluster_name,
        "spark_version": last_version['key'],
        "spark_conf": {
            "spark.databricks.delta.preview.enabled": "true"
        },
        "node_type_id": "Standard_DS3_v2",
        "driver_node_type_id": "Standard_DS3_v2",
        "autotermination_minutes": 60,
        "enable_elastic_disk": True,
        "disk_spec": {},
        "azure_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK_AZURE",
            "spot_bid_max_price": -1.0
        },
        "instance_source": {
            "node_type_id": "Standard_DS3_v2"
        },
        "driver_instance_source": {
            "node_type_id": "Standard_DS3_v2"
        },
        "autoscale": {
            "min_workers": 1,
            "max_workers": 2
        },
    }

    return cluster_config
