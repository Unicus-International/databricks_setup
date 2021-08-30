import logging
import subprocess

logger = logging.getLogger(__name__)


def create_scope(scope: str, resource_id: str, key_vault_name: str):
    """Function for creating a secret scope from databricks

    :param str scope: The scope to delete
    :param str resource_id: The resource id of the key vault
    :param str key_vault_name: The key vault to add
    """
    # Create the scope
    logger.info(f'Creating secret scope: {scope}')
    create_query = 'databricks secrets create-scope --profile AAD'
    create_query += f' --scope {scope}'
    create_query += f' --scope-backend-type AZURE_KEYVAULT'
    create_query += f' --resource-id {resource_id}'
    create_query += f' --dns-name https://{key_vault_name}.vault.azure.net/'

    # Run and enforce success
    sp = subprocess.run(create_query, capture_output=True)
    sp.check_returncode()
