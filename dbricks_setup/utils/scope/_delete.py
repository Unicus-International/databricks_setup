import logging
import subprocess


def delete_scope(scope: str, profile: str):
    """Function for deleting a secret scope from databriks

    :param str scope: The scope to delete
    :param str profile: The profile configured for the workspace
    """
    # Delete the scope
    logging.warning(f'Deleting secret scope: {scope}')
    delete_query = f'databricks secrets delete-scope --profile {profile}'
    delete_query += f' --scope {scope}'

    # Run and enforce success
    sp = subprocess.run(delete_query, capture_output=True)
    sp.check_returncode()
