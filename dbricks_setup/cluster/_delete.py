from argparse import Namespace

import logging

from ..utils._groups import delete_group, get_groups
from ..utils._profile import extract_profile
from ..utils.scope._acl import get_acls, delete_acl
from ..utils.scope._delete import delete_scope
from ..utils.scope._extract import extract_scopes

logger = logging.getLogger(__name__)


def delete_scope_cli(args: Namespace):
    """Updates the secret scope configuration of the databricks instance defined in the current profile

    :param Namespace args: The arguments from the cli
    :return:
    """

    # Get the base profile
    profile, base_config = extract_profile(args)

    # Get the workspace groups
    groups = get_groups(profile)

    # Get the existing scopes
    scopes = extract_scopes(profile)

    # Check scope name
    scope_name = args.scope_name
    scope_exists = scope_name in scopes

    # Construct the access groups
    accesses = ['read', 'write', 'manage']
    access_groups = {
        f'scope-{scope_name}-{access}': access.upper()
        for access in accesses
    }

    # Filter the existing groups
    existing_groups = [group for group in access_groups if group in groups]

    # Get the acls if the scope exists
    if scope_exists:
        # Get the acls for the scope
        acls = get_acls(scope_name, profile)
    else:
        acls = {}

    # Set deletions
    to_delete = {
        'scope': scope_name,
        'groups': existing_groups,
        'acls': acls
    }

    # Verify deletion parameters
    if (not args.a and not args.s) or not scope_exists:
        to_delete.pop('scope')
    if (not args.a and not args.g) or not existing_groups:
        to_delete.pop('groups')
    if (not args.a and not args.c) or not acls:
        to_delete.pop('acls')

    # Set the deletion warning
    deletion_warning = ''
    if 'scope' in to_delete:
        deletion_warning += '\nScope:'
        deletion_warning += f'\n\t{to_delete["scope"]}'
    if 'groups' in to_delete:
        deletion_warning += '\nGroups:'
        for group in to_delete['groups']:
            deletion_warning += f'\n\t{group}'
    if 'acls' in to_delete:
        deletion_warning += '\nAcls:'
        for acl, permission in to_delete['acls'].items():
            deletion_warning += f'\n\t{(permission+":").ljust(8)}{acl}'

    deletion_warning = 'The following resources will be deleted:' + deletion_warning

    # Provide the debug output
    if args.d:
        print(deletion_warning)

    # Check for confirmation
    elif to_delete and (args.q or input(deletion_warning + '\n(Y/N):').upper() == 'Y'):
        for principal in to_delete.get('acls', []):
            # Remove the existing acl
            delete_acl(principal, scope_name, profile)
        for group in to_delete.get('groups', []):
            # Remove the existing group
            delete_group(group, profile)
        if 'scope' in to_delete:
            # Delete the scope
            delete_scope(scope_name, profile)
