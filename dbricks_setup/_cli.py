import argparse
import logging

from .scope._update import update_scope


def cli():
    """Wrapper around the cli
    :return:
    """
    logging.basicConfig(level=logging.INFO)

    # Top level
    parser = argparse.ArgumentParser(description='CLI for helping set up databricks.')
    parser.set_defaults(which='base')
    subparsers = parser.add_subparsers(help='Sub commands')

    # Optional arguments
    parser.add_argument('--profile', type=str, help='The databricks cli profile to use')

    # cluster level commands
    cluster_parser = subparsers.add_parser(
        'cluster',
        help='Cluster commands',
        description='Cluster commands'
    )
    cluster_parser.set_defaults(which='cluster')
    cluster_subparsers = cluster_parser.add_subparsers(help='Sub commands')

    # Optional arguments
    cluster_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')

    # cluster create commands
    cluster_update_parser = cluster_subparsers.add_parser(
        'update',
        help='cluster creation/update commands',
        description="Create and/or update configure a cluster, updates include rbac/acls for the cluster"
    )
    cluster_update_parser.set_defaults(which='cluster_update')

    # Optional arguments
    cluster_update_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
    cluster_update_parser.add_argument('-r', action='store_true', help='Allow cluster to run after creation')

    # Required arguments
    required_args = cluster_update_parser.add_argument_group('required arguments')
    required_args.add_argument('--name', type=str, help='The cluster name', required=True)

    # cluster delete commands
    cluster_delete_parser = cluster_subparsers.add_parser(
        'delete',
        help='Cluster deletion commands',
        description="Delete clusters and connected items"
    )
    cluster_delete_parser.set_defaults(which='cluster_delete')

    # Optional arguments
    cluster_delete_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
    cluster_delete_parser.add_argument('-a', action='store_true', help='Delete all resources')
    cluster_delete_parser.add_argument('-c', action='store_true', help='Delete control lists')
    cluster_delete_parser.add_argument('-d', action='store_true', help='Debug, does not perform the deletes')
    cluster_delete_parser.add_argument('-g', action='store_true', help='Delete groups')
    cluster_delete_parser.add_argument('-q', action='store_true', help='Quiet')
    cluster_delete_parser.add_argument('-s', action='store_true', help='Delete cluster')

    # Required arguments
    required_args = cluster_delete_parser.add_argument_group('required arguments')
    required_args.add_argument('--name', type=str, help='Name override for cluster, case insensitive')

    # scope level commands
    scope_parser = subparsers.add_parser(
        'scope',
        help='Secret scope commands',
        description='Scope level commands'
    )
    scope_parser.set_defaults(which='scope')
    scope_subparsers = scope_parser.add_subparsers(help='Sub commands')

    # Optional arguments
    scope_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')

    # scope update commands
    scope_update_parser = scope_subparsers.add_parser(
        'update',
        help='Secret scope configuration update commands',
        description="Update/create the configuration for a key vault backed secret scope"
    )
    scope_update_parser.set_defaults(which='scope_update')

    # Optional arguments
    scope_update_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
    scope_update_parser.add_argument('--scope-name', type=str, help='Name override for the secret scope')
    scope_update_parser.add_argument('-f', action='store_true', help='Force deletion of existing secret scope')

    # Required arguments
    required_args = scope_update_parser.add_argument_group('required arguments')
    required_args.add_argument('--key-vault', type=str, help='The the key vault name', required=True)
    required_args.add_argument('--resource-id', type=str, help='The the key vault resource id', required=True)

    # scope delete commands
    scope_delete_parser = scope_subparsers.add_parser(
        'delete',
        help='Secret scope deletion commands',
        description="Delete secret scopes and connected items"
    )
    scope_delete_parser.set_defaults(which='scope_delete')

    # Optional arguments
    scope_delete_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
    scope_delete_parser.add_argument('-a', action='store_true', help='Delete all resources')
    scope_delete_parser.add_argument('-c', action='store_true', help='Delete control lists')
    scope_delete_parser.add_argument('-d', action='store_true', help='Debug, does not perform the deletes')
    scope_delete_parser.add_argument('-g', action='store_true', help='Delete groups')
    scope_delete_parser.add_argument('-q', action='store_true', help='Quiet')
    scope_delete_parser.add_argument('-s', action='store_true', help='Delete scope')

    # Required arguments
    required_args = scope_delete_parser.add_argument_group('required arguments')
    required_args.add_argument('--scope-name', type=str, help='Name override for the secret scope')

    # Initialize the cli
    args = parser.parse_args()

    if args.which == 'scope_update':
        update_scope(args)


if __name__ == '__main__':
    cli()
