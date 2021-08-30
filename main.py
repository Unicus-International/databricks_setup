import argparse
import subprocess
import json
from databricks_cli.configure.provider import ProfileConfigProvider, DEFAULT_SECTION

parser = argparse.ArgumentParser(description='CLI for helping set up databricks.')
parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
subparsers = parser.add_subparsers(help='Sub commands')

cluster_parser = subparsers.add_parser('cluster', help='Cluster commands')
cluster_parser.set_defaults(which='cluster')

required_args = cluster_parser.add_argument_group('required arguments')
cluster_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
required_args.add_argument('--name', type=str, help='The the cluster name', required=True)

scope_parser = subparsers.add_parser('scope', help='Secret scope commands')
scope_parser.set_defaults(which='scope')
required_args = scope_parser.add_argument_group('required arguments')
scope_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
required_args.add_argument('--key-vault', type=str, help='The the key vault name', required=True)
required_args.add_argument('--resource-id', type=str, help='The the key vault resource id', required=True)


if __name__ == '__main__':
    args = parser.parse_args()

    # Query what groups are available
    group_query = 'databricks groups list'

    # Update the cli call with the appropriate profile
    if args.profile is not None:
        group_query += f' --profile {args.profile}'
        profile = args.profile
    else:
        profile = DEFAULT_SECTION

    # Get the profile for extraneous requests
    cfg = ProfileConfigProvider(profile).get_config()
    if cfg is None:
        raise EnvironmentError(f'The profile {profile} has not been configured please add it to the databricks cli.')

    # Run the group query
    sp = subprocess.run(group_query, capture_output=True)
    sp.check_returncode()
    groups = json.loads(sp.stdout)
    print(args)
    print(cfg.__dict__)
