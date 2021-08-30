import argparse
import subprocess
import json
from databricks_cli.configure.provider import ProfileConfigProvider, DEFAULT_SECTION

parser = argparse.ArgumentParser(description='CLI for helping set up databricks.')
parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
subparsers = parser.add_subparsers(help='Sub commands')

cluster_parser = subparsers.add_parser('cluster', help='Cluster commands')
cluster_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
cluster_parser.add_argument('--name', type=str, help='The the cluster name')

scope_parser = subparsers.add_parser('scope', help='Secret scope commands')
scope_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
scope_parser.add_argument('--key-vault', type=str, help='The the key vault name')
scope_parser.add_argument('--resource-id', type=str, help='The the key vault resource id')


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
