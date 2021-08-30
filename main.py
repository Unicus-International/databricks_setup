import argparse


parser = argparse.ArgumentParser(description='CLI for helping set up databricks.')
parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
subparsers = parser.add_subparsers(help='Sub commands')

cluster_parser = subparsers.add_parser('cluster', help='Cluster commands')
cluster_parser.add_argument('--profile', type=str, help='The databricks cli profile to use')
cluster_parser.add_argument('--name', type=str, help='The the cluster name')

if __name__ == '__main__':
    args = parser.parse_args()
    print(args)
