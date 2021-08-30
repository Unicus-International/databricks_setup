import argparse


parser = argparse.ArgumentParser(description='CLI for helping set up databricks.')
parser.add_argument('--profile', type=str, help='The databricks cli profile to use')

if __name__ == '__main__':
    args = parser.parse_args()
    print(args)
