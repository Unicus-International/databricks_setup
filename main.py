import argparse


parser = argparse.ArgumentParser(description='CLI for helping set up databricks.')

if __name__ == '__main__':
    args = parser.parse_args()
    print(args)
