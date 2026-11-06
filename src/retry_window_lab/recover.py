"""Mark expired attempts uncertain. Does not retry external operations."""
import argparse
import json
from retry_window_lab.config import postgres_conninfo
from retry_window_lab.ownership import OwnershipRepository


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=int, default=100)
    args = parser.parse_args()
    count = OwnershipRepository(postgres_conninfo()).expire(args.limit)
    print(json.dumps({'event': 'expired_attempts', 'count': count}))


if __name__ == '__main__':
    main()
