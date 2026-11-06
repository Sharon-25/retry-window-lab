"""Operator-only controls. No control route is exposed by the provider HTTP API."""
import argparse
import json
from uuid import UUID

from retry_window_lab.config import postgres_conninfo
from retry_window_lab.provider import FAULTS, ProviderRepository


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    create = commands.add_parser('create')
    create.add_argument('--profile', choices=['authoritative','delayed','unavailable'], default='authoritative')
    create.add_argument('--window-seconds', type=int, default=60)
    create.add_argument('--visibility-delay', type=int, default=10)
    advance = commands.add_parser('advance')
    advance.add_argument('experiment_id', type=UUID)
    advance.add_argument('--seconds', type=int, required=True)
    inject = commands.add_parser('inject')
    inject.add_argument('experiment_id', type=UUID)
    inject.add_argument('--fault', choices=sorted(FAULTS), required=True)
    inject.add_argument('--delay-seconds', type=int, default=0)
    args = parser.parse_args()
    repository = ProviderRepository(postgres_conninfo())
    if args.command == 'create':
        experiment_id = repository.create_experiment(args.profile,args.window_seconds,args.visibility_delay)
        print(json.dumps({'experiment_id': str(experiment_id), 'profile': args.profile}))
    elif args.command == 'advance':
        print(json.dumps(repository.advance(args.experiment_id,args.seconds)))
    else:
        repository.inject(args.experiment_id,args.fault,args.delay_seconds)
        print(json.dumps({'event':'fault_configured','fault':args.fault}))


if __name__ == '__main__':
    main()
