"""One bounded expiry/scheduling/reconciliation cycle; no blind mutation retry."""
import argparse
from retry_window_lab.runtime_config import application_conninfo
from retry_window_lab.events import configure_event_output, emit_event
from retry_window_lab.ownership import OwnershipRepository
from retry_window_lab.recovery import RecoveryRepository,RecoveryService


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit',type=int,default=100)
    args=parser.parse_args()
    configure_event_output()
    conninfo=application_conninfo()
    expired=OwnershipRepository(conninfo).expire(args.limit)
    repository=RecoveryRepository(conninfo)
    scheduled=repository.schedule_due(args.limit)
    status=RecoveryService(repository).run_once()
    emit_event('recovery', expired=expired, scheduled=scheduled, status=status)


if __name__=='__main__':
    main()
