from edwh_migrate import activate_migrations

from pgskewer import ImprovedQueuer
from pgskewer.migrations import noop


async def main():
    activate_migrations()

    pgq = await ImprovedQueuer.from_env()

    return pgq
