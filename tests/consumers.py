from edwh_migrate import activate_migrations

from pgskewer import ImprovedQueuer, Job, parse_payload
from pgskewer.migrations import noop


async def main():
    noop()
    activate_migrations()

    pgq = await ImprovedQueuer.from_env()

    @pgq.entrypoint("basic")
    async def basic_entrypoint(job: Job):
        print("basic")
        assert parse_payload(job.payload) == {}
        return True

    @pgq.entrypoint("failing")
    async def failing_entrypoint(job: Job):
        print("failing")
        assert parse_payload(job.payload) != {}
        return True

    print("listening", pgq.channel)
    return pgq
