import asyncio

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
        parse_payload(job.payload)
        return True

    @pgq.entrypoint("failing")
    async def failing_entrypoint(job: Job):
        await asyncio.sleep(1)
        print("failing")
        assert False

    @pgq.entrypoint("slow_cancelable")
    async def slow_cancelable(job: Job):
        # this is a job that may be cancelled
        await asyncio.sleep(5)
        return "no"

    @pgq.entrypoint("slow_non_cancelable", cancelable=False)
    async def slow_non_cancelable(job: Job):
        # this is a job that should not be cancelled
        await asyncio.sleep(5)
        return "yes"

    @pgq.entrypoint("crashable", crashable=True)
    async def crashable_entrypoint(job: Job):
        print("failing but that's ok")
        raise ValueError("This is fine")

    # todo: some test with the job.payload

    pgq.entrypoint_pipeline("working_pipeline", basic_entrypoint, [slow_cancelable, slow_non_cancelable])

    pgq.entrypoint_pipeline(
        "failing_pipeline",
        [
            [crashable_entrypoint, basic_entrypoint],
            (
                slow_cancelable,
                slow_non_cancelable,
                failing_entrypoint,
            ),
        ],
    )

    pgq.entrypoint_pipeline(
        "fault_tolerant_pipeline",
        basic_entrypoint,
        [
            slow_cancelable,
            slow_non_cancelable,
            crashable_entrypoint,
        ],
    )

    @pgq.entrypoint("access_pipeline")
    async def access_pipeline(job: Job):
        payload = parse_payload(job.payload)

        pipeline = payload.get("pipeline")

        assert pipeline
        assert pipeline["name"] == "meta_pipeline"
        assert pipeline["steps"][0] == "access_pipeline"

    pgq.entrypoint_pipeline("meta_pipeline", access_pipeline, [access_pipeline, access_pipeline])

    print("listening", pgq.channel)
    return pgq
