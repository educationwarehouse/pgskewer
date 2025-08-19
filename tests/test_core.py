import os
import time

import pytest
from pydal import DAL

from src.pgskewer.helpers import queue_job as enqueue


def start_dc():
    os.system("cd tests; docker compose up -d")


def stop_dc():
    os.system("cd tests; docker compose down --timeout 0")


def connect_to_db() -> DAL:
    return DAL("postgres://test_user:test_password@localhost:55535/test_db")


@pytest.fixture(scope="session", autouse=True)
def docker_compose():
    start_dc()

    # now let's wait until healthy
    for _ in range(10):
        try:
            db = connect_to_db()
            db.executesql("SELECT id FROM pgqueuer_result;")
            break
        except Exception as e:
            print(f"db still starting, waiting 1s; {type(e)} {str(e)}")
            time.sleep(1)
    else:  # pragma: no cover
        print("db down too long, stopping")
        stop_dc()
        raise RuntimeError("db down too long, stopping")

    yield
    stop_dc()


@pytest.fixture()
def db():
    yield connect_to_db()


# for debug:
# watch -n 1 "docker compose logs --since 2s"
# watch -n 1 "docker compose logs --tail 3"

### start tests ###
DEFAULT_TIMEOUT = 3


def wait(limit: int = 100, interval=1):
    count = 0
    while count < limit:
        time.sleep(interval)
        count += 1
        yield count


def assert_job_succeeds(db: DAL, job_id: int, timeout_seconds: int = DEFAULT_TIMEOUT):
    """Assert that a job completes successfully within the timeout."""
    for iteration in wait(timeout_seconds):
        queue_rows = db.executesql(f"SELECT * FROM pgqueuer WHERE id = {job_id}")
        successful_logs = db.executesql(f"SELECT * FROM pgqueuer_log WHERE job_id = {job_id} AND status = 'successful'")
        result_rows = db.executesql(f"SELECT * FROM pgqueuer_result WHERE job_id = {job_id}")
        exception_logs = db.executesql(f"SELECT * FROM pgqueuer_log WHERE job_id = {job_id} AND status = 'exception'")

        print(
            f"Iteration {iteration}: queue={len(queue_rows)}, successful_logs={len(successful_logs)}, results={len(result_rows)}, exceptions={len(exception_logs)}"
        )

        # Check if job failed with exception
        if len(exception_logs) > 0:  # pragma: no cover
            print(exception_logs)
            pytest.fail(f"Job {job_id} failed with exception instead of succeeding")  # pragma: no cover

        # Check for good result:
        # - pgqueuer has no rows anymore (job removed from queue)
        # - pgqueuer_log has an entry with status = 'successful'
        # - pgqueuer_result has exactly 1 entry
        if len(queue_rows) == 0 and len(successful_logs) > 0 and len(result_rows) == 1:
            print(f"Job {job_id} completed successfully!")
            return  # Test passes - exit early on success

    # If we reach here, the loop completed without finding success
    pytest.fail(f"Job {job_id} did not complete successfully within timeout")  # pragma: no cover


def assert_job_fails(db: DAL, job_id: int, timeout_seconds: int = DEFAULT_TIMEOUT):
    """Assert that a job fails with an exception within the timeout."""
    for iteration in wait(timeout_seconds):
        queue_rows = db.executesql(f"SELECT * FROM pgqueuer WHERE id = {job_id}")
        exception_logs = db.executesql(f"SELECT * FROM pgqueuer_log WHERE job_id = {job_id} AND status = 'exception'")
        exception_results = db.executesql(
            f"SELECT * FROM pgqueuer_result WHERE job_id = {job_id} AND status = 'exception'"
        )

        print(
            f"Iteration {iteration}: queue={len(queue_rows)}, exception_logs={len(exception_logs)}, exception_results={len(exception_results)}"
        )

        # Check for breaking result:
        # - pgqueuer row shouldn't exist anymore
        # - pgqueuer_log with status exception should exist
        # - pgqueuer_result with status exception should exist
        if len(queue_rows) == 0 and len(exception_logs) > 0 and len(exception_results) > 0:
            print(f"Job {job_id} failed as expected!")
            return  # Test passes - job failed as expected

    # If we reach here, the loop completed without finding the expected failure
    pytest.fail(f"Job {job_id} did not fail as expected within timeout")  # pragma: no cover


def assert_job_times_out(db: DAL, job_id: int, timeout_seconds: int):
    """Assert that a job times out and remains unprocessed."""
    for iteration in wait(timeout_seconds):
        queue_rows = db.executesql(f"SELECT * FROM pgqueuer WHERE id = {job_id}")
        log_rows = db.executesql(f"SELECT * FROM pgqueuer_log WHERE job_id = {job_id}")
        result_rows = db.executesql(f"SELECT * FROM pgqueuer_result WHERE job_id = {job_id}")

        print(f"Iteration {iteration}: queue={len(queue_rows)}, logs={len(log_rows)}, results={len(result_rows)}")

    # After timeout, check final state:
    # - pgqueuer row should still exist (job never processed)
    # - no pgqueuer_log or pgqueuer_result should exist
    final_queue_rows = db.executesql(f"SELECT * FROM pgqueuer WHERE id = {job_id}")
    final_log_rows = db.executesql(f"SELECT * FROM pgqueuer_log WHERE job_id = {job_id}")
    final_result_rows = db.executesql(f"SELECT * FROM pgqueuer_result WHERE job_id = {job_id}")

    if len(final_queue_rows) > 0 and len(final_log_rows) == 1 and len(final_result_rows) == 0:
        print(f"Job {job_id} remained unprocessed as expected!")
        return  # Test passes - job was never picked up

    pytest.fail(f"Job {job_id} was unexpectedly processed or state is incorrect")  # pragma: no cover


def test_basic_consumer(db: DAL):
    job = enqueue(db, "basic", {})
    assert_job_succeeds(db, job.id, timeout_seconds=3)


def test_breaking_consumer(db):
    job = enqueue(db, "failing", {})
    assert_job_fails(db, job.id, timeout_seconds=3)


def test_nonexistent_consumer(db):
    job = enqueue(db, "fake", {})
    assert_job_times_out(db, job.id, timeout_seconds=3)


def test_basic_pipeline(db):
    payload = {"something": "unused"}
    job = enqueue(db, "working_pipeline", payload)
    assert_job_succeeds(db, job.id, timeout_seconds=10)

    data = db.executesql(f"""select result from pgqueuer_result where job_id = {job.id}""")[0][0]

    assert data["initial"] == payload
    assert data["tasks"]["basic"]["result"] is True
    assert data["tasks"]["slow_cancelable"]["result"] == "no"
    assert data["tasks"]["slow_non_cancelable"]["result"] == "yes"

    # todo: test more state of the substeps


def pipeline_job_ids(db: DAL, pipeline_id: int):
    log_data = db.executesql(
        f"SELECT traceback FROM pgqueuer_log WHERE job_id = %(job_id)s AND status = 'spawned'",
        placeholders=dict(job_id=pipeline_id),
        colnames=["job_ids"],
    )

    subjob_ids = set()

    for row in log_data:
        subjob_ids.update(row["job_ids"])

    return tuple(subjob_ids)


def test_breaking_pipeline(db):
    job = enqueue(db, "failing_pipeline", {})
    assert_job_fails(db, job.id, timeout_seconds=3)

    # wait for trailing task to complete:
    time.sleep(5)

    subjob_ids = pipeline_job_ids(db, job.id)

    def subjob_query(entrypoint: str | None):
        placeholders = {}
        if entrypoint:
            and_entrypoint = f"AND entrypoint = %(entrypoint)s"
            placeholders["entrypoint"] = entrypoint
        else:
            and_entrypoint = ""

        return db.executesql(
            f"""
                  SELECT id, entrypoint, status, ok, result, completed_at
                  FROM pgqueuer_result
                  WHERE job_id IN {subjob_ids}
                  {and_entrypoint}
                  ORDER BY id
                  """,
            placeholders=placeholders,
            colnames=("id", "entrypoint", "status", "ok", "result", "completed_at"),
        )

    assert len(subjob_query("failing")) < len(subjob_query(None))

    # todo: this SHOULD have a result:
    assert subjob_query("failing")[0]["ok"] is False
    assert subjob_query("slow_cancelable")[0]["ok"] is False
    slow_nc = subjob_query("slow_non_cancelable")[0]
    assert slow_nc["ok"] is True
    assert slow_nc["result"] == "yes"


def test_fault_tolerant_pipeline(db):
    job = enqueue(db, "fault_tolerant_pipeline", {})
    assert_job_succeeds(db, job.id, timeout_seconds=10)

    # todo: test more state of the substeps


def test_meta_pipeline(db):
    job = enqueue(db, "meta_pipeline", {})
    assert_job_succeeds(db, job.id, timeout_seconds=5)


# todo: pipeline timeouts
