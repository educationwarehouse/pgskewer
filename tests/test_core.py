import os
import time
from typing import Optional

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
            db.executesql("SELECT id FROM pgqueuer_results;")
            break
        except Exception as e:
            print(f"db still starting, waiting 1s; {type(e)} {str(e)}")
            time.sleep(1)
    else:
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


def wait(limit: int = 100, interval=1):
    count = 0
    while count < limit:
        time.sleep(interval)
        count += 1
        yield count


def test_basic_consumer(db: DAL):
    job_id = enqueue(db, "basic", {})

    for iteration in wait(3):
        queue_rows = db.executesql(f"SELECT * FROM pgqueuer WHERE id = {job_id}")
        successful_logs = db.executesql(f"SELECT * FROM pgqueuer_log WHERE job_id = {job_id} AND status = 'successful'")
        result_rows = db.executesql(f"SELECT * FROM pgqueuer_results WHERE job_id = {job_id}")

        print(
            f"Iteration {iteration}: queue={len(queue_rows)}, successful_logs={len(successful_logs)}, results={len(result_rows)}"
        )

        # Check for good result:
        # - pgqueuer has no rows anymore (job removed from queue)
        # - pgqueuer_log has an entry with status = 'successful'
        # - pgqueuer_results has exactly 1 entry
        if len(queue_rows) == 0 and len(successful_logs) > 0 and len(result_rows) == 1:
            print(f"Job {job_id} completed successfully!")
            return  # Test passes - exit early on success

    # If we reach here, the loop completed without finding success
    pytest.fail(f"Job {job_id} did not complete successfully within 10 seconds timeout")


def test_breaking_consumer(db):
    job_id = enqueue(db, "failing", {})

    for iteration in wait(3):
        queue_rows = db.executesql(f"SELECT * FROM pgqueuer WHERE id = {job_id}")
        exception_logs = db.executesql(f"SELECT * FROM pgqueuer_log WHERE job_id = {job_id} AND status = 'exception'")
        exception_results = db.executesql(
            f"SELECT * FROM pgqueuer_results WHERE job_id = {job_id} AND status = 'exception'"
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
    pytest.fail(f"Job {job_id} did not fail as expected within 10 seconds timeout")


def test_nonexistent_consumer(db):
    job_id = enqueue(db, "fake", {})

    for iteration in wait(3):
        queue_rows = db.executesql(f"SELECT * FROM pgqueuer WHERE id = {job_id}")
        log_rows = db.executesql(f"SELECT * FROM pgqueuer_log WHERE job_id = {job_id}")
        result_rows = db.executesql(f"SELECT * FROM pgqueuer_results WHERE job_id = {job_id}")

        print(f"Iteration {iteration}: queue={len(queue_rows)}, logs={len(log_rows)}, results={len(result_rows)}")

    # After timeout, check final state:
    # - pgqueuer row should still exist (job never processed)
    # - no pgqueuer_log or pgqueuer_result should exist
    final_queue_rows = db.executesql(f"SELECT * FROM pgqueuer WHERE id = {job_id}")
    final_log_rows = db.executesql(f"SELECT * FROM pgqueuer_log WHERE job_id = {job_id}")
    final_result_rows = db.executesql(f"SELECT * FROM pgqueuer_results WHERE job_id = {job_id}")

    if len(final_queue_rows) > 0 and len(final_log_rows) == 1 and len(final_result_rows) == 0:
        print(f"Job {job_id} remained unprocessed as expected!")
        return  # Test passes - job was never picked up

    pytest.fail(f"Job {job_id} was unexpectedly processed or state is incorrect")


def test_basic_pipeline(db): ...


def test_breaking_pipeline(db): ...


def test_fault_tolerant_pipeline(db): ...
