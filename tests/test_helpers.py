import asyncio

import pytest
from typedal import TypeDAL

from src.pgskewer import safe_json, unblock

pytestmark = pytest.mark.anyio


def test_safe_json():
    # validate the examples in the safe_json docstring:
    assert safe_json('{"key": "value"}') == safe_json(b'{"key": "value"}') == {"key": "value"}
    assert safe_json("invalid json") is None
    assert safe_json(None) is None


def time_blocker(duration):
    import time

    time.sleep(duration)
    return duration


def db_blocker():
    db = TypeDAL("sqlite:memory")

    try:
        db.executesql("select 1")
        return 1
    except Exception:
        return 0
    finally:
        db.close()


class UnblockTestError(RuntimeError):
    pass


def fail_blocker():
    raise UnblockTestError("boom from worker")


def sleep_blocker(duration):
    import time

    time.sleep(duration)
    return duration


async def test_unblock():
    time_result = await unblock(time_blocker, 2)
    assert time_result == 2

    db_result = await unblock(db_blocker)
    assert db_result == 1


async def test_unblock_local_callable():
    # Local callables are not importable by module path in worker processes.
    # unblock() should serialize and execute them anyway.
    def local_blocker(value):
        return value + 1

    assert await unblock(local_blocker, 41) == 42
    assert await unblock(local_blocker, 41, logs=False) == 42


async def test_unblock_forwards_exceptions():
    with pytest.raises(UnblockTestError, match="boom from worker"):
        await unblock(fail_blocker)

    with pytest.raises(UnblockTestError, match="boom from worker"):
        await unblock(fail_blocker, logs=False)


async def test_unblock_exception_contains_worker_traceback():
    with pytest.raises(UnblockTestError) as exc_info:
        await unblock(fail_blocker)

    notes = getattr(exc_info.value, "__notes__", [])
    joined = "\n".join(notes)
    assert "Remote traceback from unblock worker:" in joined
    assert "fail_blocker" in joined


async def test_unblock_cancel_propagates():
    task = asyncio.create_task(unblock(sleep_blocker, 5000))
    await asyncio.sleep(0.1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
