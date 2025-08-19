import dataclasses as dc
import datetime as dt
import json
import typing as t
import uuid

from edwh_uuid7 import uuid7
from pydal import DAL


def utcnow():
    return dt.datetime.now(dt.UTC)


@dc.dataclass
class EnqueuedJob:
    id: int
    key: uuid.UUID

    _db: DAL = None


def queue_job(
    db: DAL,
    entrypoint: str,
    payload: str | dict,
    priority: int = 10,
    execute_after: t.Optional[dt.datetime] = None,
    unique_key: str | uuid.UUID | None = None,
) -> EnqueuedJob:
    """
    Queue a job in the pgqueuer table and log it in pgqueuer_log.

    Parameters:
        db: A database connection object with an `executesql` method.
        entrypoint (str): The job entrypoint to execute.
        payload (dict): Payload for the job.
        priority (int, optional): Job priority (default is 10).
        execute_after (datetime, optional): When to execute the job. Defaults to datetime.now().
        unique_key: since job ids only live temporarily, these keys can be used to uniquely identify a run.

    Returns:
        int: The ID of the queued job.
    """
    unique_key = unique_key or uuid7()

    execute_after = execute_after or utcnow()

    # Insert the job
    result = db.executesql(
        f"""
        INSERT INTO pgqueuer
            (priority, entrypoint, payload, execute_after, dedupe_key, status)
        VALUES (%(priority)s,
                %(entrypoint)s,
                %(payload)s,
                %(execute_after)s,
                %(unique_key)s,
                'queued')
        RETURNING id;
    """,
        placeholders={
            "priority": priority,
            "entrypoint": entrypoint,
            "payload": payload if isinstance(payload, str) else json.dumps(payload),
            "unique_key": str(unique_key),
            "execute_after": execute_after,
        },
    )

    job_id = result[0][0]

    # Log the job
    db.executesql(
        """
        INSERT INTO pgqueuer_log
            (job_id, status, entrypoint, priority)
        VALUES (%(job_id)s,
                'queued',
                %(entrypoint)s,
                %(priority)s);
    """,
        placeholders={
            "job_id": job_id,
            "entrypoint": entrypoint,
            "priority": priority,
        },
    )

    db.commit()
    return EnqueuedJob(job_id, unique_key, db)


def safe_json(data: bytes | str | None) -> t.Any | None:
    """
    Safely parse JSON data with error handling.

    This function attempts to parse JSON data from bytes or string input,
    handling various error conditions gracefully by returning None instead
    of raising exceptions.

    Args:
        data: The data to parse. Can be bytes, string, or None.

    Returns:
        The parsed JSON object, or None if parsing fails or data is empty.

    Example:
        >>> safe_json('{"key": "value"}')
        {'key': 'value'}
        >>> safe_json(b'{"key": "value"}')
        {'key': 'value'}
        >>> safe_json('invalid json')
        None
        >>> safe_json(None)
        None
    """

    if not data:
        return None

    data = data.decode() if isinstance(data, bytes) else data

    try:
        return json.loads(data)
    except (TypeError, ValueError, json.decoder.JSONDecodeError):
        return None
