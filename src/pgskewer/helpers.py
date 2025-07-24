import datetime as dt
import json
import uuid
from typing import Optional

from pydal import DAL


def utcnow():
    return dt.datetime.now(dt.UTC)


def queue_job(
    db: DAL,
    entrypoint: str,
    payload: str | dict,
    priority: int = 10,
    execute_after: Optional[dt.datetime] = None,
    unique_key: Optional[str] = None,
) -> int:
    """
    Queue a job in the pgqueuer table and log it in pgqueuer_log.

    Parameters:
        db: A database connection object with an `executesql` method.
        entrypoint (str): The job entrypoint to execute.
        payload (dict): Payload for the job.
        priority (int, optional): Job priority (default is 10).
        execute_after (datetime, optional): When to execute the job. Defaults to datetime.now().

    Returns:
        int: The ID of the queued job.
    """
    unique_key = unique_key or str(uuid.uuid4())

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
            "unique_key": unique_key,
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
    return job_id
