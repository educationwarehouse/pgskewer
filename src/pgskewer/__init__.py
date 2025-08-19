"""
Enhanced PgQueuer implementation with pipeline support and improved job management.

This module provides an improved version of PgQueuer with additional features like
result storage, cancellation support, crash protection, and pipeline execution.
"""

import asyncio
import datetime as dt
import functools
import inspect
import json
import os
import sys
import tempfile
import traceback
import typing as t
from pathlib import Path

import asyncpg
from anyio.to_process import run_sync
from edwh_uuid7 import uuid7
from pgqueuer import PgQueuer, executors
from pgqueuer.completion import CompletionWatcher
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import JOB_STATUS, Job

from .helpers import safe_json

type AsyncTask = t.Callable[[Job], t.Awaitable[t.Any]]
# type AsyncTask = executors.AsyncEntrypoint

# ASCII End of Transmission character (EOT)
END_OF_TRANSMISSION = "\x04"


# Wrap each job with the substep name and job id, using a helper coroutine
async def named_future(
    substep: str, job_id: int, fut: asyncio.Future[JOB_STATUS]
) -> tuple[str, int, JOB_STATUS | Exception]:
    """
    Associate a substep name and job ID with a future for tracking purposes.

    This helper function wraps a future with identifying information so that
    when used with asyncio.as_completed(), you can track which future belongs
    to which job without losing context.

    Args:
        substep: The name of the substep/task being executed.
        job_id: The unique identifier for the job.
        fut: The future representing the job's execution.

    Returns:
        A tuple containing (substep_name, job_id, result_or_exception).
        If the future completes successfully, result is the JOB_STATUS.
        If it raises an exception, the exception is captured and returned.

    Example:
        >>> futures = [named_future("task1", 123, future1), named_future("task2", 124, future2)]
        >>> for coro in asyncio.as_completed(futures):
        ...     substep, job_id, result = await coro
        ...     print(f"Task {substep} (job {job_id}) completed with: {result}")
    """

    try:
        result = await fut
        return substep, job_id, result
    except Exception as e:  # pragma: no cover
        return substep, job_id, e  # Still capture which substep failed


class TaskResult(t.TypedDict):
    """
    Result structure for individual task execution.

    Attributes:
        status: The final status of the job execution.
        ok: Whether the task completed successfully without exceptions.
        result: The actual result data returned by the task.
    """

    status: JOB_STATUS
    ok: bool
    result: t.Any


class PipelineMeta(t.TypedDict):
    name: str
    steps: list[str | list[str]]


class PipelinePayload(t.TypedDict):
    """
    Complete payload structure for pipeline execution results.

    Attributes:
        initial: The original input data that started the pipeline.
        tasks: Dictionary mapping task names to their individual results.
    """

    initial: t.Any
    pipeline: PipelineMeta
    tasks: dict[str, TaskResult]


class SkewerException(Exception): ...


class SubstepFailed(SkewerException): ...


def is_async(fn: t.Callable[..., t.Awaitable[...]]) -> bool:
    """
    Determine whether a given callable is an asynchronous function.

    This function checks if the provided callable is recognized as a coroutine
    function. It is typically used to differentiate between regular synchronous
    functions and asynchronous coroutine functions.

    Args:
        fn: A callable object to be checked. This can be any callable,
            including functions, methods, or classes implementing `__call__`.

    Returns:
        bool: True if the provided callable is a coroutine function, False otherwise.
    """
    return inspect.iscoroutinefunction(fn)


type InputStep = str | AsyncTask | t.Sequence[str | AsyncTask] | t.Sequence[t.Sequence[str | AsyncTask]]


class ImprovedQueuer(PgQueuer):
    """
    Enhanced PgQueuer with additional features for job management and pipeline execution.

    This class extends the base PgQueuer with:
    - Result storage in database tables
    - Job cancellation support
    - Crash protection for unreliable tasks
    - Pipeline execution with sequential and parallel steps
    """

    def entrypoint(
        self,
        name: str,
        *,
        requests_per_second: float = float("inf"),
        concurrency_limit: int = 0,
        retry_timer: dt.timedelta = dt.timedelta(seconds=0),
        serialized_dispatch: bool = False,
        executor_factory: t.Callable[
            [executors.EntrypointExecutorParameters],
            executors.AbstractEntrypointExecutor,
        ]
        | None = None,
        # new:
        cancelable: bool = True,
        store_results: bool = True,
        crashable: bool = False,
    ) -> t.Callable[[AsyncTask], AsyncTask]:
        """
        Enhanced entrypoint decorator with additional job management features.

        This decorator extends the base PgQueuer entrypoint with three optional
        wrappers that can be applied to job functions: cancelable, store_results,
        and crashable. These wrappers are applied in a specific order to ensure
        proper functionality.

        Args:
            name: The name of the entrypoint for job identification.
            requests_per_second: Maximum requests per second for rate limiting.
            concurrency_limit: Maximum concurrent executions (0 = unlimited).
            retry_timer: Time to wait before retrying failed jobs.
            serialized_dispatch: Whether to serialize job dispatch.
            executor_factory: Custom executor factory function.
            cancelable: Whether to enable job cancellation support.
            store_results: Whether to store job results in the database.
            crashable: Whether to prevent task failures from halting pipeline execution.
                When True, exceptions are caught and None is returned instead of propagating.

        Returns:
            A decorator function that can be applied to async job functions.

        Example:
            >>> @pgq.entrypoint("my_task", store_results=True, cancelable=True)
            ... async def my_task(job: Job):
            ...     return {"processed": job.payload}
        """

        def decorator(func: AsyncTask) -> AsyncTask:
            if not is_async(func):  # pragma: no cover
                raise RuntimeError(
                    f"Please use only `async` functions (with `unblock`) for pgskewer entrypoints! (culprit: {func.__name__})"
                )

            if cancelable:
                # Apply cancelable wrapper if requested
                func = self.cancelable(func)

            if store_results:
                # Apply results wrapper if requested
                func = self.store_results(func)

            if crashable:
                # Apply exception wrapper if requested
                func = self.crashable(func)

            # Apply the original entrypoint decorator
            return self.qm.entrypoint(
                name=name,
                requests_per_second=requests_per_second,
                concurrency_limit=concurrency_limit,
                retry_timer=retry_timer,
                serialized_dispatch=serialized_dispatch,
                executor_factory=executor_factory,
            )(func)

        return decorator

    def store_results(self, async_fn: executors.EntrypointTypeVar) -> executors.EntrypointTypeVar:
        """
        Decorator that stores job execution results in the pgqueuer_result table.

        This decorator wraps an async function to automatically persist its execution
        results to a database table for later retrieval. It captures both successful
        results and exceptions, storing them with metadata about the job execution.

        Args:
            async_fn: The async function to wrap. Must accept a Job parameter and
                return any serializable result.

        Returns:
            The wrapped function that will store results in the database after execution.

        Note:
            Depends on the table structure as defined in `pgskewer_add_pgq_result_table`

        Example:
            >>> @pgq.entrypoint("name", store_results=True) # try by Default
            ... async def my_task(job: Job):
            ...     return {"processed": True}

            The result will be stored in pgqueuer_result table with job metadata.
        """

        @functools.wraps(async_fn)
        async def wrapper(job: Job):
            exc = None

            job_row = (
                await self.connection.fetch(
                    """
                    SELECT dedupe_key
                    FROM pgqueuer
                    WHERE id = $1;
                    """,
                    job.id,
                )
            )[0]
            # ^ before running the function, otherwise the row may already be removed

            try:
                result = await async_fn(job)
            except Exception as e:
                exc = e  # cursed but otherwise the scope is f'ed up
                result = {
                    "exception": [type(exc).__name__, exc],
                }

            # pgqueuer_log for job_id with status = 'successful' doesn't exit yet so store in pgqueuer_result table
            ok = exc is None
            await self.connection.execute(
                """
                INSERT INTO pgqueuer_result (job_id, entrypoint, result, ok, status, unique_key)
                VALUES ($1, $2, $3, $4, $5, $6);
                """,
                job.id,
                job.entrypoint,
                json.dumps(result, default=str),
                ok,
                "successful" if ok else "exception",
                job_row["dedupe_key"],
            )

            if exc is not None:
                raise exc

            return result

        return wrapper

    def cancelable(self, async_fn: executors.EntrypointTypeVar) -> executors.EntrypointTypeVar:
        """
        Wraps an asynchronous function to provide cancelation capability for a job.

        This decorator ensures that the wrapped asynchronous function respects a cancellation context. If the job gets
        canceled during execution, a `ChildProcessError` exception is raised.

        Note that canceling only works on async, non-blocking functions.
        You can use `unblock` to move cpu-blocking sync tasks to a cancelable context.

        You can set `cancelable` to False in case you do need a subtask to finish even when other tasks fail,
        such as cleanup operations, data consistency checks, or critical logging that must complete regardless
        of pipeline cancellation.

        Example:
            >>> @pgq.entrypoint("name") # cancelable by default
            ... async def unreliable_task(job: Job):
            ...     await unblock(time.sleep, 5) # cancelable
            ...     return "success" # unless a sibling task crashes
            >>> @pgq.entrypoint("name", cancelable=False)
            ... async def unreliable_task(job: Job):
            ...     await unblock(time.sleep, 5) # it's still a good idea not to block this thread
            ...     return "success" # even if a sibling task crashes

        """

        @functools.wraps(async_fn)
        async def wrapper(job: Job):
            with self.qm.get_context(job.id).cancellation:
                return await async_fn(job)

            raise ChildProcessError("Job cancelled!")

        return wrapper

    def crashable(self, async_fn: executors.EntrypointTypeVar) -> executors.EntrypointTypeVar:
        """
        Decorator that prevents job functions from crashing the worker process.

        This decorator wraps an async function to catch all exceptions and return
        None instead of propagating them. This is useful for jobs where you want
        to continue processing other jobs even if individual jobs fail.

        Args:
            async_fn: The async function to wrap. Must accept a Job parameter.

        Returns:
            The wrapped function that returns None on any exception instead of
            raising it.

        Warning:
            This decorator silently swallows all exceptions. Use with caution and
            consider combining with store_results() to track failures.

        Example:
            >>> @pgq.entrypoint("name", crashable=True) # False by Default
            ... async def unreliable_task(job: Job):
            ...     if random.random() < 0.5:
            ...         raise ValueError("Random failure")
            ...     return "success"

            # This will return None on failures instead of crashing
        """

        @functools.wraps(async_fn)
        async def wrapper(job: Job):
            try:
                return await async_fn(job)
            except Exception as e:
                print(f"Warn: crashable job `{job.entrypoint}` failed", file=sys.stderr)
                traceback.print_exception(e)
                return None

        return wrapper

    async def log(
        self,
        job: Job,
        status: str,
        data: str | t.Any,
        priority: int = 0,
    ):
        await self.connection.execute(
            """
            INSERT INTO pgqueuer_log (job_id, status, priority, entrypoint, traceback)
            VALUES ($1,
                    $2,
                    $3,
                    $4,
                    $5)
            """,
            job.id,
            status,
            priority,
            job.entrypoint,
            data if isinstance(data, str) else json.dumps(data),
        )

    def pipeline(
        self,
        *input_steps: InputStep,
        check: bool = True,
    ) -> AsyncTask:
        """
        Defines a pipeline of tasks to be executed in sequence or parallel.

        The top-level steps are executed **sequentially**, one after the other.
        If a step is a list of tasks, those tasks are executed **in parallel**.
        For example:
            pgq.pipeline([
                "task_1",
                ["task_2a", "task_2b"],
                "task_3"
            ])
        This runs `task_1`, then runs both `task_2a` and `task_2b` concurrently,
        and once both are complete, runs `task_3`.

        If any task in a parallel group fails (raises or returns an error status),
        its sibling tasks are terminated, and the pipeline halts—no subsequent steps are executed.

        You can pass steps using either:
        - A single list of steps (as shown above), or
        - Variadic arguments (e.g. `pgq.pipeline(task_1, ["task_2a", task_2b], "task_3")`)

        Each step can be either:
        - The **name** of an entrypoint (as a `str`)
        - A **reference** to an `AsyncTask` function

        If `check=True` (default), any task provided by **name** (as a string) will be validated
        against the task registry. If a name is missing, a warning will be shown.
        This helps catch typos or missing entrypoints.
        If you're defining the pipeline **before** the entrypoints are registered,
        you can set `check=False` to skip this validation.

        ### Result structure

        The pipeline returns a `PipelinePayload` with the following structure:

        ```python
        class TaskResult(t.TypedDict):
            status: JOB_STATUS
            ok: bool
            result: t.Any

        class PipelinePayload(t.TypedDict):
            initial: t.Any
            tasks: dict[str, TaskResult]  # keyed by entrypoint name
        ```

        Each task's result is stored under its entrypoint name in the `tasks` dictionary.
        """

        # todo:
        #  - configuring timeouts (which pgqueuer doesn't seem to support?)
        #  - configuring retries (which pgqueuer should already support?)
        #  - improved pytests/coverage

        key_to_fn = t.cast(
            dict[str, AsyncTask],
            {k: v.parameters.func for k, v in self.qm.entrypoint_registry.items()},
        )
        fn_to_key = {v: k for k, v in key_to_fn.items()}

        # 1. Map functions into entrypoint names
        def map_step(step: str | AsyncTask):
            if isinstance(step, str):
                return step
            elif callable(step):
                if step in fn_to_key:
                    return fn_to_key[step]
                raise ValueError(f"Function {step.__name__} is not registered as an entrypoint")
            elif isinstance(step, t.Sequence):
                # tuple, list; NOT dict, set
                return [map_step(s) for s in step]
            else:
                raise TypeError(f"Step must be a string, function, or Sequence, not {type(step)}")

        # 2. Ensure pipeline() can be called with one list as input or multiple inputs
        if len(input_steps) == 1 and isinstance(input_steps[0], list):
            # Single list input
            steps = [map_step(step) for step in input_steps[0]]
        else:
            # Multiple inputs
            steps = [map_step(step) for step in input_steps]

        # 3. Check for missing steps if check is True
        if check:
            for step in steps:
                if isinstance(step, str) and step not in key_to_fn:
                    print(f"warn: step '{step}' is missing, are you declaring a pipeline before the steps it uses?")
                elif isinstance(step, list):
                    for substep in step:
                        if isinstance(substep, str) and substep not in key_to_fn:
                            print(
                                f"warn: step '{substep}' is missing, are you declaring a pipeline before the steps it uses?"
                            )

        async def callback(job: Job) -> PipelinePayload:
            results: PipelinePayload = {
                "initial": safe_json(job.payload) or None,
                "pipeline": {
                    "name": job.entrypoint,
                    "steps": steps,
                },
                "tasks": {},
            }

            driver = self.connection

            for step in steps:
                substeps = [step] if isinstance(step, str) else list(step)

                payload = json.dumps(results).encode()

                queue = self.qm.queries
                job_ids = await queue.enqueue(
                    substeps,
                    payload=[payload] * len(substeps),
                    priority=[0] * len(substeps),
                    dedupe_key=[str(uuid7()) for _ in substeps],
                )

                await self.log(job, "spawned", job_ids)

                async with CompletionWatcher(driver) as w:
                    jobs = [w.wait_for(j) for j in job_ids]
                    named_jobs = [named_future(name, job_id, fut) for name, job_id, fut in zip(substeps, job_ids, jobs)]

                    for coro in asyncio.as_completed(named_jobs):
                        substep, job_id, status = await coro

                        if status == "exception" or isinstance(status, Exception):
                            await queue.mark_job_as_cancelled(job_ids)
                            raise SubstepFailed(substep)

                        print(f"✅ {substep} completed: {status}")

                        if task_result := await self.result(job_id, timeout=1):
                            results["tasks"][substep] = task_result

            return results

        return callback

    def entrypoint_pipeline(
        self,
        name: str,
        *input_steps: InputStep,
        check: bool = True,
    ):
        """
        Register a pipeline as an entrypoint that can be queued like any other job.

        This is a convenience method that combines pipeline() and entrypoint()
        to create a named pipeline that can be enqueued and executed.

        Args:
            name: The entrypoint name for the pipeline.
            *input_steps: The steps to include in the pipeline (same as pipeline()).
            check: Whether to validate step names against the registry.

        Returns:
            A decorator that registers the pipeline as an entrypoint.

        Example:
            >>> pgq.entrypoint_pipeline("data_processing", "extract", ["transform", "validate"], "load")
        """

        return self.entrypoint(name)(self.pipeline(*input_steps, check=check))

    async def result(self, job_id: int, timeout: t.Optional[int] = None) -> TaskResult | None:
        """
        Retrieve the stored result of a job by its ID.

        This method polls the pgqueuer_result table to find the result of a
        specific job. It will wait until the result is available or the timeout
        is reached.

        Args:
            job_id: The unique identifier of the job to retrieve results for.
            timeout: Maximum time to wait for results in seconds. None means wait indefinitely.

        Returns:
            TaskResult containing the job's status, success flag, and result data.
            Returns None if timeout is reached and no result is found.

        Example:
            >>> result = await pgq.result(123, timeout=30)
            >>> if result and result["ok"]:
            ...     print(f"Job succeeded: {result['result']}")
            ... else:
            ...     print("Job failed or timed out")
        """

        start_time = asyncio.get_event_loop().time()

        while True:
            # Query the database for results
            rows = await self.connection.fetch(
                """
                SELECT ok, result, status
                FROM pgqueuer_result
                WHERE job_id = $1
                ;
                """,
                job_id,
            )

            # If we found results, return them
            if rows:
                row = rows[0]
                result: TaskResult = {
                    "status": row["status"],
                    "ok": row["ok"],
                    "result": safe_json(row["result"]),
                }
                return result

            # Check if we've exceeded the timeout
            if timeout is not None:  # todo: pytests?
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    return None  # Return None if timeout reached

                # Wait a bit before trying again, but don't exceed the remaining timeout
                remaining = timeout - elapsed
                await asyncio.sleep(min(0.1, remaining))
            else:
                # No timeout specified, wait a bit before trying again
                await asyncio.sleep(0.1)

    @classmethod
    async def from_env(cls, key: str = "POSTGRES_URI") -> t.Self:
        """
        Create an ImprovedQueuer instance from environment variables.

        This class method provides a convenient way to initialize the queuer
        using a PostgreSQL connection string from environment variables.

        Args:
            key: The environment variable name containing the PostgreSQL URI.
                Defaults to "POSTGRES_URI".

        Returns:
            A new ImprovedQueuer instance connected to the specified database.

        Raises:
            KeyError: If the environment variable is not set.
            ConnectionError: If the database connection fails.

        Example:
            >>> # Set environment variable: POSTGRES_URI=postgresql://user:pass@host:5432/db
            >>> pgq = await ImprovedQueuer.from_env()
            >>> # Or use a custom environment variable
            >>> pgq = await ImprovedQueuer.from_env("DATABASE_URL")
        """

        connection = await asyncpg.connect(
            os.getenv(key),
        )
        driver = AsyncpgDriver(connection)
        return cls(driver)


def _unblock_with_logs[P, R](
    sync_fn: t.Callable[[t.Unpack[P]], R],
    stdout_path: str,
    stderr_path: str,
    *args: t.Unpack[P],
) -> R:
    """
    Execute a synchronous function with stdout/stderr redirected to files.

    This function redirects the standard output and error streams to specified
    files, executes the function, and then restores the original streams.
    It also writes an End of Transmission (EOT) character to signal completion.

    Args:
        sync_fn: The synchronous function to execute.
        stdout_path: Path to redirect stdout to.
        stderr_path: Path to redirect stderr to.
        *args: Arguments to pass to the synchronous function.

    Returns:
        The return value of the synchronous function.

    Note:
        This function is designed to be used with anyio.to_process.run_sync
        for converting blocking operations to async with log capture.
    """
    # todo: pytests?
    # low buffering for autoflush (0 only works with binary-mode; 1 means line-mode)
    with (
        open(stdout_path, "w", buffering=1) as out,
        open(stderr_path, "w", buffering=1) as err,
    ):
        _out = sys.stdout
        _err = sys.stderr
        sys.stdout = out
        sys.stderr = err
        try:
            return sync_fn(*args)
        finally:
            # Write EOT character to signal completion
            print(END_OF_TRANSMISSION, file=out, flush=True)
            print(END_OF_TRANSMISSION, file=err, flush=True)
            # reset streams:
            sys.stdout = _out
            sys.stderr = _err
            # Force flush before closing
            out.flush()
            err.flush()


async def stream_file(file_path: Path, stream: t.Literal["out", "err"], stop_event: asyncio.Event = None):
    """
    Stream a file's content in real-time to stdout or stderr.

    This function continuously reads from a file and writes new content to
    the specified output stream. It stops when an End of Transmission (EOT)
    character is detected or when the stop_event is set.

    Args:
        file_path: Path to the file to stream.
        stream: Target stream - "out" for stdout, "err" for stderr.
        stop_event: Optional event to signal when to stop streaming.

    Example:
        >>> stop_event = asyncio.Event()
        >>> task = asyncio.create_task(stream_file(Path("output.log"), "out", stop_event))
        >>> # ... do some work ...
        >>> stop_event.set()  # Stop streaming
        >>> await task
    """

    pos = 0
    output_stream = sys.stdout if stream == "out" else sys.stderr
    eot_detected = False

    while not eot_detected and not (stop_event and stop_event.is_set()):
        if file_path.exists():
            with open(file_path, "r") as f:
                f.seek(pos)
                new_content = f.read()
                if new_content:
                    # Check for EOT character
                    if END_OF_TRANSMISSION in new_content:
                        eot_detected = True

                    # Just write the content, no need to filter EOT as it's invisible
                    output_stream.write(new_content)
                    output_stream.flush()  # Force flush for real-time output

                pos = f.tell()

                # If we found EOT, no need to continue
                if eot_detected:
                    break

        await asyncio.sleep(0.1)


async def unblock[**P, R](sync_fn: t.Callable[P, R], *args: P.args, logs: bool = True) -> R:
    """
    Convert a blocking synchronous function to async with real-time log streaming.

    This function runs a synchronous function in a separate process while
    providing real-time streaming of its stdout and stderr output. It's
    useful for integrating blocking operations into async workflows.

    Args:
        sync_fn: The synchronous function to execute.
        *args: Arguments to pass to the synchronous function.
        logs: Whether to enable real-time log streaming. If False, logs
            are redirected to /dev/null.

    Returns:
        The return value of the synchronous function.

    Raises:
        Any exception raised by the synchronous function.

    Example:
        >>> import time
        >>> def slow_task(duration):
        ...     print(f"Starting task for {duration} seconds")
        ...     time.sleep(duration)
        ...     print("Task completed")
        ...     return f"Done in {duration}s"
        >>>
        >>> result = await unblock(slow_task, 5)
        >>> print(result)  # "Done in 5s"
    """
    # todo: include unblock() in the test scenario's!
    if not logs:
        # logs are redirected to /dev/null by anyio
        return await run_sync(sync_fn, *args, cancellable=True)

    # store logs in a file so we can read them:
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create log files
        temp_path = Path(temp_dir)
        stdout_path = temp_path / "stdout.txt"
        stderr_path = temp_path / "stderr.txt"

        # Create empty files
        stdout_path.touch()
        stderr_path.touch()

        # Event to stop streaming when task completes
        stop_event = asyncio.Event()

        # Start streaming tasks
        stdout_streamer = asyncio.create_task(stream_file(stdout_path, "out", stop_event))
        stderr_streamer = asyncio.create_task(stream_file(stderr_path, "err", stop_event))

        try:
            # Run the subprocess
            result = await run_sync(
                _unblock_with_logs,
                sync_fn,
                str(stdout_path),
                str(stderr_path),
                *args,
                cancellable=True,
            )

            # No need to sleep, just wait for streamers to finish reading EOT
            await asyncio.wait([stdout_streamer, stderr_streamer], timeout=1.0)

            return result

        finally:
            # Clean up streaming tasks
            stop_event.set()

            # Make sure EOT is written if tasks are cancelled
            for path in (stdout_path, stderr_path):
                if path.exists():
                    try:
                        with open(path, "a", buffering=1) as f:
                            f.write(END_OF_TRANSMISSION)
                    except Exception:
                        pass

            # Cancel tasks if they're still running
            for task in [stdout_streamer, stderr_streamer]:
                if not task.done():
                    task.cancel()

            # Wait for tasks to complete
            try:
                await asyncio.gather(stdout_streamer, stderr_streamer, return_exceptions=True)
            except asyncio.CancelledError:
                pass


def parse_payload(data: bytes | str | None) -> PipelinePayload:
    """
    Parse job payload data into a PipelinePayload structure.

    This function is a convenience wrapper around safe_json() specifically
    for parsing pipeline payload data. It returns the parsed data as a
    PipelinePayload type.

    Args:
        data: The payload data to parse (bytes, string, or None).

    Returns:
        The parsed payload as a PipelinePayload, or None if parsing fails.

    Example:
        >>> payload_data = '{"initial": {"id": 1}, "tasks": {}}'
        >>> payload = parse_payload(payload_data)
        >>> print(payload["initial"])  # {"id": 1}
    """

    return safe_json(data)
