import asyncio
import datetime as dt
import functools
import json
import sys
import tempfile
import typing as t
from pathlib import Path

import asyncpg
from anyio.to_process import run_sync
from pgqueuer import PgQueuer, executors
from pgqueuer.completion import CompletionWatcher
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import JOB_STATUS, Job

# ASCII End of Transmission character (EOT)
END_OF_TRANSMISSION = "\x04"


# Wrap each job with the substep name and job id, using a helper coroutine
async def named_future(
    substep: str, job_id: int, fut: asyncio.Future[JOB_STATUS]
) -> tuple[str, int, JOB_STATUS | Exception]:
    """
    Zip substep (name) + job id + future value

    For use with `asyncio.as_completed` without losing which future belongs to which job.
    """
    try:
        result = await fut
        return substep, job_id, result
    except Exception as e:
        return substep, job_id, e  # Still capture which substep failed


type AsyncTask = t.Callable[[Job], t.Awaitable[t.Any]]


class TaskResult(t.TypedDict):
    status: JOB_STATUS
    ok: bool
    result: t.Any


class PipelinePayload(t.TypedDict):
    initial: t.Any
    tasks: dict[str, TaskResult]


class ImprovedQueuer(PgQueuer):
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
    ) -> t.Callable[[executors.EntrypointTypeVar], executors.EntrypointTypeVar]:
        """
        Wrapper around PgQueuer's default entrypoint that also enables `cancelable` and `store_results`
        """

        def decorator(func: executors.EntrypointTypeVar) -> executors.EntrypointTypeVar:
            # Apply cancelable wrapper if requested
            if cancelable:
                func = self.cancelable(func)

            if store_results:
                func = self.store_results(func)

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

    def store_results(self, async_fn):
        @functools.wraps(async_fn)
        async def wrapper(job: Job, *a):
            exc = None

            try:
                result = await async_fn(job, *a)
            except Exception as e:
                exc = e  # cursed but otherwise the scope is f'ed up
                result = {
                    "exception": [type(exc).__name__, exc],
                }

            # pgqueuer_log for job_id with status = 'successful' doesn't exit yet so store in pgqueuer_results table
            await self.connection.execute(
                """
                INSERT INTO pgqueuer_results (job_id, entrypoint, result, ok)
                VALUES ($1, $2, $3, $4);
                """,
                job.id,
                job.entrypoint,
                json.dumps(result, default=str),
                exc is None,
            )

            if exc is not None:
                raise exc

            return result

        return wrapper

    def cancelable(self, async_fn):
        @functools.wraps(async_fn)
        async def wrapper(job: Job, *a):
            with self.qm.get_context(job.id).cancellation:
                return await async_fn(job, *a)

            raise ChildProcessError("Job cancelled!")

        return wrapper

    def pipeline(
        self,
        *input_steps: str | AsyncTask | list[str | AsyncTask],
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
                raise ValueError(
                    f"Function {step.__name__} is not registered as an entrypoint"
                )
            elif isinstance(step, list):
                return [map_step(s) for s in step]
            else:
                raise TypeError(
                    f"Step must be a string, function, or list, not {type(step)}"
                )

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
                    print(
                        f"warn: step '{step}' is missing, are you declaring a pipeline before the steps it uses?"
                    )
                elif isinstance(step, list):
                    for substep in step:
                        if isinstance(substep, str) and substep not in key_to_fn:
                            print(
                                f"warn: step '{substep}' is missing, are you declaring a pipeline before the steps it uses?"
                            )

        async def callback(job: Job) -> PipelinePayload:
            # currently, the initial payload is set as the result but that looks kinda weird:
            # {'ka': 'via', 'fetch': {'status': 'successful', 'ok': True, 'result': 'yay 114'}, 'slow': {'status': 'successful', 'ok': True, 'result': 'waiting is over'}}
            # (in this example `{ka: via}` was the initial payload.
            # how else would you restructure this?
            # I would also like a TypedDict definition for improved typing
            # and then maybe a parse_payload function (that returns that typeddict) instead of a generic safe_json
            results: PipelinePayload = {
                "initial": safe_json(job.payload) or None,
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
                )

                async with CompletionWatcher(driver) as w:
                    jobs = [w.wait_for(j) for j in job_ids]
                    named_jobs = [
                        named_future(name, job_id, fut)
                        for name, job_id, fut in zip(substeps, job_ids, jobs)
                    ]

                    for coro in asyncio.as_completed(named_jobs):
                        substep, job_id, status = await coro

                        if status == "exception" or isinstance(status, Exception):
                            await queue.mark_job_as_cancelled(job_ids)
                            raise RuntimeError(f"{substep} failed")

                        print(f"✅ {substep} completed: {status}")

                        rows = await self.connection.fetch(
                            """
                            SELECT ok, result
                            FROM pgqueuer_results
                            WHERE job_id = $1
                            ;
                            """,
                            job_id,
                        )

                        if rows:
                            row = rows[0]
                            results["tasks"][substep] = {
                                "status": status,
                                "ok": row["ok"],
                                "result": safe_json(row["result"]),
                            }

            return results

        return callback

    def entrypoint_pipeline(
        self,
        name: str,
        *input_steps: str | AsyncTask | list[str | AsyncTask],
        check: bool = True,
    ):
        """
        Register your pipeline as an entrypoint
        """
        return self.entrypoint(name)(self.pipeline(*input_steps, check=check))


def _unblock_with_logs[P, R](
    sync_fn: t.Callable[[t.Unpack[P]], R],
    stdout_path: str,
    stderr_path: str,
    *args: t.Unpack[P],
) -> R:
    """Run sync function with logging to specified files"""
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


async def stream_file(
    file_path: Path, stream: t.Literal["out", "err"], stop_event: asyncio.Event = None
):
    """Stream a file's content in real-time until EOT character is detected or stop_event is set"""
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


async def unblock[**P, R](
    sync_fn: t.Callable[P, R], *args: P.args, logs: bool = True
) -> R:
    """
    Convert blocking function to async with real-time log streaming
    """
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
        stdout_streamer = asyncio.create_task(
            stream_file(stdout_path, "out", stop_event)
        )
        stderr_streamer = asyncio.create_task(
            stream_file(stderr_path, "err", stop_event)
        )

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
                await asyncio.gather(
                    stdout_streamer, stderr_streamer, return_exceptions=True
                )
            except asyncio.CancelledError:
                pass


def safe_json(data: bytes | str | None) -> t.Any | None:
    if not data:
        return None

    data = data.decode() if not isinstance(data, str) else data

    try:
        return json.loads(data)
    except (TypeError, ValueError, json.decoder.JSONDecodeError):
        return None


def parse_payload(data: bytes | str | None) -> PipelinePayload:
    return safe_json(data)
