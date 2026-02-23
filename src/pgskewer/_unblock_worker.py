import contextlib
import sys
import traceback

import dill


def _dump_exception(error_path: str, exc: BaseException) -> None:
    try:
        payload = dill.dumps(exc)
    except Exception:
        payload = dill.dumps(RuntimeError(traceback.format_exc()))

    with open(error_path, "wb") as fh:
        fh.write(payload)


def main() -> int:
    """
    Execute one serialized synchronous call in an isolated Python subprocess.

    This worker is launched by `pgskewer.unblock()` via:
    `python -m <package>._unblock_worker ...`.
    It acts as a tiny RPC endpoint using filesystem-based message passing.

    Flow:
    1. Validate CLI arguments. The worker expects exactly 6 payload arguments
       (7 total argv items including argv[0]):
       - function payload file path
       - args payload file path
       - result payload file path
       - error payload file path
       - stdout capture file path
       - stderr capture file path
    2. Deserialize callable and positional args from the input files via `dill`.
    3. Redirect `stdout`/`stderr` into dedicated files (line-buffered), so
       the parent process can stream logs in near real time.
    5. Run the callable:
       - on success: serialize return value to `result_path`, exit code `0`
       - on failure: serialize exception to `error_path`, exit code `1`
    6. Close redirected stdout/stderr when done; parent-side streamers stop
       when the parent signals completion and no more bytes are observed.

    Serialization strategy:
    - `dill` is used for callable/arg/result/exception payloads.
    - If serializing the actual exception fails, `_dump_exception()` falls back
      to a pickled `RuntimeError` containing a traceback string.

    Exit codes:
    - `0`: call succeeded and result payload was written
    - `1`: call failed (deserialization, execution, or result serialization path)
           and error payload should be present
    - `2`: invalid CLI contract (argument count mismatch)
    """
    if len(sys.argv) != 7:
        return 2

    fn_path, args_path, result_path, error_path, stdout_path, stderr_path = sys.argv[1:]
    try:
        with open(fn_path, "rb") as fh:
            sync_fn = dill.loads(fh.read())  # nosec
        with open(args_path, "rb") as fh:
            args = dill.loads(fh.read())  # nosec
    except Exception as exc:
        _dump_exception(error_path, exc)
        return 1

    with (
        open(stdout_path, "w", buffering=1) as out,
        open(stderr_path, "w", buffering=1) as err,
        contextlib.redirect_stdout(out),
        contextlib.redirect_stderr(err),
    ):
        try:
            result = sync_fn(*args)
            with open(result_path, "wb") as fh:
                fh.write(dill.dumps(result))
            return_code = 0
        except BaseException as exc:
            _dump_exception(error_path, exc)
            return_code = 1

    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
