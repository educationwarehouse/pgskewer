import contextlib
import json
import os
import sys
import tempfile
import textwrap
import warnings
from uuid import UUID

from dotenv import load_dotenv

with warnings.catch_warnings():
    # silence `invalid escape sequence` warnings in pydal:
    warnings.filterwarnings("ignore", category=SyntaxWarning)
    from pydal import DAL

from .helpers import queue_job

PROGRAM_NAME = "pgskewer"


@contextlib.contextmanager
def setup_db():
    load_dotenv(override=False)

    pg_uri = os.environ["POSTGRES_URI"]

    with tempfile.TemporaryDirectory() as folder:
        db = DAL(pg_uri, folder=folder)
        yield db
        db.close()


def enqueue(entrypoint: str, data: str = "{}") -> UUID:
    with setup_db() as db:
        job = queue_job(db, entrypoint, data)

        print(f"Enqueued Job({job.id})")
        return job.key


def print_help():
    help_text = textwrap.dedent("""
    Usage: %(program)s <command> [arguments]
    
    Commands:
      enqueue <entrypoint> [payload]    Enqueue a task with the given entrypoint
                                        Optional json payload data parameter (defaults to '{}')
    
    Options:
      -h, --help                        Show this help message
    
    Examples:
      %(program)s enqueue my_task
      %(program)s enqueue my_task '{"key": "value"}'
    """) % dict(program=PROGRAM_NAME)
    print(help_text.strip())


def print_unknown_command(command):
    print(f"Error: Unknown command '{command}'")
    print(f"Use '{PROGRAM_NAME} --help' to see available commands.")


def print_invalid_usage():
    print("Error: Invalid command usage")
    print(f"Use '{PROGRAM_NAME} --help' to see available commands.")


def main(args: list[str] = ()):
    args = tuple(args or sys.argv[1:])

    match args:
        case () | ("--help", *_) | ("-h", *_):
            print_help()
        case ("enqueue", entrypoint):
            # Handle case with just entrypoint (no data)
            enqueue(entrypoint)
        case ("enqueue", entrypoint, data):
            # Handle case with entrypoint and data
            enqueue(entrypoint, data)
        case ("enqueue", *_):
            # Handle any other enqueue usage
            print_invalid_usage()

        # todo: more subcommands

        case (command, *_):
            # Handle unknown commands
            print_unknown_command(command)
        case _:
            print_help()


if __name__ == "__main__":
    main(sys.argv[1:])
