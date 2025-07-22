import os
import time

import pytest
from pydal import DAL


def start_dc():
    os.system("cd tests; docker compose up -d")


def stop_dc():
    os.system("cd tests; docker compose down")


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


### start tests ###

def test_basic_consumer(db: DAL):
    ...


def test_breaking_consumer(db):
    ...


def test_basic_pipeline(db):
    ...


def test_breaking_pipeline(db):
    ...


def test_fault_tolerant_pipeline(db):
    ...
