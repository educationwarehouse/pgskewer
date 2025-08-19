from pathlib import Path

import edwh.tasks
from edwh import task


@task(pre=[edwh.tasks.require_sudo])
def run(c):
    tests_dir = Path("./tests")
    cov_dir = tests_dir / "coverage"
    covrc = tests_dir / ".coveragerc"

    assert covrc.exists(), "what the helly"

    cov_dir.mkdir(exist_ok=True)
    c.sudo(f"chown -R 1050:1050 {cov_dir}")

    c.run(f"coverage run --parallel-mode --data-file={cov_dir}/.coverage -m pytest -svx", pty=True)

    c.run(f"coverage combine {cov_dir}")

    c.run(f"coverage html --rcfile={covrc}", hide=True)
    c.run(f"coverage report --rcfile={covrc}")
