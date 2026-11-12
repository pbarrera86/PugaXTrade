"""Run integration tests with portable R and PostgreSQL packages, no installation.

PUGA_TEST_RUNTIME must contain the extracted Debian packages under root/.
Only a freshly initialized temporary database is used.
"""
import os
from pathlib import Path
import subprocess
import tempfile
import time

base = Path(os.environ["PUGA_TEST_RUNTIME"])
root = base / "root"
env = os.environ.copy()
env.update(
    LD_LIBRARY_PATH=f"{root}/usr/lib/x86_64-linux-gnu:{root}/usr/lib/R/lib",
    R_HOME=f"{root}/usr/lib/R",
    R_LIBS_SITE=f"{root}/usr/lib/R/site-library",
    EDITOR="vi",
    PUGA_TEST_DATABASE="local-disposable",
)
bins = root / "usr/lib/postgresql/17/bin"
with tempfile.TemporaryDirectory(prefix="pgtest_", dir=base) as directory:
    subprocess.run([str(bins / "initdb"), "-D", directory, "-L", str(root / "usr/share/postgresql/17"), "--no-locale", "--encoding=UTF8", "--auth=trust"], env=env, check=True, stdout=subprocess.DEVNULL)
    with open(base / "integration-postgres.log", "w") as log:
        proc = subprocess.Popen([str(bins / "postgres"), "-D", directory, "-k", "", "-h", "127.0.0.1", "-p", "55432"], env=env, stdout=log, stderr=log)
        try:
            time.sleep(1)
            subprocess.run([str(bins / "psql"), "-h", "127.0.0.1", "-p", "55432", "-U", "agent", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-f", "schema.sql", "-f", "tests/migration_seed.sql", "-f", "migrations/001_commercial_security.sql", "-f", "migrations/002_analysis_capacity.sql"], env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run([str(root / "usr/lib/R/bin/exec/R"), "--vanilla", "--slave", "-f", "tests/regression.R"], env=env, check=True)
            subprocess.run([str(root / "usr/lib/R/bin/exec/R"), "--vanilla", "--slave", "-f", "tests/database.R"], env=env, check=True)
        finally:
            proc.terminate()
            proc.wait(timeout=15)
