"""Run test modules in separate interpreters so Spark JVM package sets are isolated."""
import os
from pathlib import Path
import subprocess
import sys


def main():
    results = []
    for path in sorted(Path("tests").glob("test_*.py")):
        print(f"\nRunning {path.name} in an isolated Python/JVM process", flush=True)
        process = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", path.name, "-v"], env=os.environ.copy())
        results.append((path.name, process.returncode))
    print("\nModule results:", results, flush=True)
    return 1 if any(code for _, code in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
