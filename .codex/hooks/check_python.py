import json
import re
import subprocess
import sys
from pathlib import Path

CHECKS = (
    ("uvx", "ruff", "check"),
    ("uvx", "ty", "check"),
    ("uv", "run", "--locked", "--extra", "session", "pytest"),
    ("cargo", "test", "--locked", "--no-default-features"),
    ("cargo", "test", "--locked", "-p", "snowpark-rs", "--features", "session"),
    (
        "cargo",
        "clippy",
        "--locked",
        "--workspace",
        "--all-targets",
        "--all-features",
        "--",
        "-D",
        "warnings",
    ),
)
TEST_REVIEW = (
    "Review the added or modified Python and Rust tests in the working tree. "
    "Remove temporary agent probes, scaffolding and duplicate cases that add no coverage. "
    "Keep tests that protect distinct behavior or regressions and preserve unrelated edits."
)
RUST_TEST_ATTRIBUTE = re.compile(r"#\[\s*(?:(?:\w+::)*test\b|cfg\(\s*test\b)")


def has_test_changes(root: Path) -> bool:
    changed = subprocess.check_output(
        [
            "git",
            "--no-optional-locks",
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--no-renames",
        ],
        cwd=root,
        encoding="utf-8",
        stderr=subprocess.PIPE,
    )
    for entry in filter(None, changed.split("\0")):
        name = Path(entry[3:])
        path = root / name
        if path.suffix not in {".py", ".rs"} or not path.is_file():
            continue
        if "tests" in name.parts:
            return True
        if path.suffix == ".py":
            if path.match("test_*.py") or path.match("*_test.py") or path.name == "conftest.py":
                return True
        elif RUST_TEST_ATTRIBUTE.search(path.read_text(encoding="utf-8", errors="replace")):
            return True
    return False


def main() -> None:
    event = json.load(sys.stdin)
    root = Path(__file__).resolve().parents[2]
    failures = []

    for command in CHECKS:
        try:
            result = subprocess.run(
                command,
                cwd=root,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
        except OSError as error:
            failures.append(f"`{' '.join(command)}`: {error}")
            continue
        if result.returncode:
            output = result.stdout.strip()
            if len(output) > 6_000:
                output = f"{output[:2_000]}\n\n[Output truncated]\n\n{output[-4_000:]}"
            failures.append(
                f"`{' '.join(command)}` exited with {result.returncode}\n{output}"
            )

    response = {}
    if event.get("stop_hook_active"):
        if failures:
            diagnostics = "\n\n".join(failures)
            response = {"systemMessage": f"Project checks still failing\n\n{diagnostics}"}
    else:
        follow_up = []
        if has_test_changes(root):
            follow_up.append(TEST_REVIEW)
        if failures:
            follow_up.extend(("Review the project check failures before finishing.", *failures))
        if follow_up:
            response = {"decision": "block", "reason": "\n\n".join(follow_up)}
    print(json.dumps(response))


if __name__ == "__main__":
    main()
