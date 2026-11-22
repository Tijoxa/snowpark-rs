import json
import subprocess
import sys
from pathlib import Path


def changed_python_files(root: Path) -> list[str]:
    changes = subprocess.check_output(
        [
            "git",
            "--no-optional-locks",
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--no-renames",
            "--",
            "*.py",
            "*.pyi",
        ],
        cwd=root,
        encoding="utf-8",
    )
    paths = (entry[3:] for entry in changes.split("\0") if entry)
    return [path for path in paths if (root / path).is_file()]


def main() -> None:
    event = json.load(sys.stdin)
    root = Path(__file__).resolve().parents[2]
    checks = [("cargo", "fmt", "--all", "--", "--check")]
    if paths := changed_python_files(root):
        checks.append(("uvx", "ruff", "format", "--check", "--", *paths))

    failures = []
    for command in checks:
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

    diagnostics = "\n\n".join(failures)
    if not failures:
        response = {}
    elif event.get("stop_hook_active"):
        response = {
            "systemMessage": f"Formatting checks still failing\n\n{diagnostics}"
        }
    else:
        response = {"decision": "block", "reason": diagnostics}
    print(json.dumps(response))


if __name__ == "__main__":
    main()
