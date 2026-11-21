"""Publish new Python distribution versions as GitHub Release assets."""

import json
import os
import subprocess
import sys
import tempfile
import tomllib
from email import message_from_bytes
from fnmatch import fnmatchcase
from pathlib import Path
from zipfile import ZipFile

# Publish the session backend first so the Python extra can be resolved immediately.
PROJECTS = {
    "snowpark-rs-session": Path("crates/snowpark-rs-session/pyproject.toml"),
    "snowpark-rs": Path("pyproject.toml"),
}
PLATFORMS = {
    "win_amd64",
    "manylinux_2_28_x86_64",
    "macosx_*_arm64",
    "macosx_*_x86_64",
}


def gh(*args: str) -> str:
    return subprocess.check_output(["gh", *args], encoding="utf-8").strip()


def releases(repository: str) -> dict:
    pages = json.loads(
        gh("api", "--paginate", "--slurp", f"repos/{repository}/releases?per_page=100")
    )
    return {release["tag_name"]: release for page in pages for release in page}


def project_metadata(name: str) -> dict:
    return tomllib.loads(PROJECTS[name].read_text(encoding="utf-8"))["project"]


def tag_name(name: str, version: str) -> str:
    return f"{name}-v{version}"


def plan(repository: str) -> None:
    existing = releases(repository)
    packages = []
    for name in PROJECTS:
        version = project_metadata(name)["version"]
        release = existing.get(tag_name(name, version))
        if release is None or release["draft"]:
            packages.append([name, version])
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
        print(f"packages={json.dumps(packages, separators=(',', ':'))}", file=output)


def wheel_assets(name: str, version: str) -> tuple[list[Path], str]:
    files = sorted(Path("dist").glob(f"{name.replace('-', '_')}-{version}-*.whl"))
    platforms = [wheel.stem.rsplit("-", 1)[-1] for wheel in files]
    if len(files) != len(PLATFORMS) or any(
        sum(fnmatchcase(platform, pattern) for platform in platforms) != 1
        for pattern in PLATFORMS
    ):
        raise ValueError(f"Expected one wheel for each platform for {name} {version}: {files}")

    requires_python = project_metadata(name)["requires-python"]
    for wheel in files:
        with ZipFile(wheel) as archive:
            metadata_name = next(
                entry for entry in archive.namelist() if entry.endswith(".dist-info/METADATA")
            )
            metadata = message_from_bytes(archive.read(metadata_name))
        if (
            metadata["Name"] != name
            or metadata["Version"] != version
            or metadata["Requires-Python"] != requires_python
        ):
            raise ValueError(f"Unexpected distribution metadata in {wheel}")
    return files, requires_python


def publish(repository: str, commit: str, packages: list[list[str]]) -> None:
    existing = releases(repository)
    ready = [(name, version, *wheel_assets(name, version)) for name, version in packages]
    for name, version, wheels, requires_python in ready:
        tag = tag_name(name, version)
        release = existing.get(tag)
        if release is not None:
            if not release["draft"]:
                continue
            if release["target_commitish"] != commit:
                raise ValueError(f"Draft {tag} targets {release['target_commitish']}, expected {commit}")
        else:
            gh(
                "release", "create", tag, "--repo", repository, "--draft",
                "--target", commit, "--title", f"{name} {version}",
                "--notes", "Python wheels for Windows, Linux, and macOS.",
            )
            release = releases(repository)[tag]

        uploaded = {asset["name"] for asset in release["assets"] if asset["state"] == "uploaded"}
        with tempfile.TemporaryDirectory() as directory:
            metadata = Path(directory) / "wheel-index.json"
            metadata.write_text(
                json.dumps(
                    {
                        "name": name,
                        "version": version,
                        "requires-python": requires_python,
                        "wheels": [wheel.name for wheel in wheels],
                    },
                    indent=2,
                ) + "\n",
                encoding="utf-8",
            )
            missing = [str(file) for file in [*wheels, metadata] if file.name not in uploaded]
            if missing:
                gh("release", "upload", tag, *missing, "--repo", repository)
        result = gh(
            "release", "edit", tag, "--draft=false", "--repo", repository,
            *(["--latest=false"] if name == "snowpark-rs-session" else []),
        )
        print(result, flush=True)


if __name__ == "__main__":
    repository = os.environ["GITHUB_REPOSITORY"]
    match sys.argv[1:]:
        case ["plan"]:
            plan(repository)
        case ["publish"]:
            publish(repository, os.environ["GITHUB_SHA"], json.loads(os.environ["RELEASE_PACKAGES"]))
        case _:
            sys.exit("Usage: release_wheels.py plan|publish")
