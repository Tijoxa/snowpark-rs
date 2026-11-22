---
name: review-tests
description: Review added or modified Rust and Python tests in snowpark-rs and select relevant Cargo or pytest runs for changes that can affect project behavior. Use for code, test, dependency, and build changes that affect tested behavior.
---

# Review and run relevant tests

Inspect the task's working-tree changes, including staged and untracked files. Review added or modified tests under `tests/` and inline Rust test modules. Check what behavior each test protects against the existing coverage. Remove temporary probes, unnecessary scaffolding, and duplicates introduced by the task that add no coverage. Keep distinct behavior and regression coverage, and preserve unrelated edits.

Choose checks based on the behavior affected, including changes to implementation with no test diff. Skip Cargo tests and pytest for documentation, formatting, and tooling changes that leave tested behavior untouched. Start with the affected test file, target, or filter when identifiable. Run a full relevant suite for shared code or broad changes.

| Change | Command from the repository root |
| --- | --- |
| Rust SQL compiler or core behavior | `cargo test --locked --no-default-features` |
| Rust Polars translation | `cargo test --locked -p snowpark-rs --features polars` |
| Rust session or Arrow behavior | `cargo test --locked -p snowpark-rs --features session` |
| Python API, tests, or Rust behavior exposed to Python | `uv run --locked --extra session --no-editable pytest` |

On Windows, `--no-editable` builds a wheel and avoids locking an editable `.pyd` in the source tree. Run live Snowflake tests when remote behavior needs verification and a test profile is available. Use the repository's documented live test commands.

For changes that warrant static checks, use `uvx ruff check` and `uvx ty check` for Python or `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` for Rust. Report the checks run and any relevant checks blocked by the environment.
