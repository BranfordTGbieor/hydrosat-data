#!/usr/bin/env python3
"""Validate repo commit messages in CI and local commit-msg hooks."""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ALLOWED_TYPES = ("feat", "fix", "chore", "docs", "ci", "refactor", "test")
TITLE_PREFIXES = tuple(f"{kind} " for kind in ALLOWED_TYPES)
SCOPED_TYPE_PREFIXES = tuple(f"{kind}(" for kind in ALLOWED_TYPES)
TITLE_MAX_LENGTH = 100
IGNORED_PREFIXES = ("Merge ", "Revert ")


def run_git(*args: str) -> str:
    """Run a git command and return stripped stdout."""
    return subprocess.check_output(["git", *args], text=True).strip()


def get_commit_message(commit: str) -> str:
    """Return the full message body for a commit SHA or ref."""
    return run_git("show", "-s", "--format=%B", commit)


def get_commits_in_range(rev_range: str) -> list[str]:
    """Return commits in chronological order for the provided revision range."""
    output = run_git("rev-list", "--reverse", rev_range)
    return [line for line in output.splitlines() if line]


def is_valid_title_prefix(title: str) -> bool:
    """Allow either '<type> summary' or '<type>(scope): summary' titles."""
    if title.startswith(TITLE_PREFIXES):
        return True

    for prefix in SCOPED_TYPE_PREFIXES:
        if title.startswith(prefix) and "): " in title:
            return True

    return False


def validate_message(message: str) -> list[str]:
    """Validate one commit message and return human-readable errors."""
    errors: list[str] = []
    lines = message.splitlines()

    if not lines:
        return ["commit message is empty"]

    title = lines[0].strip()
    if not title:
        return ["commit title is empty"]

    if title.startswith(IGNORED_PREFIXES):
        return []

    if len(title) > TITLE_MAX_LENGTH:
        errors.append(f"commit title exceeds {TITLE_MAX_LENGTH} characters")

    if not is_valid_title_prefix(title):
        allowed = ", ".join(ALLOWED_TYPES)
        errors.append(
            "commit title must start with an allowed type using '<type> summary' or '<type>(scope): summary' "
            f"where type is one of: {allowed}"
        )

    if title.endswith("."):
        errors.append("commit title must not end with a period")

    if len(lines) < 3:
        errors.append("commit message must include a blank line and at least one bullet in the body")
        return errors

    if lines[1].strip() != "":
        errors.append("second line must be blank")

    body_lines = [line.rstrip() for line in lines[2:] if line.strip()]
    if not body_lines:
        errors.append("commit body must contain at least one non-empty bullet line")
        return errors

    for line in body_lines:
        if not line.startswith("* "):
            errors.append("each non-empty body line must start with '* '")
            break

    return errors


def validate_commit(commit: str) -> list[str]:
    """Validate a stored commit object."""
    return validate_message(get_commit_message(commit))


def validate_file(path: Path) -> list[str]:
    """Validate a raw commit message file from a commit-msg hook."""
    return validate_message(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for commit, range, or hook-file validation."""
    parser = argparse.ArgumentParser(description="Validate repo commit message format")
    parser.add_argument("commit_msg_file", nargs="?", help="commit message file path for commit-msg hooks")
    parser.add_argument("--rev-range", help="git revision range to validate")
    parser.add_argument("--commit", action="append", default=[], help="single commit SHA to validate")
    return parser.parse_args()


def main() -> int:
    """Run commit-message validation and print failures in CI-friendly form."""
    args = parse_args()
    failures: list[tuple[str, list[str]]] = []

    if args.commit_msg_file:
        path = Path(args.commit_msg_file)
        failures.append((str(path), validate_file(path)))
    elif args.rev_range:
        commits = get_commits_in_range(args.rev_range)
        for commit in commits:
            failures.append((commit, validate_commit(commit)))
    elif args.commit:
        for commit in args.commit:
            failures.append((commit, validate_commit(commit)))
    else:
        print("Provide a commit message file, --rev-range, or --commit.", file=sys.stderr)
        return 2

    failed = [(target, errors) for target, errors in failures if errors]
    if not failed:
        return 0

    for target, errors in failed:
        print(f"Commit message validation failed for {target}:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
