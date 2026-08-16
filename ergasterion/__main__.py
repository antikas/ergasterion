"""Launcher-free entry point for ``python -m ergasterion``.

A thin shim only: it delegates to the same ``ergasterion.cli:main`` the
``ergasterion`` console command (the ``[project.scripts]`` entry point in
``pyproject.toml``) already runs. No logic lives here and none should be
added here -- add subcommands to ``ergasterion/cli.py`` instead.

This makes ``python -m ergasterion <subcommand> ...`` work identically to
``ergasterion <subcommand> ...`` on every OS. It does not depend on the
platform-specific launcher being present on ``PATH``.
"""
from ergasterion.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
