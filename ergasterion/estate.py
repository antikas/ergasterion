"""Paths for the data-product estate operated on by the installed engine.

Two-class path principle (the whole point of this module, stated so no caller mis-threads
it):

  * ENGINE DATA -- Ergasterion's shipped assets: the Jinja templates and the vendored
    JSON Schemas. These resolve PACKAGE-relative, ``__file__``-anchored inside ``ergasterion/``
    and shipped as package data (pyproject ``[tool.setuptools.package-data]``). They are
    NOT estate paths and never ride an EstateContext -- an installed wheel finds them next
    to its own code regardless of which estate it is pointed at. Each emitter keeps its own
    ``TEMPLATES_DIR`` / ``SCHEMA_PATH`` = ``Path(__file__).resolve().parent / ...``.

  * ESTATE PATHS -- the data-product estate the engine operates on: declarations, domains,
    the emitted models/contracts/graphs trees, the compiled manifest, the target-neutral
    root ``estate.yml`` (``ergasterion.source_delivery.load_estate_namespace`` reads its
    ``estate.namespace``), the LICENSE, and (optionally) an external model repo for
    canonical-mapping validation. Every one of these rides this object.

The two classes never mix. Threading an estate path into engine-data resolution (loading a
template from the estate) or vice versa (loading declarations from the package) is the named
mis-thread failure this split exists to prevent.

Estate-root resolution, highest precedence first:

  1. an explicit ``estate_root`` (the ``--estate-root`` flag);
  2. the ``DPF_ESTATE_ROOT`` environment variable;
  3. a walk UP from the current directory to the nearest ancestor that carries BOTH
     ``dbt_project.yml`` and a ``domains/`` directory -- dbt's own project-resolution
     precedent, no new marker file invented;
  4. failing all of the above, the package anchor (``ergasterion/`` -> its parent), which is the
     source-tree / editable-install case: the estate is co-located with the engine. This
     keeps a bare ``python ergasterion/emit.py`` from a repository-root working directory
     resolving to the co-located estate.

Every path field is INDIVIDUALLY overridable at construction (``resolve(declarations_dir=...)``)
so a test can point one path at a temp directory without monkeypatching a module global.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ergasterion._repo_root import REPO_ROOT

ESTATE_ROOT_ENV = "DPF_ESTATE_ROOT"
# The walk-up markers: an estate root is the nearest ancestor carrying BOTH of these.
_MARKER_FILE = "dbt_project.yml"
_MARKER_DIR = "domains"


def _walk_up_root(start: Path) -> Path | None:
    """Nearest ancestor of ``start`` (inclusive) carrying dbt_project.yml + domains/, or None."""
    start = start.resolve()
    for candidate in (start, *start.parents):
        if (candidate / _MARKER_FILE).is_file() and (candidate / _MARKER_DIR).is_dir():
            return candidate
    return None


def resolve_root(
    estate_root: str | os.PathLike[str] | None = None,
    *,
    env: dict[str, str] | None = None,
    start: str | os.PathLike[str] | None = None,
) -> Path:
    """Resolve the estate root by the precedence chain in the module docstring."""
    if estate_root is not None:
        return Path(estate_root).resolve()
    environ = os.environ if env is None else env
    from_env = environ.get(ESTATE_ROOT_ENV)
    if from_env:
        return Path(from_env).resolve()
    walked = _walk_up_root(Path(start) if start is not None else Path.cwd())
    if walked is not None:
        return walked
    return REPO_ROOT


@dataclass(frozen=True)
class EstateContext:
    """Every estate path the emitters read, all derived from one resolved estate root.

    Construct via :meth:`resolve` (which runs the precedence chain) or :meth:`default` (the cached
    ambient context each emitter falls back to when no caller supplies one). ``openim_root``
    is ``None`` by default -- the canonical-mapping validation is skip-safe, so a None here
    simply skips it; there is no repo-literal default path.
    """

    root: Path
    domains_dir: Path
    declarations_dir: Path
    models_dir: Path
    contracts_dir: Path
    graphs_dir: Path
    tests_dir: Path
    license_path: Path
    manifest_path: Path
    estate_file: Path
    openim_root: Path | None

    @classmethod
    def resolve(
        cls,
        *,
        estate_root: str | os.PathLike[str] | None = None,
        openim_root: str | os.PathLike[str] | None = None,
        env: dict[str, str] | None = None,
        start: str | os.PathLike[str] | None = None,
        domains_dir: Path | None = None,
        declarations_dir: Path | None = None,
        models_dir: Path | None = None,
        contracts_dir: Path | None = None,
        graphs_dir: Path | None = None,
        tests_dir: Path | None = None,
        license_path: Path | None = None,
        manifest_path: Path | None = None,
        estate_file: Path | None = None,
    ) -> "EstateContext":
        """Resolve the root, derive every estate path from it, then
        apply any per-field override the caller supplied (each defaults to root-relative)."""
        root = resolve_root(estate_root, env=env, start=start)
        return cls(
            root=root,
            domains_dir=domains_dir if domains_dir is not None else root / "domains",
            declarations_dir=declarations_dir if declarations_dir is not None else root / "declarations",
            models_dir=models_dir if models_dir is not None else root / "models",
            contracts_dir=contracts_dir if contracts_dir is not None else root / "contracts",
            graphs_dir=graphs_dir if graphs_dir is not None else root / "graphs",
            tests_dir=tests_dir if tests_dir is not None else root / "tests",
            license_path=license_path if license_path is not None else root / "LICENSE",
            manifest_path=manifest_path if manifest_path is not None else root / "target" / "manifest.json",
            estate_file=estate_file if estate_file is not None else root / "estate.yml",
            openim_root=Path(openim_root) if openim_root is not None else None,
        )

    @classmethod
    def default(cls) -> "EstateContext":
        """The ambient context, resolved once from the process's cwd/env at import time.

        Each emitter binds this as its module-level fallback so a plain ``load_domains()`` or
        a module path alias keeps resolving against the same estate a bare script invocation
        always used. A caller that wants a DIFFERENT estate constructs its own via ``resolve``
        and threads it through (``ctx=``); it never mutates this one.
        """
        return cls.resolve()
