"""Paths for the data-product estate operated on by the installed engine.

Two-class path principle (the whole point of this module, stated so no caller mis-threads
it):

  * ENGINE DATA -- Ergasterion's shipped assets: the Jinja templates and the vendored
    JSON Schemas. These resolve PACKAGE-relative, ``__file__``-anchored inside ``ergasterion/``
    and shipped as package data (pyproject ``[tool.setuptools.package-data]``). They are
    NOT estate paths and never ride an EstateContext -- an installed wheel finds them next
    to its own code regardless of which estate it is pointed at. Each emitter keeps its own
    ``TEMPLATES_DIR`` / ``SCHEMA_PATH`` = ``Path(__file__).resolve().parent / ...``.

  * ESTATE PATHS -- the data-product estate the engine operates on: declarations,
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
     ``dbt_project.yml`` and a ``declarations/`` directory -- dbt's own project-resolution
     precedent, no new marker file invented;
  4. failing all of the above, the package anchor (``ergasterion/`` -> its parent), which is the
     source-tree / editable-install case: the estate is co-located with the engine. This
     keeps a bare ``python ergasterion/emit_products.py`` from a repository-root working
     directory resolving to the co-located estate.

Reference-model resolution, highest precedence first, owned by
``resolve_openim_root`` so the command, its skip message and every test read
one answer:

  1. an explicit ``openim_root`` (the ``--openim-root`` flag);
  2. the ``DPF_OPENIM_ROOT`` environment variable;
  3. a checkout named ``openim`` beside the estate root, taken only when it
     carries the reference model's own ``model/`` directory;
  4. failing all of the above, ``None`` -- there is no repo-literal default,
     and a command with nothing to validate against says so.

Every path field is INDIVIDUALLY overridable at construction (``resolve(declarations_dir=...)``)
so a test can point one path at a temp directory without monkeypatching a module global.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from ergasterion._repo_root import REPO_ROOT

ESTATE_ROOT_ENV = "DPF_ESTATE_ROOT"
# The reference model an estate's canonical products may map onto: the
# environment variable that names a checkout of it, the directory name it is
# looked for under beside the estate, and the directory inside a checkout
# that makes it one. Named here because the resolution below, the command
# that reads it and the message it prints when it finds nothing all have to
# mean the same three things.
OPENIM_ROOT_ENV = "DPF_OPENIM_ROOT"
OPENIM_SIBLING_NAME = "openim"
REFERENCE_MODEL_DIRECTORY = "model"
# The walk-up markers: an estate root is the nearest ancestor carrying BOTH of these.
_MARKER_FILE = "dbt_project.yml"
_MARKER_DIR = "declarations"


def _walk_up_root(start: Path) -> Path | None:
    """Nearest ancestor of ``start`` (inclusive) carrying dbt_project.yml + declarations/, or None."""
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


def resolve_openim_root(
    openim_root: str | os.PathLike[str] | None = None,
    *,
    env: dict[str, str] | None = None,
    root: Path | None = None,
) -> Path | None:
    """Resolve the reference model checkout by the precedence chain in the
    module docstring, or ``None`` when there is none to resolve.

    The flag and the environment variable are taken as given, so a path that
    is not a checkout reaches the caller and is reported as such rather than
    silently falling through to something else. The sibling is a convention
    rather than a statement, so it is taken only when it carries the
    reference model's own directory."""

    if openim_root is not None:
        return Path(openim_root)
    environ = os.environ if env is None else env
    from_env = environ.get(OPENIM_ROOT_ENV)
    if from_env:
        return Path(from_env)
    if root is not None:
        sibling = root.parent / OPENIM_SIBLING_NAME
        if (sibling / REFERENCE_MODEL_DIRECTORY).is_dir():
            return sibling
    return None


@dataclass(frozen=True)
class EstateContext:
    """Every estate path the emitters read, all derived from one resolved estate root.

    Construct via :meth:`resolve` (which runs the precedence chain) or :meth:`default` (the cached
    ambient context each emitter falls back to when no caller supplies one). ``openim_root``
    is whatever ``resolve_openim_root`` found, and ``None`` when it found nothing -- the
    canonical-mapping validation is skip-safe, so a None here simply skips it; there is no
    repo-literal default path.
    """

    root: Path
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
            declarations_dir=declarations_dir if declarations_dir is not None else root / "declarations",
            models_dir=models_dir if models_dir is not None else root / "models",
            contracts_dir=contracts_dir if contracts_dir is not None else root / "contracts",
            graphs_dir=graphs_dir if graphs_dir is not None else root / "graphs",
            tests_dir=tests_dir if tests_dir is not None else root / "tests",
            license_path=license_path if license_path is not None else root / "LICENSE",
            manifest_path=manifest_path if manifest_path is not None else root / "target" / "manifest.json",
            estate_file=estate_file if estate_file is not None else root / "estate.yml",
            openim_root=resolve_openim_root(openim_root, env=env, root=root),
        )

    @classmethod
    def default(cls) -> "EstateContext":
        """The ambient context, resolved once from the process's cwd/env at import time.

        Each emitter binds this as its module-level fallback so a module path alias keeps
        resolving against the same estate a bare script invocation always used. A caller that wants a DIFFERENT estate constructs its own via ``resolve``
        and threads it through (``ctx=``); it never mutates this one.
        """
        return cls.resolve()


# --------------------------------------------------------------------- estate.yml: adapters and translators
#
# estate.yml's `adapters:` and `translators:` blocks are ESTATE data (unlike
# ergasterion/adapters/<name>/conventions.yml, which is engine package data --
# see ergasterion.framework.adapters's module docstring for that split).
# Loaders here raise plain ValueError on a malformed estate, the same
# convention ergasterion.structure_gate.load_structure_declarations already
# uses for estate.yml's siblings under declarations/targets/: these are
# estate misconfigurations, not a framework-layer error, so this module
# stays free of any ergasterion.framework import (estate.py is imported BY
# the framework -- ergasterion.framework.declaration reads EstateContext --
# never the other way round).


@dataclass(frozen=True)
class EstateAdapterDeclaration:
    """One estate-declared adapter: its name and kind ("reference" or
    "deployment", architecture section 10)."""

    name: str
    kind: str


@dataclass(frozen=True)
class EstateAdapters:
    """``estate.yml``'s ``adapters:`` block plus its ``final_target``
    (architecture section 11; owner ruling R10; plan decision D35: "the
    adapter held to the highest evidence standard")."""

    adapters: tuple[EstateAdapterDeclaration, ...]
    final_target: str

    def names(self) -> tuple[str, ...]:
        return tuple(a.name for a in self.adapters)

    def kind_of(self, name: str) -> str | None:
        for a in self.adapters:
            if a.name == name:
                return a.kind
        return None


_ADAPTER_KINDS = ("reference", "deployment")


def load_estate_adapters(estate_file: Path) -> EstateAdapters:
    """Load and validate ``estate.yml``'s ``adapters:`` block and
    ``final_target``. Fails closed with plain ``ValueError`` on a missing
    file, a missing block, an unknown kind, no declared reference adapter, no
    declared deployment adapter, or a ``final_target`` naming an adapter the
    block does not declare."""

    if not estate_file.is_file():
        raise ValueError(f"estate file is missing: {estate_file}")
    document = yaml.safe_load(estate_file.read_text(encoding="utf-8")) or {}
    estate = document.get("estate")
    if not isinstance(estate, dict):
        raise ValueError(f"{estate_file}: missing its top-level 'estate:' block")

    raw_adapters = estate.get("adapters")
    if not isinstance(raw_adapters, dict) or not raw_adapters:
        raise ValueError(f"{estate_file}: 'adapters' must be a non-empty mapping of adapter name to {{kind: ...}}")

    declarations: list[EstateAdapterDeclaration] = []
    for name, body in raw_adapters.items():
        if not isinstance(body, dict) or "kind" not in body:
            raise ValueError(f"{estate_file}: adapter {name!r} must carry a 'kind'")
        kind = body["kind"]
        if kind not in _ADAPTER_KINDS:
            raise ValueError(f"{estate_file}: adapter {name!r} kind must be one of {_ADAPTER_KINDS}, got {kind!r}")
        declarations.append(EstateAdapterDeclaration(name=str(name), kind=str(kind)))

    if not any(d.kind == "reference" for d in declarations):
        raise ValueError(f"{estate_file}: no adapter declares kind 'reference'")
    if not any(d.kind == "deployment" for d in declarations):
        raise ValueError(f"{estate_file}: no adapter declares kind 'deployment'")

    final_target = estate.get("final_target")
    names = tuple(d.name for d in declarations)
    if not isinstance(final_target, str) or final_target not in names:
        raise ValueError(
            f"{estate_file}: 'final_target' must name one of the declared adapters {names!r}, "
            f"got {final_target!r}"
        )

    return EstateAdapters(adapters=tuple(declarations), final_target=final_target)


def load_translator_table(estate_file: Path) -> dict[str, dict[str, str]]:
    """Load ``estate.yml``'s ``translators:`` block: for each layer label,
    the translator that renders each pattern or shape (architecture sections
    9, 11; owner ruling R1; plan decision D29). Structural validation only --
    every key and value a non-empty string -- since the router
    (``ergasterion.framework.routing.TranslationRouter._route_by_table``)
    is what fails closed on a missing entry or a missing capability, naming
    label, pattern and adapter (architecture check 12); this loader's job is
    only to hand the router well-shaped data. Fails closed with plain
    ``ValueError`` on a missing file, a missing block, or a malformed entry."""

    if not estate_file.is_file():
        raise ValueError(f"estate file is missing: {estate_file}")
    document = yaml.safe_load(estate_file.read_text(encoding="utf-8")) or {}
    estate = document.get("estate")
    if not isinstance(estate, dict):
        raise ValueError(f"{estate_file}: missing its top-level 'estate:' block")

    raw_translators = estate.get("translators")
    if not isinstance(raw_translators, dict) or not raw_translators:
        raise ValueError(
            f"{estate_file}: 'translators' must be a non-empty mapping of layer label to "
            "{pattern-or-shape: translator name}"
        )

    table: dict[str, dict[str, str]] = {}
    for label, entries in raw_translators.items():
        if not isinstance(label, str) or not label:
            raise ValueError(f"{estate_file}: translators: every label key must be a non-empty string")
        if not isinstance(entries, dict) or not entries:
            raise ValueError(f"{estate_file}: translators: label {label!r} must map to a non-empty mapping")
        label_table: dict[str, str] = {}
        for pattern_or_shape, translator_name in entries.items():
            if (
                not isinstance(pattern_or_shape, str)
                or not pattern_or_shape
                or not isinstance(translator_name, str)
                or not translator_name
            ):
                raise ValueError(
                    f"{estate_file}: translators.{label}: every entry must map a non-empty "
                    "pattern-or-shape string to a non-empty translator name string"
                )
            label_table[pattern_or_shape] = translator_name
        table[label] = label_table
    return table
