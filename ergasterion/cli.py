"""``ergasterion`` console entry point -- multiplexes factory emitters and Bronze commands.

    ergasterion emit [args...]          ->  ergasterion.emit:main
    ergasterion evolve <args...>        ->  ergasterion.emit:evolve_main
        evolve rebaseline <domain> <entity>   recompute stored hashdiffs under a new basis
        evolve audit-window <source> <table>  report keys wholly outside the delta window
    ergasterion contracts [args...]     ->  ergasterion.emit_contracts:main
    ergasterion odps [args...]          ->  ergasterion.emit_odps:main
    ergasterion graph [args...]         ->  ergasterion.emit_graph:main
    ergasterion import-odcs <args...>   ->  ergasterion.import_odcs:main
    ergasterion import-ddl <args...>    ->  ergasterion.import_ddl:main
    ergasterion lint [args...]          ->  ergasterion.dialect_lint:main
    ergasterion structure [args...]     ->  ergasterion.structure_gate:main
    ergasterion init <dir>              ->  ergasterion.init:main

Bronze local-ingestion commands (lazy-imported so ``--help`` works without DuckDB):

    plan, contract, deployment, ingest, reconcile, local-backup, status, inspect, quarantine

Each emitter subcommand delegates to that module's existing ``main()``. Bronze
commands live in ``ergasterion.ingestion.commands`` and share
``--project-dir --source --table --binding --environment``.
"""
from __future__ import annotations

import importlib
import sys

from ergasterion.ingestion.settings import MISSING_EXTRA_REMEDY

# subcommand token -> the module whose main() implements it.
SUBCOMMANDS: dict[str, str] = {
    "emit": "ergasterion.emit",
    "evolve": "ergasterion.emit",
    "contracts": "ergasterion.emit_contracts",
    "odps": "ergasterion.emit_odps",
    "graph": "ergasterion.emit_graph",
    "import-odcs": "ergasterion.import_odcs",
    "import-ddl": "ergasterion.import_ddl",
    "lint": "ergasterion.dialect_lint",
    "structure": "ergasterion.structure_gate",
    "init": "ergasterion.init",
    "plan": "ergasterion.ingestion.commands",
    "contract": "ergasterion.ingestion.commands",
    "deployment": "ergasterion.ingestion.commands",
    "ingest": "ergasterion.ingestion.commands",
    "reconcile": "ergasterion.ingestion.commands",
    "local-backup": "ergasterion.ingestion.commands",
    "status": "ergasterion.ingestion.commands",
    "inspect": "ergasterion.ingestion.commands",
    "quarantine": "ergasterion.ingestion.commands",
}

# Subcommands whose module entry point is named something other than main(). The
# estate-evolution operations live beside the emitter that owns the evolution ledger,
# so they share ergasterion.emit and carry their own entry point.
SUBCOMMAND_ENTRYPOINTS: dict[str, str] = {
    "evolve": "evolve_main",
}

INGESTION_COMMANDS = frozenset({
    "plan", "contract", "deployment", "ingest", "reconcile",
    "local-backup", "status", "inspect", "quarantine",
})


def _usage() -> str:
    names = ", ".join(SUBCOMMANDS)
    return (
        "usage: ergasterion <subcommand> [args...]\n\n"
        "Bronze is the source-aligned product layer: an immutable received-batch "
        "receipt, typed records with validation disposition, an accepted downstream "
        "projection, quarantine, and lineage. The file connector consumes a sidecar "
        "manifest plus payload at the received-batch boundary. A direct connector is "
        "another implementation of the same source-connector port; it does not change "
        "the Bronze contract or downstream interfaces.\n\n"
        "Read-only inspection: plan, status, inspect, quarantine --action list, "
        "ingest due --dry-run.\n"
        "Mutating commands: contract register/activate, deployment register/activate, "
        "ingest file, ingest due, reconcile, quarantine revalidate/release, local-backup.\n\n"
        "Bronze commands require --project-dir PATH --source NAME --table KEY "
        "--binding PATH --environment NAME. RuntimeBinding.environment is the source "
        "of truth; --environment is a mandatory assertion.\n"
        "Safe next actions: plan, then contract register/activate, then deployment "
        "register/activate, then ingest file. Use status and inspect to read evidence. "
        "Use local-backup only when the runtime is quiescent.\n\n"
        f"subcommands: {names}\n"
        "run `ergasterion <subcommand> --help` for a subcommand's own options "
        "(command syntax is defined by those parsers).\n"
    )


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in ("-h", "--help"):
        sys.stdout.write(_usage())
        return 0
    sub, rest = args[0], args[1:]
    module_name = SUBCOMMANDS.get(sub)
    if module_name is None:
        sys.stderr.write(f"ergasterion: unknown subcommand {sub!r}\n\n{_usage()}")
        return 2
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        if sub in INGESTION_COMMANDS:
            sys.stderr.write(f"missing_extra: {MISSING_EXTRA_REMEDY}\n")
            return 2
        raise
    sys.argv = [f"ergasterion {sub}", *rest]
    if sub in INGESTION_COMMANDS:
        return int(module.main([sub, *rest]))
    entrypoint = SUBCOMMAND_ENTRYPOINTS.get(sub)
    if entrypoint is not None:
        return int(getattr(module, entrypoint)(rest))
    return int(module.main())


if __name__ == "__main__":
    raise SystemExit(main())
