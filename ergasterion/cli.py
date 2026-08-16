"""``ergasterion`` console entry point -- multiplexes the factory's emitters as subcommands.

    ergasterion emit [args...]          ->  ergasterion.emit:main
    ergasterion contracts [args...]     ->  ergasterion.emit_contracts:main
    ergasterion odps [args...]          ->  ergasterion.emit_odps:main
    ergasterion graph [args...]         ->  ergasterion.emit_graph:main
    ergasterion import-odcs <args...>   ->  ergasterion.import_odcs:main
    ergasterion import-ddl <args...>    ->  ergasterion.import_ddl:main
    ergasterion lint [args...]          ->  ergasterion.dialect_lint:main
    ergasterion structure [args...]     ->  ergasterion.structure_gate:main
    ergasterion init <dir>              ->  ergasterion.init:main

Each subcommand delegates to that module's existing ``main()``, which owns its own
``argparse`` surface. The multiplexer strips the subcommand token and hands the remaining
argv to that ``main`` (via ``sys.argv``), so ``ergasterion emit --help`` prints the
emitter's own help verbatim. The subcommand map is the only engine-side seam; the emitters
are untouched by the entry point.
"""
from __future__ import annotations

import importlib
import sys

# subcommand token -> the module whose main() implements it.
SUBCOMMANDS: dict[str, str] = {
    "emit": "ergasterion.emit",
    "contracts": "ergasterion.emit_contracts",
    "odps": "ergasterion.emit_odps",
    "graph": "ergasterion.emit_graph",
    "import-odcs": "ergasterion.import_odcs",
    "import-ddl": "ergasterion.import_ddl",
    "lint": "ergasterion.dialect_lint",
    "structure": "ergasterion.structure_gate",
    "init": "ergasterion.init",
}


def _usage() -> str:
    names = ", ".join(SUBCOMMANDS)
    return (
        "usage: ergasterion <subcommand> [args...]\n\n"
        f"subcommands: {names}\n"
        "run `ergasterion <subcommand> --help` for a subcommand's own options\n"
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
    module = importlib.import_module(module_name)
    # Hand the delegate its own clean argv: prog name + the remaining tokens. Each
    # emitter's main() reads sys.argv through argparse; `--help` there exits via argparse.
    sys.argv = [f"ergasterion {sub}", *rest]
    return int(module.main())


if __name__ == "__main__":
    raise SystemExit(main())
