"""Package anchor for the source tree containing the running ``ergasterion`` package.

``REPO_ROOT = Path(__file__).resolve().parents[1]`` is the parent of ``ergasterion/``: for both
script-mode (``python ergasterion/emit.py``) and an editable install this is the source tree.

This is a dependency-free leaf of the import graph. ``ergasterion.estate`` imports it as the final
fallback of the estate-root resolution chain (``--estate-root`` > ``DPF_ESTATE_ROOT`` >
cwd walk-up > this anchor): when no estate is named and the cwd is not inside one, the estate
is assumed co-located with the engine's own source tree for development and direct invocation.

It is a PACKAGE-location fact, not an estate path. Estate paths ride ``EstateContext`` (see
``ergasterion/estate.py``); this only seeds the chain's last resort.
"""
from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
