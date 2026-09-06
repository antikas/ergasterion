"""The shape plug-ins (architecture section 6: "Registering a shape is a
plug-in act, never an engine change").

One package per shape. Each names its ``target.shape_config`` schema, the
relations it renders with the fields each one publishes, and the
constraints it adds; each registers itself with
``ergasterion.framework.shapes``. Importing this package is what loads
them, and the registry does that the first time anything asks it a
question, so a shape is added by adding a package here and naming it in an
estate's translator table -- never by changing the engine.

Rendering a registered shape into artefacts belongs to the translator the
estate's table names for it, never to these packages.
"""

from __future__ import annotations

from ergasterion.shapes import canonical as canonical
from ergasterion.shapes import data_vault as data_vault
from ergasterion.shapes import dimensional as dimensional
from ergasterion.shapes import ods as ods

__all__ = ["canonical", "data_vault", "dimensional", "ods"]
