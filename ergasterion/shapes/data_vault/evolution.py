"""The hashdiff basis and its record: how a vault product's satellites
evolve without reinterpreting what they already stored.

A satellite detects change by fingerprinting a column set. That column set
is the **hashdiff basis**, and every fingerprint already stored was
computed over it. Change the basis and every stored fingerprint means
something different from every new one, so the basis is frozen the moment
the first version is stored, and the estate carries it from one emission
to the next in the **evolution ledger**: one document per vault product,
under ``declarations/evolution/``, written by the emission route with
every other generated file and read by it before the next one.

Four gradings, and only the first two are silent:

  * **bootstrap** -- the ledger carries no record of this satellite, so
    the declared basis becomes the recorded one at basis version 1;
  * **extension** -- the declaration adds payload columns and leaves every
    recorded one in place. The basis stays frozen, the new columns are
    stored and are outside change detection, and the grading says so in a
    notice naming them;
  * **estate migration requirement** -- a recorded payload column is gone,
    the satellite's kind or change column is restated, a stored column's
    declared type is restated, or the declared basis differs from the
    recorded one for any reason other than a new column. Each fails closed
    naming the product, the shape, the satellite, the column, the change
    class and the remedy;
  * **pending basis** -- a re-baseline has been staged and not yet
    promoted. Emission stops until it is, because a build between the two
    would store versions under a basis nobody has adopted.

The re-baseline itself is three declared steps
(``ergasterion.shapes.data_vault.rebaseline``): stage the declared basis
as pending, promote it, then emit and build. Promotion increments the
basis version, and a satellite's change detection is scoped to its own
basis version, so the first build after a promotion stores exactly one
version per entity under the new basis and leaves every version stored
under the old one exactly as it was. Nothing rewrites stored history: a
fingerprint is a fact about the basis it was computed under, and the row
carries that basis version beside it. What each superseded basis covered
is kept on the record too (``superseded``), so a row tagged with an older
version can still be read against the column set and the declared types it
was stored under.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from ergasterion.framework.models import FrameworkError

# The shape this record belongs to. Named in every failure, because a
# product's declaration carries one shape section and a reader of the
# failure has to know which one answered.
SHAPE_NAME = "data_vault"

# Where one product's record sits inside the estate.
LEDGER_DIRECTORY = "declarations/evolution"
LEDGER_SUFFIX = ".vault.yml"

# The first basis version. A promotion adds one.
FIRST_BASIS_VERSION = 1

# The change classes a grading reports, each with the remedy it names.
CLASS_REMOVAL = "removal"
CLASS_REDEFINITION = "redefinition"
CLASS_RETYPE = "declared_type"
CLASS_BASIS = "hashdiff_basis"

# The two remedies this module names. A declared change the recorded basis
# cannot absorb starts the re-baseline; a re-baseline already staged is
# finished or abandoned, and telling its owner to stage it again would be
# the one instruction that cannot help.
REMEDY = (
    "re-baseline the satellite: `ergasterion vault-rebaseline --product {product} "
    "--satellite {satellite} --stage`, then `--promote`"
)
PENDING_REMEDY = (
    "finish the staged re-baseline: `ergasterion vault-rebaseline --product {product} "
    "--satellite {satellite} --promote`, or abandon it with `--abort`"
)


class VaultEvolutionError(FrameworkError):
    """A declared change to a satellite that the estate's recorded hashdiff
    basis cannot absorb. Always names the product, the shape, the
    satellite, what changed, the change class and the remedy."""

    code = "estate_migration_requirement"

    def __init__(
        self,
        *,
        product: str,
        satellite: str,
        change_class: str,
        detail: str,
        remedy: str,
    ) -> None:
        self.product = product
        self.shape = SHAPE_NAME
        self.satellite = satellite
        self.change_class = change_class
        self.detail = detail
        self.remedy = remedy
        super().__init__(
            f"product {product!r}: shape {SHAPE_NAME!r}: satellite {satellite!r}: "
            f"{change_class}: {detail}; remedy: {remedy}"
        )


class PendingBasisError(FrameworkError):
    """A re-baseline is staged and not promoted. Emission stops here rather
    than storing versions under a basis nobody adopted."""

    code = "pending_hashdiff_basis"

    def __init__(self, *, product: str, satellite: str, remedy: str) -> None:
        self.product = product
        self.shape = SHAPE_NAME
        self.satellite = satellite
        self.remedy = remedy
        super().__init__(
            f"product {product!r}: shape {SHAPE_NAME!r}: satellite {satellite!r}: a re-baseline "
            f"is staged and not promoted; remedy: {remedy}"
        )


def ledger_relative_path(*, domain: str, name: str) -> str:
    """Where one vault product's record sits inside the estate."""

    return f"{LEDGER_DIRECTORY}/{domain}.{name}{LEDGER_SUFFIX}"


def declared_record(entry: Mapping[str, Any], *, types: Mapping[str, Any]) -> dict:
    """One satellite's declared shape as the ledger records it: the payload
    roster, the columns excluded from change detection, the basis those two
    leave, the kind, the column ordering its versions, and the neutral type
    each stored column publishes as.

    The roster comes straight from the declaration and the types from the
    relation the shape renders from it, so the emission route and the
    re-baseline command derive the same record from the same two sources."""

    payload = [str(column) for column in entry.get("payload") or []]
    exclusions = [str(column) for column in entry.get("hashdiff_exclude") or []]
    return {
        "kind": str(entry.get("kind")),
        "change_column": str(entry.get("change_column")),
        "payload": payload,
        "exclusions": exclusions,
        "basis": [column for column in payload if column not in exclusions],
        "types": {column: types[column] for column in payload if column in types},
    }


def declared_records(
    entries: Sequence[Mapping[str, Any]], *, types: Mapping[str, Mapping[str, Any]]
) -> dict[str, dict]:
    """Every declared satellite of one product, keyed by its declared
    name. ``types`` carries, per satellite, the neutral type each of its
    stored columns publishes as."""

    return {
        str(entry.get("name")): declared_record(
            entry, types=types.get(str(entry.get("name"))) or {}
        )
        for entry in entries
    }


def _remedy(product: str, satellite: str) -> str:
    return REMEDY.format(product=product, satellite=satellite)


def _pending_remedy(product: str, satellite: str) -> str:
    return PENDING_REMEDY.format(product=product, satellite=satellite)


def _grade_one(
    *, product: str, satellite: str, declared: Mapping[str, Any], recorded: Mapping[str, Any]
) -> tuple[dict, list[str]]:
    """One satellite graded against its recorded basis. Returns the record
    the ledger keeps and the notices the grading produced."""

    remedy = _remedy(product, satellite)
    if recorded.get("pending"):
        raise PendingBasisError(
            product=product, satellite=satellite, remedy=_pending_remedy(product, satellite)
        )

    recorded_payload = [str(column) for column in recorded.get("payload") or []]
    recorded_basis = [str(column) for column in recorded.get("basis") or []]
    basis_version = int(recorded.get("basis_version") or FIRST_BASIS_VERSION)

    for field, label in (("kind", "kind"), ("change_column", "change column")):
        if str(recorded.get(field)) != declared[field]:
            raise VaultEvolutionError(
                product=product,
                satellite=satellite,
                change_class=CLASS_REDEFINITION,
                detail=(
                    f"the {label} is restated from {str(recorded.get(field))!r} to "
                    f"{declared[field]!r}; every version already stored was detected under the "
                    "recorded one"
                ),
                remedy=remedy,
            )

    recorded_types = dict(recorded.get("types") or {})
    restated = [
        (column, recorded_types[column], declared["types"][column])
        for column in recorded_payload
        if column in recorded_types
        and column in declared["types"]
        and recorded_types[column] != declared["types"][column]
    ]
    if restated:
        column, was, now = restated[0]
        raise VaultEvolutionError(
            product=product,
            satellite=satellite,
            change_class=CLASS_RETYPE,
            detail=(
                f"column {column!r} is restated from {was!r} to {now!r}, and the satellite "
                "already stores versions written under the recorded type"
            ),
            remedy=remedy,
        )

    removed = [column for column in recorded_payload if column not in declared["payload"]]
    if removed:
        raise VaultEvolutionError(
            product=product,
            satellite=satellite,
            change_class=CLASS_REMOVAL,
            detail=(
                f"payload column(s) {', '.join(removed)} are no longer declared, and the "
                "satellite already stores versions carrying them"
            ),
            remedy=remedy,
        )

    added = [column for column in declared["payload"] if column not in recorded_payload]
    frozen = [column for column in declared["basis"] if column not in added]
    if frozen != recorded_basis:
        raise VaultEvolutionError(
            product=product,
            satellite=satellite,
            change_class=CLASS_BASIS,
            detail=(
                f"change detection is declared over {frozen} and every stored fingerprint was "
                f"computed over {recorded_basis}"
            ),
            remedy=remedy,
        )

    notices: list[str] = []
    outside = [column for column in added if column not in recorded_basis]
    if outside:
        notices.append(
            f"{product}: satellite {satellite!r}: payload column(s) {', '.join(outside)} are "
            f"stored and stay outside change detection; the hashdiff basis remains the "
            f"{len(recorded_basis)} column(s) recorded at basis version {basis_version}"
        )
    record = {
        "kind": declared["kind"],
        "change_column": declared["change_column"],
        "basis_version": basis_version,
        "basis": list(recorded_basis),
        "payload": list(declared["payload"]),
        "exclusions": list(declared["exclusions"]),
        "types": dict(declared["types"]),
    }
    superseded = list(recorded.get("superseded") or [])
    if superseded:
        record["superseded"] = superseded
    return record, notices


def grade(
    *, product: str, declared: Mapping[str, Mapping[str, Any]], recorded: Mapping[str, Any] | None
) -> tuple[dict, tuple[str, ...]]:
    """This product's evolution ledger, graded. ``declared`` is every
    satellite the declaration carries (``declared_records``) and
    ``recorded`` the document the estate carries from the last emission, or
    ``None`` on the first one."""

    previous = dict((recorded or {}).get("satellites") or {})
    satellites: dict[str, dict] = {}
    notices: list[str] = []
    for satellite in sorted(declared):
        entry = declared[satellite]
        stored = previous.get(satellite)
        if stored is None:
            satellites[satellite] = {
                "kind": entry["kind"],
                "change_column": entry["change_column"],
                "basis_version": FIRST_BASIS_VERSION,
                "basis": list(entry["basis"]),
                "payload": list(entry["payload"]),
                "exclusions": list(entry["exclusions"]),
                "types": dict(entry["types"]),
            }
            notices.append(
                f"{product}: satellite {satellite!r}: hashdiff basis recorded at version "
                f"{FIRST_BASIS_VERSION} over {', '.join(entry['basis'])}"
            )
            continue
        record, satellite_notices = _grade_one(
            product=product, satellite=satellite, declared=entry, recorded=stored
        )
        satellites[satellite] = record
        notices.extend(satellite_notices)

    for satellite in sorted(previous):
        if satellite in declared:
            continue
        notices.append(
            f"{product}: satellite {satellite!r} is no longer declared; the versions it stored "
            "are no longer maintained by this product"
        )

    return {"product": product, "satellites": satellites}, tuple(notices)


def basis_versions(document: Mapping[str, Any]) -> dict[str, int]:
    """The basis version each satellite's fingerprints are computed under,
    read off one graded ledger. The generated satellite carries this number
    on every row it stores, and scopes its change detection to it."""

    return {
        name: int(entry.get("basis_version") or FIRST_BASIS_VERSION)
        for name, entry in ((document.get("satellites") or {}) or {}).items()
    }


def stage(
    *, document: dict, satellite: str, declared: Mapping[str, Any], product: str
) -> dict:
    """Record the declared basis as this satellite's pending one. The next
    emission stops on the pending-basis gate until the pending basis is
    promoted or abandoned."""

    entry = _entry(document, satellite=satellite, product=product)
    entry["pending"] = {
        "basis": list(declared["basis"]),
        "payload": list(declared["payload"]),
        "exclusions": list(declared["exclusions"]),
        "kind": declared["kind"],
        "change_column": declared["change_column"],
        "types": dict(declared["types"]),
    }
    return document


def promote(*, document: dict, satellite: str, product: str) -> dict:
    """Adopt the pending basis. The basis version advances, so the first
    build after this stores one version per entity under the new basis and
    leaves every version stored under the old one untouched.

    The superseded record is archived rather than overwritten. Rows already
    stored carry the version they were written under, and that version has
    to stay readable: what columns it covered, and what type each of them
    was stored as."""

    entry = _entry(document, satellite=satellite, product=product)
    pending = entry.get("pending")
    if not pending:
        raise VaultEvolutionError(
            product=product,
            satellite=satellite,
            change_class=CLASS_BASIS,
            detail="no re-baseline is staged for this satellite",
            remedy=_remedy(product, satellite),
        )
    superseded = list(entry.get("superseded") or [])
    superseded.append(
        {
            "basis_version": int(entry.get("basis_version") or FIRST_BASIS_VERSION),
            "basis": list(entry.get("basis") or []),
            "payload": list(entry.get("payload") or []),
            "types": dict(entry.get("types") or {}),
        }
    )
    entry["basis_version"] = int(entry.get("basis_version") or FIRST_BASIS_VERSION) + 1
    entry["basis"] = list(pending["basis"])
    entry["payload"] = list(pending["payload"])
    entry["exclusions"] = list(pending["exclusions"])
    entry["kind"] = pending["kind"]
    entry["change_column"] = pending["change_column"]
    entry["types"] = dict(pending.get("types") or {})
    entry["superseded"] = superseded
    entry.pop("pending", None)
    return document


def abort(*, document: dict, satellite: str, product: str) -> dict:
    """Abandon the staged re-baseline. The recorded basis is unchanged, so
    the declared change that prompted it fails closed again on the next
    emission."""

    entry = _entry(document, satellite=satellite, product=product)
    if not entry.pop("pending", None):
        raise VaultEvolutionError(
            product=product,
            satellite=satellite,
            change_class=CLASS_BASIS,
            detail="no re-baseline is staged for this satellite",
            remedy=_remedy(product, satellite),
        )
    return document


def _entry(document: Mapping[str, Any], *, satellite: str, product: str) -> dict:
    satellites = (document.get("satellites") or {}) if document else {}
    entry = satellites.get(satellite)
    if entry is None:
        raise VaultEvolutionError(
            product=product,
            satellite=satellite,
            change_class=CLASS_BASIS,
            detail=(
                f"the ledger records no satellite {satellite!r} for this product: "
                f"{sorted(satellites)}"
            ),
            remedy="emit the product once so its hashdiff basis is recorded",
        )
    return entry


__all__ = [
    "CLASS_BASIS",
    "CLASS_REDEFINITION",
    "CLASS_REMOVAL",
    "CLASS_RETYPE",
    "FIRST_BASIS_VERSION",
    "LEDGER_DIRECTORY",
    "LEDGER_SUFFIX",
    "PENDING_REMEDY",
    "REMEDY",
    "SHAPE_NAME",
    "PendingBasisError",
    "VaultEvolutionError",
    "abort",
    "basis_versions",
    "declared_record",
    "declared_records",
    "grade",
    "ledger_relative_path",
    "promote",
    "stage",
]
