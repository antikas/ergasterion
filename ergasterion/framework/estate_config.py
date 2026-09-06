"""The estate configuration gate (architecture sections 9, 11; owner rulings
R1, R10; plan decisions D29, D35).

Everything an estate declares as policy on top of the engine -- the adapters
and the final target, the labels and the profiles each admits, the profiles
the estate declares of its own, and the translator table naming an owner per
label for each pattern and each shape -- is read and cross-checked here, once,
before a product declaration is validated or an occurrence is routed.

The three loaders behind it each keep their own job. ``ergasterion.estate``
reads the ``adapters``, ``final_target`` and ``translators`` blocks
structurally; ``ergasterion.framework.declaration.load_estate_policy`` reads
the expression mode, the structured types, the labels and the profile set.
This module adds the cross-checks none of them can make alone, because each
needs two blocks at once:

  * every declared adapter is one the engine actually carries, so an adapter
    named here has conventions, a dialect and a type mapping behind it;
  * every label in the translator table is a declared label, and every
    declared label has a table;
  * every table entry names a token that is either one of the fifteen
    patterns or a registered shape, and a translator this engine carries;
  * no table entry names a pattern every profile its label admits forbids --
    an owner for a pattern the label can never compose.

A table entry that is simply absent is not checked here: a pattern with no
owner is a failure of the occurrence that needs one, and the router
(``ergasterion.framework.routing.resolve_table_owners``) already fails closed
on it naming label, pattern and adapter (architecture check 12). Checking it
twice would put the same rule in two places and give one estate two different
messages for one fault.

Each failure names the estate file and the key at fault: the label, and the
pattern, shape, translator, profile or adapter under it. This gate is the
whole-table reading of the estate data, so a contradiction in the
configuration itself is reported before any work is done rather than at the
first product that happens to hit it.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from ergasterion.estate import EstateAdapters, load_estate_adapters, load_translator_table
from ergasterion.framework.adapters import UnknownAdapterError, load_adapter_conventions
from ergasterion.framework.declaration import EstatePolicy, load_estate_policy
from ergasterion.framework.models import FrameworkError, PatternDisposition, PatternId
from ergasterion.framework.shapes import registered_shape_names

PATTERN_TOKENS: frozenset[str] = frozenset(pattern.value for pattern in PatternId)


class EstateConfigurationError(FrameworkError):
    """One estate configuration failure. Always names the estate file and the
    key at fault."""

    code = "estate_configuration_error"


@dataclass(frozen=True)
class EstateConfiguration:
    """One estate's whole configuration, loaded and cross-checked once.

    ``policy`` carries the expression mode, the structured types, the labels
    and every profile the estate may name; ``adapters`` the declared adapters
    and the final target; ``translators`` the translator table, label by
    label."""

    policy: EstatePolicy
    adapters: EstateAdapters
    translators: dict[str, dict[str, str]]

    def adapter_names(self) -> tuple[str, ...]:
        return self.adapters.names()


def load_estate_configuration(
    estate_file: Path, *, translator_names: Sequence[str]
) -> EstateConfiguration:
    """Load and cross-check the whole estate configuration.

    ``translator_names`` is the set of translators the calling route carries,
    so a table entry naming a translator this engine has no implementation for
    fails closed here rather than at the occurrence that first needs it."""

    policy = load_estate_policy(estate_file)
    adapters = load_estate_adapters(estate_file)
    table = load_translator_table(estate_file)

    _check_adapters(estate_file, adapters)
    _check_table(estate_file, policy=policy, table=table, translator_names=tuple(translator_names))

    return EstateConfiguration(policy=policy, adapters=adapters, translators=table)


def _check_adapters(estate_file: Path, adapters: EstateAdapters) -> None:
    for name in adapters.names():
        try:
            load_adapter_conventions(name)
        except UnknownAdapterError as exc:
            raise EstateConfigurationError(
                f"{estate_file}: adapter {name!r} is not an adapter this engine carries: {exc}"
            ) from exc


def _check_table(
    estate_file: Path,
    *,
    policy: EstatePolicy,
    table: Mapping[str, Mapping[str, str]],
    translator_names: tuple[str, ...],
) -> None:
    for label in table:
        if label not in policy.labels:
            raise EstateConfigurationError(
                f"{estate_file}: translators name label {label!r}, which the estate does not declare; "
                f"declared labels: {sorted(policy.labels)!r}"
            )

    shape_names = registered_shape_names()
    for label, admitted in policy.labels.items():
        entries = table.get(label)
        if entries is None:
            raise EstateConfigurationError(
                f"{estate_file}: label {label!r} has no translator table; every label declares the "
                "translator that renders each pattern and each shape it composes"
            )
        profiles = [policy.profiles[name] for name in admitted]

        for token, translator_name in entries.items():
            if translator_name not in translator_names:
                raise EstateConfigurationError(
                    f"{estate_file}: label {label!r} names translator {translator_name!r} for "
                    f"{token!r}, which is not a translator this engine carries; carried: "
                    f"{sorted(translator_names)!r}"
                )
            if token in shape_names:
                continue
            if token not in PATTERN_TOKENS:
                raise EstateConfigurationError(
                    f"{estate_file}: label {label!r} names an owner for {token!r}, which is neither "
                    f"one of the fifteen patterns nor a registered shape; registered shapes: "
                    f"{list(shape_names)!r}"
                )
            pattern = PatternId(token)
            forbidding = [
                profile.name
                for profile in profiles
                if profile.disposition_of(pattern) is PatternDisposition.FORBIDDEN
            ]
            if len(forbidding) == len(profiles):
                raise EstateConfigurationError(
                    f"{estate_file}: label {label!r} names an owner for pattern {token!r}, which "
                    f"every profile it admits forbids: {sorted(forbidding)!r}"
                )
