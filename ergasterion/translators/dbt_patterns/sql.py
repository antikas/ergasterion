"""SQL fragments shared by every product-pattern renderer.

Nothing here knows a pattern. It knows how a neutral type is cast, how a
declared identifier is written into generated SQL, how a Python value
becomes a Jinja literal, and what a product's relations and models are
called. Every renderer in this package builds its text out of these, so
one identifier convention and one cast convention hold across all of them.

Casts go through the dispatch macros in ``macros/cross_db.sql``: the
adapter's own physical type mapping is the macro's job, never this
module's (architecture section 3.3, "Each adapter owns the mapping to its
physical types"). A neutral type the macros cannot cast to fails closed
here rather than rendering a guessed physical type.
"""

from __future__ import annotations

import json
import re
from typing import Any, Sequence

from ergasterion.framework.models import FrameworkError, RelationField

# The dispatch macro every scalar cast resolves through, and the one that
# keeps a declared decimal's precision and scale.
TYPE_MACRO = "dpf_type"
DECIMAL_TYPE_MACRO = "dpf_decimal_type"

# Declaration neutral scalar type (architecture section 3.3) to the
# dpf_type() token that renders it. The tokens are the ones every adapter's
# conventions.yml resolves (ergasterion.framework.adapters.
# NEUTRAL_TYPE_TOKENS); the mapping between the two vocabularies lives here,
# once.
SCALAR_TYPE_TOKENS: dict[str, str] = {
    "string": "string",
    "integer": "int",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
}

DECIMAL_TYPE_NAME = "decimal"

# A declared identifier is written into generated SQL unquoted, so it must
# already be a plain lower-case SQL name. Anything else fails closed rather
# than being quoted, folded or rewritten: identifier normalisation is a
# parked adapter question (P7), and guessing one here would make the same
# declaration render differently per adapter.
IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


class RenderingError(FrameworkError):
    """One product-rendering failure. Always names the product and, where
    the failure belongs to one occurrence, that occurrence."""

    code = "dbt_rendering_error"

    def __init__(self, *, product: str, rule: str, detail: str, occurrence: str | None = None) -> None:
        self.product = product
        self.rule = rule
        self.occurrence = occurrence
        self.detail = detail
        where = f" occurrence {occurrence!r}" if occurrence else ""
        super().__init__(f"product {product!r}: rule {rule!r}{where}: {detail}")


def identifier(value: object, *, product: str, occurrence: str | None = None) -> str:
    """A declared name, checked as a plain unquoted SQL identifier."""

    if not isinstance(value, str) or not IDENTIFIER_RE.match(value):
        raise RenderingError(
            product=product,
            occurrence=occurrence,
            rule="unrenderable_identifier",
            detail=(
                f"{value!r} is not a plain lower-case SQL identifier; the engine never quotes or "
                "folds a declared name, so it must already be one"
            ),
        )
    return value


def macro_call(name: str, *arguments: str) -> str:
    """One dispatch macro call as a generated artefact carries it: the
    macro name, its arguments and the braces the per-adapter parse gate
    resolves. Written here rather than by each renderer that needs one, so
    a call the gate cannot resolve cannot be spelled by accident."""

    return "{{ " + f"{name}({', '.join(arguments)})" + " }}"


# The publication instant every generated relation stamps its rows with.
# Dialect-divergent, so it is a dispatch macro rather than inline SQL.
PUBLISH_TIMESTAMP_CALL = macro_call("dpf_publish_timestamp")


def jinja_literal(value: Any) -> str:
    """A Python value as a Jinja literal. JSON's escaping is exactly the
    escaping a Jinja double-quoted string literal accepts, so a predicate
    or a relation name carrying a quote survives into the generated call
    unchanged."""

    return json.dumps(value)


def cast_expression(expression: str, declared_type: Any, *, product: str, occurrence: str) -> str:
    """``expression`` cast to the physical type ``declared_type`` maps to on
    whichever adapter compiles the model. A structured type (an estate's
    ``array`` and its kin) has no portable cast and fails closed naming the
    product, the occurrence and the type."""

    if isinstance(declared_type, str):
        token = SCALAR_TYPE_TOKENS.get(declared_type)
        if token is None:
            raise RenderingError(
                product=product,
                occurrence=occurrence,
                rule="unrenderable_type",
                detail=f"no cast is declared for neutral type {declared_type!r}",
            )
        return f"cast({expression} as {{{{ {TYPE_MACRO}('{token}') }}}})"
    if isinstance(declared_type, dict) and declared_type.get("name") == DECIMAL_TYPE_NAME:
        precision = declared_type["precision"]
        scale = declared_type["scale"]
        return (
            f"cast({expression} as "
            f"{{{{ {DECIMAL_TYPE_MACRO}({precision}, {scale}) }}}})"
        )
    raise RenderingError(
        product=product,
        occurrence=occurrence,
        rule="unrenderable_type",
        detail=(
            f"neutral type {declared_type!r} has no portable cast; a structured type is rendered "
            "by the shape or the named rule that produces it, never by a cast"
        ),
    )


def declared_type_projection(
    fields: Sequence[RelationField], *, product: str, occurrence: str
) -> list[str]:
    """Every field of one relation as a select-list entry: the column cast
    to the neutral type its contract declares, under its own name. One
    implementation, so a relation a shape renders and a relation a declared
    select body produces carry their declared types the same way."""

    return [
        "{expression} as {name}".format(
            expression=cast_expression(
                identifier(field.name, product=product, occurrence=occurrence),
                field.type,
                product=product,
                occurrence=occurrence,
            ),
            name=identifier(field.name, product=product, occurrence=occurrence),
        )
        for field in fields
    ]


def select_projection(
    columns: Sequence[str],
    source: str,
    *,
    where: str | None = None,
    distinct: bool = False,
    group_by: Sequence[str] = (),
) -> str:
    """A select of ``columns`` from ``source``, in the one layout every
    generated common table expression uses. One implementation, so two
    renderers can never lay the same select out differently and make a
    re-emission look like a change. Duplicate removal, a filter and a
    grouping are the three things a generated select ever adds around that
    projection, so each is a keyword here rather than text a caller
    assembles for itself."""

    lines = ["    select distinct" if distinct else "    select"]
    for position, column in enumerate(columns):
        comma = "," if position < len(columns) - 1 else ""
        lines.append(f"        {column}{comma}")
    lines.append(f"    from {source}")
    if where is not None:
        lines.append(f"    where {where}")
    if group_by:
        lines.append("    group by " + ", ".join(group_by))
    return "\n".join(lines)


def value_literal(value: Any, *, product: str, occurrence: str) -> str:
    """A declared scalar as the SQL literal a generated artefact carries.

    A validation bound and a regular-expression pattern reach the generated
    schema file as text a test macro interpolates straight into SQL, so the
    engine decides once, here, how a declared value is spelled: a boolean as
    a keyword, a number as digits, a string (a date among them) as a quoted
    literal. A value of any other shape fails closed rather than being
    rendered as whatever ``str()`` makes of it."""

    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return sql_string_literal(value)
    raise RenderingError(
        product=product,
        occurrence=occurrence,
        rule="unrenderable_literal",
        detail=(
            f"declared value {value!r} has no SQL literal; state a number, a boolean or a "
            "string"
        ),
    )


def model_name(domain: str, name: str) -> str:
    """The dbt model name for one product relation. Domain-qualified,
    because a dbt project's model names share one namespace across every
    domain the estate declares."""

    return f"{domain}__{name}"


def suffixed_model_name(domain: str, name: str, suffix: str) -> str:
    """The dbt model name for one relation rendered under a product's own
    name: a translator-private relation, or one of the relations its shape
    renders beside the composition's own."""

    return f"{domain}__{name}__{suffix}"


def suffixed_relation_name(domain: str, name: str, suffix: str) -> str:
    """That relation's name in the estate: a translator-private relation
    (architecture section 10) or one a shape publishes, both under the
    product's own namespace."""

    return f"{domain}.{name}__{suffix}"


def ref(model: str) -> str:
    """A dbt read of another model or seed in the same project."""

    return "{{ ref(" + jinja_literal(model) + ") }}"


def sql_string_literal(value: str) -> str:
    """``value`` as a single-quoted SQL string literal."""

    return "'" + value.replace("'", "''") + "'"


def normalise_generated_text(text: str) -> str:
    """Every generated artefact's final shape: no trailing whitespace on
    any line, no leading or trailing blank lines, one closing newline. One
    implementation, so two artefacts rendered by two templates can never
    differ by invisible characters and a re-emission is byte-stable."""

    return "\n".join(line.rstrip() for line in text.strip().splitlines()) + "\n"
