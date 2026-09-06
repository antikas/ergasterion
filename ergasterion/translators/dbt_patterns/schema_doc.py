"""The generated dbt schema document for one product.

Everything a dbt project learns about a generated relation without reading
its SQL lives here: the product's own model with the contract metadata and
the published columns, every translator-private relation rendered beside
it, and every generated test.

A product publishes one model per relation its shape renders, so the
document carries one entry per published relation and one per private
relation beside them.

A generated test is written in dbt's nested form: the test's own keyword
arguments sit under ``arguments``, its severity and any other dbt setting
under ``config``, and the test's name beside them. The flat form, where a
test's arguments sit at the top level of the test mapping, is deprecated by
dbt and would put a declared value where dbt looks for one of its own
settings.

The document is built as data and dumped once, rather than rendered from a
template, because a test argument is a declared value of any shape -- a
number, a list, a rendered SQL literal -- and only a real YAML dump can
carry all of them unambiguously.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import yaml

SEVERITY_ERROR = "error"
SEVERITY_WARN = "warn"


@dataclass(frozen=True)
class GeneratedTest:
    """One dbt test the translator generates. ``column`` is the column the
    test hangs from, or ``None`` for a test over the whole relation."""

    model: str
    test: str
    name: str
    column: str | None = None
    arguments: Mapping[str, Any] = field(default_factory=dict)
    severity: str = SEVERITY_ERROR
    store_failures: bool = False


def _test_entry(test: GeneratedTest) -> dict[str, Any]:
    body: dict[str, Any] = {"name": test.name}
    if test.arguments:
        body["arguments"] = dict(test.arguments)
    configuration: dict[str, Any] = {"severity": test.severity}
    if test.store_failures:
        configuration["store_failures"] = True
    body["config"] = configuration
    return {test.test: body}


def _tests_for(tests: Sequence[GeneratedTest], *, model: str, column: str | None) -> list[dict]:
    return [
        _test_entry(test)
        for test in tests
        if test.model == model and test.column == column
    ]


@dataclass(frozen=True)
class PublishedModel:
    """One model a product publishes: the dbt model, the relation its
    contract publishes it as, and one entry per published column carrying
    its ``name`` and, where a named rule computed it, a ``rule`` mapping
    with the rule and the declared rule version. A product whose shape
    renders several relations supplies one of these per relation."""

    model: str
    relation: str
    columns: tuple[Mapping[str, Any], ...]


def build_schema_document(
    *,
    product: str,
    profile: str,
    shape: str,
    publication_mode: str,
    published: Sequence[PublishedModel],
    auxiliary_models: Sequence[Mapping[str, Any]],
    tests: Sequence[GeneratedTest],
) -> dict[str, Any]:
    """The whole schema document for one product: every model it
    publishes with its columns and the named rule each computed one
    carries, every auxiliary model, and every generated test placed on the
    model or column it belongs to.

    ``auxiliary_models`` carries one entry per private relation: its model
    ``name``, a ``description``, and the ``columns`` a generated test hangs
    from."""

    models: list[dict[str, Any]] = []
    for entry_model in published:
        product_columns: list[dict[str, Any]] = []
        for column in entry_model.columns:
            entry: dict[str, Any] = {"name": column["name"]}
            rule = column.get("rule")
            if rule:
                entry["meta"] = {"dpf.rule": rule["rule"], "dpf.rule_version": rule["rule_version"]}
            column_tests = _tests_for(tests, model=entry_model.model, column=str(column["name"]))
            if column_tests:
                entry["data_tests"] = column_tests
            product_columns.append(entry)

        product_model: dict[str, Any] = {
            "name": entry_model.model,
            "description": (
                f"The relation product {product} publishes as {entry_model.relation}."
            ),
            "config": {
                "meta": {
                    "dpf.product": product,
                    "dpf.relation": entry_model.relation,
                    "dpf.profile": profile,
                    "dpf.shape": shape,
                    "dpf.publication_mode": publication_mode,
                }
            },
        }
        model_tests = _tests_for(tests, model=entry_model.model, column=None)
        if model_tests:
            product_model["data_tests"] = model_tests
        product_model["columns"] = product_columns
        models.append(product_model)

    for auxiliary in auxiliary_models:
        name = str(auxiliary["name"])
        entry = {"name": name, "description": auxiliary["description"]}
        auxiliary_tests = _tests_for(tests, model=name, column=None)
        if auxiliary_tests:
            entry["data_tests"] = auxiliary_tests
        auxiliary_columns: list[dict[str, Any]] = []
        for column_name in auxiliary.get("columns") or ():
            column_tests = _tests_for(tests, model=name, column=str(column_name))
            if column_tests:
                auxiliary_columns.append({"name": column_name, "data_tests": column_tests})
        if auxiliary_columns:
            entry["columns"] = auxiliary_columns
        models.append(entry)

    return {"version": 2, "models": models}


def dump_schema_document(document: Mapping[str, Any], *, generated_header: str) -> str:
    """``document`` as the YAML the route writes, under the frozen
    generated marker."""

    body = yaml.safe_dump(
        dict(document), sort_keys=False, default_flow_style=False, allow_unicode=True, width=100
    )
    return f"{generated_header}\n{body}"


__all__ = [
    "SEVERITY_ERROR",
    "SEVERITY_WARN",
    "GeneratedTest",
    "PublishedModel",
    "build_schema_document",
    "dump_schema_document",
]
