"""Python-level unit tests for ergasterion/emit.py's declaration validation gates.

The declaration coverage check must
catch a vault declaration whose bridge.select column list does not cover the entity's
required columns (payload + hashdiff columns) and fail loudly, naming every missing
column -- instead of emitting clean SQL that only dies inside the warehouse at dbt
build time.

No pytest dependency in this repo's .venv, so this follows the plain
argparse/assert-and-report convention already used by ergasterion/dialect_lint.py: each
test_* function raises AssertionError on failure, main() runs them all and reports
PASS/FAIL, exit code 0 = all green, 1 = any failure.

Usage:
    python tests/python/test_emit.py
"""

from __future__ import annotations

import contextlib
import io
import sys
import tempfile
import traceback
from pathlib import Path

import yaml

# Allow direct execution as `python tests/python/test_emit.py`.
if __package__ in (None, ""):
    import os as _os, sys as _sys
    _sys.path.insert(0, _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))

from ergasterion import dialect_lint, emit


def _write_declaration(dir_path: Path, filename: str, data: dict) -> Path:
    path = dir_path / filename
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return path


def _write_structure_minimum(estate: Path) -> None:
    """The structural minimum emit.main()'s post-emit gates need in a scratch
    estate: the per-target budget declarations (ergasterion/structure_gate.py is
    fail-closed) plus a dbt_project.yml materialising every layer as a table."""
    import shutil

    targets_dest = estate / "declarations" / "targets"
    targets_dest.mkdir(parents=True, exist_ok=True)
    for target_file in (emit.REPO_ROOT / "declarations" / "targets").glob("*.yml"):
        shutil.copy2(target_file, targets_dest / target_file.name)
    (estate / "dbt_project.yml").write_text(
        "name: fixture\nmodels:\n  fixture:\n    +materialized: table\n",
        encoding="utf-8",
    )


def _gp_select(names: list[str]) -> list[dict[str, str]]:
    return [{"name": name, "expression": f"source.{name}"} for name in names]


# gp is the entity with the smallest payload (9 columns) -- convenient for a
# deliberately-broken declaration built entirely in-memory/temp, never committed.
# ENTITY_CONFIGS lives in domains/investment.yml and is reached through the loader.
# The engine holds no domain vocabulary. Read inside a function so the module can
# import without an estate present.
def _gp_payload() -> list[str]:
    return list(emit.load_domains()["entity_configs"]["gp"]["payload"])


def _tmp_estate_ctx(declarations_dir: Path) -> "emit.EstateContext":
    """A context pinned to the committed estate root (so domains/ still resolve there) but
    with declarations/ pointed at a temporary directory."""
    return emit.EstateContext.resolve(estate_root=emit.REPO_ROOT, declarations_dir=declarations_dir)


def _minimal_declaration(select_names: list[str]) -> dict:
    return {
        "source": {"name": "test_src"},
        "tables": {
            "gps_table": {
                "vault_entities": [
                    {
                        "entity": "gp",
                        "bridge": {"select": _gp_select(select_names)},
                    }
                ]
            }
        },
    }


def test_missing_bridge_columns_raise_named_value_error() -> None:
    """A bridge select missing required columns must fail with every missing name."""
    incomplete_names = ["source_gp_id", "source_fund_id", "gp_name"]
    missing_expected = sorted(set(_gp_payload()) - set(incomplete_names))
    assert missing_expected, "test setup must actually omit columns"

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        _write_declaration(tmp_path, "broken.yml", _minimal_declaration(incomplete_names))

        ctx = _tmp_estate_ctx(tmp_path)
        try:
            emit.load_declarations(ctx=ctx)
        except ValueError as exc:
            message = str(exc)
            for column in missing_expected:
                assert column in message, (
                    f"expected missing column {column!r} named in error, got: {message}"
                )
            # Precedent style (emit.py): path:table_name:vault_name prefix.
            assert "broken.yml:gps_table:gp" in message, (
                f"expected path:table_name:vault_name prefix, got: {message}"
            )
        else:
            raise AssertionError("expected ValueError for incomplete bridge select, none raised")


def test_complete_bridge_columns_pass() -> None:
    """A bridge select that covers payload + hashdiff columns must load cleanly."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        _write_declaration(tmp_path, "complete.yml", _minimal_declaration(_gp_payload()))

        ctx = _tmp_estate_ctx(tmp_path)
        declarations = emit.load_declarations(ctx=ctx)
        assert len(declarations) == 1


def test_committed_declarations_still_load_clean() -> None:
    """Regression guard: every currently-committed declaration must still pass the
    new coverage gate (acceptance criterion 2 -- no behavioural change for valid
    declarations, only a new gate that today's clean declarations all clear)."""
    declarations = emit.load_declarations()
    assert declarations, "expected at least one committed declaration to load"


# ---------------------------------------------------------------------------
# Fixture domain: the domain-parameterisation proof.
#
# A minimal, entirely invented domain -- two toy entities (alpha, beta) and one link
# (alpha_beta) -- loaded and rendered through the SAME public code path as the
# investment domain (load_domains -> validate_bv -> load_declarations -> validate_er
# -> generate_files), with zero engine edits. If the engine had any
# hardcoded investment vocabulary, this toy domain could not render.
# ---------------------------------------------------------------------------

FIXTURE_DOMAIN = {
    "entity_configs": {
        "alpha": {
            "satellite_base": "alpha",
            "src_pk": "alpha_hk",
            "hashdiff": "alpha_hashdiff",
            "payload": ["source_id", "alpha_name", "alpha_code"],
            "hashed_columns": {
                "alpha_hk": "golden_alpha_key",
                "beta_hk": "golden_beta_key",
                "alpha_beta_lhk": ["golden_alpha_key", "golden_beta_key"],
                "alpha_hashdiff": {"is_hashdiff": True},
            },
            "links": ["alpha_beta"],
        },
        "beta": {
            "satellite_base": "beta",
            "src_pk": "beta_hk",
            "hashdiff": "beta_hashdiff",
            "payload": ["source_id", "beta_name"],
            "hashed_columns": {
                "beta_hk": "golden_beta_key",
                "beta_hashdiff": {"is_hashdiff": True},
            },
            "links": [],
        },
    },
    "hashdiff_exclude": {},
    "hub_configs": {
        "alpha": {
            "path": "models/raw_vault/hubs/hub_alpha.sql",
            "src_pk": "alpha_hk",
            "src_nk": "golden_alpha_key",
        },
        "beta": {
            "path": "models/raw_vault/hubs/hub_beta.sql",
            "src_pk": "beta_hk",
            "src_nk": "golden_beta_key",
        },
    },
    "link_configs": {
        "alpha_beta": {
            "path": "models/raw_vault/links/link_alpha_beta.sql",
            "src_pk": "alpha_beta_lhk",
            "src_fk": ["alpha_hk", "beta_hk"],
        },
    },
    "bv_configs": {
        "alpha": {
            "path": "models/business_vault/bv_alpha_golden_record.sql",
            "hub_model": "hub_alpha",
            "hub_pk": "alpha_hk",
            "hub_nk": "golden_alpha_key",
            "rules": {"alpha_name": "first_non_null", "alpha_code": "first_non_null"},
        },
        "beta": {
            "path": "models/business_vault/bv_beta_golden_record.sql",
            "hub_model": "hub_beta",
            "hub_pk": "beta_hk",
            "hub_nk": "golden_beta_key",
            "rules": {"beta_name": "first_non_null"},
        },
    },
    "res_configs": {
        "alpha": {
            "path": "models/entity_resolution/res_alpha.sql",
            "stable_key_entity": "alpha",
            "golden_key_column": "golden_alpha_key",
            "columns": ["source_system", "source_id", "alpha_name", "normalised_name", "alpha_code"],
            "match_keys": [
                {"column": "alpha_code", "expression": "concat('code:', alpha_code)", "type": "alpha_code"},
                {"column": "normalised_name", "expression": "concat('name:', normalised_name)", "type": "normalised_name"},
            ],
            "output_columns": [
                "source_system", "source_id", "alpha_name", "normalised_name",
                "deterministic_match_key", "exact_key_type",
            ],
            "trailing_columns": ["alpha_code", "match_row_count", "match_source_count"],
            # tier0 exercises the config-driven tier-0 branch on a NON-investment key.
            "tier0": {"key_type": "alpha_code", "tier_label": "tier_0_alpha_code"},
        },
        "beta": {
            "path": "models/entity_resolution/res_beta.sql",
            "stable_key_entity": "beta",
            "golden_key_column": "golden_beta_key",
            "columns": ["source_system", "source_id", "beta_name", "normalised_name"],
            "match_keys": [
                {"column": "normalised_name", "expression": "concat('name:', normalised_name)", "type": "normalised_name"},
            ],
            "output_columns": [
                "source_system", "source_id", "beta_name", "normalised_name",
                "deterministic_match_key", "exact_key_type",
            ],
            "trailing_columns": ["beta_name", "match_row_count", "match_source_count"],
        },
    },
}


def _fixture_declaration() -> dict:
    def _res_branch(model: str, columns: list[str]) -> dict:
        expressions = {name: f"source.{name}" for name in columns}
        entity_name = next(name for name in columns if name.endswith("_name") and name != "normalised_name")
        expressions["normalised_name"] = "{{ normalise_name('source." + entity_name + "') }}"
        return {"model": model, "columns": expressions}

    return {
        "source": {"name": "toysrc", "display_name": "TOYSRC", "priority": 50},
        "tables": {
            "things": {
                "raw_model": "raw_toysrc_things",
                "description": "Toy fixture source feeding both fixture entities and the link.",
                "seed_tests": [{"name": "id", "data_tests": ["unique", "not_null"]}],
                "projection": [
                    {"name": "source_system", "expression": "'toysrc'"},
                    {"name": "source_id", "expression": "cast(id as string)"},
                    {"name": "alpha_name", "expression": "cast(alpha_name as string)"},
                    {"name": "alpha_code", "expression": "cast(alpha_code as string)"},
                    {"name": "beta_name", "expression": "cast(beta_name as string)"},
                ],
                "vault_entities": [
                    {
                        "name": "alpha",
                        "entity": "alpha",
                        "bridge": {
                            "resolutions": [{
                                "alias": "alpha_res", "model": "res_alpha", "join_type": "inner",
                                "conditions": [
                                    "alpha_res.source_system = source.source_system",
                                    "alpha_res.source_id = source.source_id",
                                ],
                            }],
                            "select": [
                                {"name": "source_id", "expression": "source.source_id"},
                                {"name": "alpha_name", "expression": "source.alpha_name"},
                                {"name": "alpha_code", "expression": "source.alpha_code"},
                                {"name": "effective_from", "expression": "source.source_id"},
                                {"name": "golden_alpha_key", "expression": "alpha_res.golden_alpha_key"},
                                {"name": "golden_beta_key", "expression": "alpha_res.golden_alpha_key"},
                            ],
                            "where": ["alpha_res.golden_alpha_key is not null"],
                        },
                    },
                    {
                        "name": "beta",
                        "entity": "beta",
                        "bridge": {
                            "resolutions": [{
                                "alias": "beta_res", "model": "res_beta", "join_type": "inner",
                                "conditions": [
                                    "beta_res.source_system = source.source_system",
                                    "beta_res.source_id = source.source_id",
                                ],
                            }],
                            "select": [
                                {"name": "source_id", "expression": "source.source_id"},
                                {"name": "beta_name", "expression": "source.beta_name"},
                                {"name": "effective_from", "expression": "source.source_id"},
                                {"name": "golden_beta_key", "expression": "beta_res.golden_beta_key"},
                            ],
                            "where": ["beta_res.golden_beta_key is not null"],
                        },
                    },
                ],
            },
        },
        "entity_resolution": {
            "alpha": _res_branch(
                "stg_toysrc_things",
                ["source_system", "source_id", "alpha_name", "normalised_name", "alpha_code"],
            ),
            "beta": _res_branch(
                "stg_toysrc_things",
                ["source_system", "source_id", "beta_name", "normalised_name"],
            ),
        },
    }


def test_fixture_domain_loads_and_renders_through_same_code_path() -> None:
    """A wholly invented two-entity/one-link domain must load and render through the
    same public emit functions the investment domain uses, with zero engine edits --
    the mechanical proof that the loader is domain-parameterised, not hardcoded to
    investment."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        domains_dir = tmp_path / "domains"
        domains_dir.mkdir()
        (domains_dir / "fixture.yml").write_text(
            yaml.safe_dump(FIXTURE_DOMAIN, sort_keys=False), encoding="utf-8"
        )
        decls_dir = tmp_path / "declarations"
        decls_dir.mkdir()
        _write_declaration(decls_dir, "toysrc.yml", _fixture_declaration())

        # SAME code path as main(): load the domain, run the gates, generate.
        domain = emit.load_domains(domains_dir)
        assert set(domain["entity_configs"]) == {"alpha", "beta"}, "fixture entities must load"
        # hashdiff derivation ran through the loader for the toy domain too.
        alpha = domain["entity_configs"]["alpha"]
        assert alpha["hashed_columns"]["alpha_hashdiff"]["columns"] == alpha["payload"]

        emit.validate_bv_rules_subset_of_payload(domain["bv_configs"], domain["entity_configs"])

        # Context construction, not global monkeypatching: a context whose domains/ and
        # declarations/ both point at the fixture temp dirs, threaded through the same public
        # code path main() uses.
        ctx = emit.EstateContext.resolve(
            estate_root=tmp_path, domains_dir=domains_dir, declarations_dir=decls_dir
        )
        declarations = emit.load_declarations(domain, ctx=ctx)
        emit.validate_unique_source_priorities(declarations)
        emit.validate_er_branch_coverage(declarations, domain["res_configs"], ctx=ctx)
        env = emit.template_env()
        files = emit.generate_files(declarations, env, domain, ctx=ctx)

        rendered = {f.path.name: f.content for f in files}
        # Every layer emitted for the toy domain, keyed only by fixture vocabulary.
        assert "hub_alpha.sql" in rendered and "hub_beta.sql" in rendered, "toy hubs must render"
        assert "link_alpha_beta.sql" in rendered, "the one toy link must render"
        assert "res_alpha.sql" in rendered and "res_beta.sql" in rendered, "toy res models must render"
        assert "bv_alpha_golden_record.sql" in rendered, "toy golden record must render"
        assert "stg_toysrc_things.sql" in rendered, "toy staging must render"
        # The config-driven tier-0 block fired on the toy key (proves it is data, not
        # an investment literal baked into the template).
        assert "tier_0_alpha_code" in rendered["res_alpha.sql"], "tier0 must be config-driven"
        assert "golden_alpha_key" in rendered["res_alpha.sql"]
        assert "from {{ ref('stg_toysrc_things') }} as source" in rendered["res_alpha.sql"], (
            "resolution branches must declare the source alias used by qualified column expressions"
        )
        # No investment vocabulary leaked into the toy output.
        assert "fund" not in rendered["res_alpha.sql"], "no investment token in a foreign domain"


def test_source_landing_renders_source_node_tests_and_staging_source_call() -> None:
    """A source-backed table renders its tests on the dbt source node and reads
    that relation with source(), without retaining a nonexistent seed node."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        domains_dir = tmp_path / "domains"
        domains_dir.mkdir()
        (domains_dir / "fixture.yml").write_text(
            yaml.safe_dump(FIXTURE_DOMAIN, sort_keys=False), encoding="utf-8"
        )
        declarations_dir = tmp_path / "declarations"
        declarations_dir.mkdir()
        declaration = _fixture_declaration()
        table = declaration["tables"]["things"]
        table.pop("raw_model")
        table["landing"] = {
            "kind": "source",
            "source_name": "warehouse_feed",
            "identifier": "things_live",
        }
        _write_declaration(declarations_dir, "toysrc.yml", declaration)
        ctx = emit.EstateContext.resolve(
            estate_root=tmp_path,
            domains_dir=domains_dir,
            declarations_dir=declarations_dir,
        )

        domain = emit.load_domains(ctx=ctx)
        declarations = emit.load_declarations(domain, ctx=ctx)
        files = emit.generate_files(
            declarations, emit.template_env(), domain, ctx=ctx
        )
        rendered = {file.path.name: file.content for file in files}
        sources_yml = rendered["_sources.yml"]
        staging_sql = rendered["stg_toysrc_things.sql"]

        expected_prefix = f"{emit.YAML_HEADER}\nversion: 2\n\nsources:\n"
        assert sources_yml.startswith(expected_prefix), (
            f"all-source output must have exactly one blank line before sources: {sources_yml!r}"
        )
        parsed = yaml.safe_load(sources_yml)
        assert "seeds" not in parsed, (
            f"an all-source estate must omit the seeds key: {sources_yml}"
        )
        source = parsed["sources"][0]
        assert source["name"] == "warehouse_feed", f"expected source name, got: {source}"
        source_table = source["tables"][0]
        assert source_table["name"] == "things_live", f"expected source identifier, got: {source_table}"
        assert source_table["columns"] == [
            {"name": "id", "data_tests": ["unique", "not_null"]}
        ], f"expected seed_tests moved to source columns, got: {source_table}"
        assert "{{ source('warehouse_feed', 'things_live') }}" in staging_sql
        assert "ref(" not in staging_sql, f"source landing must not render ref(): {staging_sql}"


def test_mixed_seed_and_source_landings_render_separate_tested_nodes() -> None:
    """A migration estate keeps seed and source nodes, with one blank line between
    top-level blocks and each table's tests attached to its actual landing node."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        domains_dir = tmp_path / "domains"
        domains_dir.mkdir()
        (domains_dir / "fixture.yml").write_text(
            yaml.safe_dump(FIXTURE_DOMAIN, sort_keys=False), encoding="utf-8"
        )
        declarations_dir = tmp_path / "declarations"
        declarations_dir.mkdir()
        declaration = _fixture_declaration()
        declaration["tables"]["live_things"] = {
            "staging_model": "stg_toysrc_live_things",
            "description": "Live fixture relation.",
            "landing": {
                "kind": "source",
                "source_name": "warehouse_feed",
                "identifier": "things_live",
            },
            "seed_tests": [{"name": "id", "data_tests": ["not_null"]}],
            "projection": [{"name": "id", "expression": "cast(id as string)"}],
        }
        _write_declaration(declarations_dir, "toysrc.yml", declaration)
        ctx = emit.EstateContext.resolve(
            estate_root=tmp_path,
            domains_dir=domains_dir,
            declarations_dir=declarations_dir,
        )

        domain = emit.load_domains(ctx=ctx)
        declarations = emit.load_declarations(domain, ctx=ctx)
        rendered = {
            file.path.name: file.content
            for file in emit.generate_files(
                declarations, emit.template_env(), domain, ctx=ctx
            )
        }
        sources_yml = rendered["_sources.yml"]
        assert "\n\nsources:\n" in sources_yml, sources_yml
        parsed = yaml.safe_load(sources_yml)
        assert parsed["seeds"][0]["name"] == "raw_toysrc_things", parsed
        assert parsed["seeds"][0]["columns"] == [
            {"name": "id", "data_tests": ["unique", "not_null"]}
        ], parsed
        source_table = parsed["sources"][0]["tables"][0]
        assert source_table["name"] == "things_live", parsed
        assert source_table["columns"] == [
            {"name": "id", "data_tests": ["not_null"]}
        ], parsed
        assert "{{ ref('raw_toysrc_things') }}" in rendered["stg_toysrc_things.sql"]
        assert (
            "{{ source('warehouse_feed', 'things_live') }}"
            in rendered["stg_toysrc_live_things.sql"]
        )


def test_landing_rejects_ignored_fields_and_source_raw_model() -> None:
    """Landing declarations fail loudly instead of accepting fields that dbt ignores."""
    invalid_landings = (
        ({"kind": "seed", "source_name": "misplaced"}, "source_name"),
        (
            {
                "kind": "source",
                "source_name": "warehouse_feed",
                "identifier": "things_live",
                "identifer": "misspelled",
            },
            "identifer",
        ),
    )
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        for index, (landing, expected) in enumerate(invalid_landings):
            declaration = _fixture_declaration()
            declaration["tables"]["things"]["landing"] = landing
            declaration_path = _write_declaration(
                tmp_path, f"invalid_{index}.yml", declaration
            )
            try:
                emit.load_declarations(ctx=_tmp_estate_ctx(tmp_path))
            except ValueError as exc:
                assert expected in str(exc), str(exc)
            else:
                raise AssertionError(f"landing field {expected!r} must fail")
            declaration_path.unlink()

        declaration = _fixture_declaration()
        declaration["tables"]["things"]["landing"] = {
            "kind": "source",
            "source_name": "warehouse_feed",
            "identifier": "things_live",
        }
        _write_declaration(tmp_path, "orphan_seed.yml", declaration)
        try:
            emit.load_declarations(ctx=_tmp_estate_ctx(tmp_path))
        except ValueError as exc:
            message = str(exc)
            assert "raw_model" in message and "obsolete" in message, message
        else:
            raise AssertionError("source landing with raw_model must fail")


def test_duplicate_source_landing_fails_loudly_with_both_declarations() -> None:
    """Two declarations cannot silently compete for one dbt source node."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        domains_dir = tmp_path / "domains"
        domains_dir.mkdir()
        (domains_dir / "fixture.yml").write_text(
            yaml.safe_dump(FIXTURE_DOMAIN, sort_keys=False), encoding="utf-8"
        )
        declarations_dir = tmp_path / "declarations"
        declarations_dir.mkdir()
        for filename, source_name in (("first.yml", "first"), ("second.yml", "second")):
            declaration = _fixture_declaration()
            declaration["source"]["name"] = source_name
            declaration["source"]["display_name"] = source_name.upper()
            declaration["source"]["priority"] = 10 if source_name == "first" else 20
            table = declaration["tables"]["things"]
            table["staging_model"] = f"stg_{source_name}_things"
            table.pop("raw_model")
            table["landing"] = {
                "kind": "source",
                "source_name": "warehouse_feed",
                "identifier": "things_live",
            }
            _write_declaration(declarations_dir, filename, declaration)
        ctx = emit.EstateContext.resolve(
            estate_root=tmp_path,
            domains_dir=domains_dir,
            declarations_dir=declarations_dir,
        )
        domain = emit.load_domains(ctx=ctx)
        declarations = emit.load_declarations(domain, ctx=ctx)

        try:
            emit.generate_files(declarations, emit.template_env(), domain, ctx=ctx)
        except ValueError as exc:
            message = str(exc)
            assert "first.yml:tables.things" in message, message
            assert "second.yml:tables.things" in message, message
            assert "source('warehouse_feed', 'things_live')" in message, message
        else:
            raise AssertionError("duplicate source relation must fail generation")


# --- --strict-openim + summary warnings count --------------------------------------

def _write_openim_model(dir_path: Path, rel_path: str, columns: list[str]) -> None:
    """A minimal OpenIM-shaped model markdown file with one '## Attribute schema'
    table, just enough for parse_model_attribute_schema to bind `columns`."""
    model_path = dir_path / rel_path
    model_path.parent.mkdir(parents=True, exist_ok=True)
    rows = "\n".join(f"| `{name}` | `string` |" for name in columns)
    model_path.write_text(
        f"## Attribute schema\n\n| Column | Type |\n|---|---|\n{rows}\n",
        encoding="utf-8",
    )


def test_canonical_mapping_missing_openim_root_warns_and_counts_one() -> None:
    """No --openim-root given: warn, skip validation, and return
    (the new warnings-count contract) is 1 so main() can surface it."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        count = emit.validate_canonical_mappings([], None)
    assert count == 1, f"expected 1 warning for a missing --openim-root, got {count}"
    assert "WARN canonical mapping validation skipped: no --openim-root given" in buf.getvalue()


def test_canonical_mapping_nonexistent_openim_root_warns_and_counts_one() -> None:
    """--openim-root given but not a real checkout: same WARN-skip, count 1."""
    with tempfile.TemporaryDirectory() as tmp:
        missing_root = Path(tmp) / "not-a-real-checkout"
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            count = emit.validate_canonical_mappings([], missing_root)
        assert count == 1, f"expected 1 warning for a non-resolving --openim-root, got {count}"
        assert "OpenIM repo not found" in buf.getvalue()


def test_canonical_mapping_omitted_business_keys_uses_pre_f6_default() -> None:
    """Convergence bar (a), omission 1 of 2: a canonical_mappings entry that omits the
    optional business_keys section must fall back to the default (nothing to
    check for that section) rather than raising KeyError."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        _write_openim_model(tmp_path, "model.md", ["gp_name_field"])
        declarations = [
            {
                "source": {"name": "test_src"},
                "canonical_mappings": {
                    "gp": {
                        "model_file": "model.md",
                        "attributes": {"gp_name": "gp_name_field"},
                        # business_keys intentionally omitted.
                    }
                },
            }
        ]
        count = emit.validate_canonical_mappings(declarations, tmp_path)
        assert count == 0, f"expected a clean run (no skip) once a real root is given, got {count}"


def test_canonical_mapping_omitted_attributes_uses_pre_f6_default() -> None:
    """Convergence bar (a), omission 2 of 2: a canonical_mappings entry that omits the
    optional attributes section must fall back to the default, not raise KeyError."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        _write_openim_model(tmp_path, "model.md", ["gp_name_field"])
        declarations = [
            {
                "source": {"name": "test_src"},
                "canonical_mappings": {
                    "gp": {
                        "model_file": "model.md",
                        "business_keys": {"gp_id": "gp_name_field"},
                        # attributes intentionally omitted.
                    }
                },
            }
        ]
        count = emit.validate_canonical_mappings(declarations, tmp_path)
        assert count == 0, f"expected a clean run (no skip) once a real root is given, got {count}"


def _run_main_capturing(argv: list[str]) -> tuple[int, str, str]:
    old_argv = sys.argv
    out, err = io.StringIO(), io.StringIO()
    try:
        sys.argv = ["emit.py"] + argv
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            exit_code = emit.main()
    finally:
        sys.argv = old_argv
    return exit_code, out.getvalue(), err.getvalue()


def test_default_check_run_summary_carries_warnings_count() -> None:
    """Acceptance: default (non-strict) run against the committed estate stays exit-code
    and byte-stable (a --check run against already-generated files reports
    0 changed, exit 0); the ONLY default-mode delta is the summary line gaining the
    warnings count, because the committed declarations give no --openim-root."""
    exit_code, out, _err = _run_main_capturing(["--check"])
    assert exit_code == 0, f"expected unchanged clean --check exit, got {exit_code}: {out}"
    assert "WARN canonical mapping validation skipped: no --openim-root given" in out
    summary_lines = [line for line in out.splitlines() if line.startswith(("generated ", "would change "))]
    assert summary_lines, f"expected a generated/would-change summary line, got: {out}"
    assert summary_lines[0].endswith("(1 warning)"), (
        f"expected the summary line to carry the warnings count, got: {summary_lines[0]!r}"
    )


def test_bigquery_deny_list_is_non_empty() -> None:
    assert dialect_lint.DENY_LISTS["bigquery"], "BigQuery must have portability rules"


def test_bigquery_lint_rejects_snowflake_construct_in_hand_authored_model() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        model = root / "models" / "marts" / "hand_authored.sql"
        model.parent.mkdir(parents=True)
        model.write_text("select to_varchar(created_at) from source_rows\n", encoding="utf-8")
        ctx = emit.EstateContext.resolve(estate_root=root)
        offenses = dialect_lint.lint_models("bigquery", ctx=ctx)
        assert any(offense.token == "to_varchar" and offense.path == model for offense in offenses), (
            f"expected hand-authored Snowflake syntax to fail BigQuery lint, got {offenses}"
        )


def test_dialect_gate_rejects_any_registered_adapter_with_empty_rules() -> None:
    original = dialect_lint.DENY_LISTS["bigquery"]
    dialect_lint.DENY_LISTS["bigquery"] = []
    try:
        try:
            dialect_lint.lint_models("snowflake")
        except ValueError as error:
            message = str(error)
            assert "bigquery" in message and "empty deny-list" in message, (
                f"expected the empty adapter and failure reason to be named, got: {error}"
            )
        else:
            raise AssertionError("expected an empty registered deny-list to fail the gate")
    finally:
        dialect_lint.DENY_LISTS["bigquery"] = original


def test_emit_lints_every_declared_deployment_adapter() -> None:
    seen: list[str] = []
    original = emit.lint_models

    def recording_lint(target: str, *, ctx: emit.EstateContext | None = None) -> list:
        seen.append(target)
        return []

    emit.lint_models = recording_lint
    try:
        exit_code, out, _err = _run_main_capturing(["--check"])
    finally:
        emit.lint_models = original

    assert exit_code == 0, f"expected clean emit while recording lint targets, got {exit_code}: {out}"
    assert seen == ["bigquery", "snowflake"], f"expected both declared adapters once, got {seen}"
    assert "dialect-lint OK [bigquery]" in out and "dialect-lint OK [snowflake]" in out


def test_strict_openim_without_root_exits_non_zero() -> None:
    """Acceptance: strict run without --openim-root exits non-zero, WARN still printed."""
    exit_code, out, err = _run_main_capturing(["--check", "--strict-openim"])
    assert exit_code == 1, f"expected non-zero exit under --strict-openim, got {exit_code}"
    assert "WARN canonical mapping validation skipped: no --openim-root given" in out
    assert "FAIL --strict-openim" in err, f"expected the strict failure reason on stderr, got: {err!r}"


# --- declaration input hardening (identifier / path / res-config gates) -----------

def test_jinja_injection_in_raw_model_rejected() -> None:
    """A declared name that breaks out of the emitted ref() literal and injects
    live template text must fail at declaration load, naming the field."""
    hostile = _fixture_declaration()
    hostile["tables"]["things"]["raw_model"] = "raw_x') }} {{ env_var('SECRET"
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        _write_declaration(tmp_path, "hostile.yml", hostile)
        try:
            emit.load_declarations(FIXTURE_DOMAIN, ctx=_tmp_estate_ctx(tmp_path))
        except ValueError as exc:
            assert "raw_model" in str(exc), f"expected the field named, got: {exc}"
            assert "identifier" in str(exc), f"expected the identifier rule named, got: {exc}"
        else:
            raise AssertionError("expected ValueError for a Jinja-injecting raw_model")


def test_display_name_with_space_rejected() -> None:
    """display_name reaches CTE-name and join-alias positions, so a natural label
    with a space must fail at load rather than as a first-build syntax error."""
    labelled = _fixture_declaration()
    labelled["source"]["display_name"] = "Fund Admin Feed"
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        _write_declaration(tmp_path, "labelled.yml", labelled)
        try:
            emit.load_declarations(FIXTURE_DOMAIN, ctx=_tmp_estate_ctx(tmp_path))
        except ValueError as exc:
            assert "display_name" in str(exc), f"expected the field named, got: {exc}"
        else:
            raise AssertionError("expected ValueError for a display_name with a space")


def test_domain_path_outside_models_rejected() -> None:
    """A declared model path with a '..' segment, or outside models/, must fail:
    outside the layer roots the project config's materialisation no longer
    governs the generated file."""
    import copy

    for bad_path, expected in [
        ("models/../secrets/hub_alpha.sql", ".."),
        ("elsewhere/hub_alpha.sql", "models/"),
        ("/abs/models/hub_alpha.sql", "estate-relative"),
    ]:
        domain = copy.deepcopy(FIXTURE_DOMAIN)
        domain["hub_configs"]["alpha"]["path"] = bad_path
        try:
            emit.validate_domain_paths(domain)
        except ValueError as exc:
            assert expected in str(exc), f"expected {expected!r} named for {bad_path!r}, got: {exc}"
        else:
            raise AssertionError(f"expected ValueError for path {bad_path!r}")


def test_committed_domain_paths_pass() -> None:
    emit.validate_domain_paths(emit.load_domains())


def test_res_config_bad_iterations_rejected() -> None:
    """transitive_merge with zero, negative, or non-integer merge_iterations
    renders broken or silently degraded SQL, so each must fail before render."""
    for bad in (0, -3, "5", None, True):
        try:
            emit.validate_res_configs(
                {"thing": {"transitive_merge": True, "merge_iterations": bad}}
            )
        except ValueError as exc:
            assert "merge_iterations" in str(exc), f"expected the field named, got: {exc}"
        else:
            raise AssertionError(f"expected ValueError for merge_iterations={bad!r}")


def test_res_config_dangling_convergence_test_rejected() -> None:
    try:
        emit.validate_res_configs(
            {"thing": {"convergence_test": "tests/assert_no_such_file.sql"}}
        )
    except ValueError as exc:
        assert "convergence_test" in str(exc), f"expected the field named, got: {exc}"
    else:
        raise AssertionError("expected ValueError for a dangling convergence_test")


def test_committed_res_configs_pass() -> None:
    emit.validate_res_configs(emit.load_domains()["res_configs"])


def test_shared_source_priority_for_same_entity_rejected() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        domains_dir = tmp_path / "domains"
        domains_dir.mkdir()
        (domains_dir / "fixture.yml").write_text(
            yaml.safe_dump(FIXTURE_DOMAIN, sort_keys=False), encoding="utf-8"
        )
        declarations_dir = tmp_path / "declarations"
        declarations_dir.mkdir()

        amber = _fixture_declaration()
        amber["source"]["name"] = "amber"
        azure = _fixture_declaration()
        azure["source"]["name"] = "azure"
        _write_declaration(declarations_dir, "amber.yml", amber)
        _write_declaration(declarations_dir, "azure.yml", azure)

        ctx = emit.EstateContext.resolve(
            estate_root=tmp_path,
            domains_dir=domains_dir,
            declarations_dir=declarations_dir,
        )
        declarations = emit.load_declarations(emit.load_domains(ctx=ctx), ctx=ctx)
        try:
            emit.validate_unique_source_priorities(declarations)
        except ValueError as exc:
            message = str(exc)
            assert "alpha" in message, f"expected the tied entity named, got: {exc}"
            assert "amber, azure" in message, f"expected both tied sources named, got: {exc}"
            assert "priority 50" in message, f"expected the tied priority named, got: {exc}"
        else:
            raise AssertionError("expected a same-entity source-priority tie to fail")


def test_committed_source_priorities_pass() -> None:
    emit.validate_unique_source_priorities(emit.load_declarations())


def test_write_files_refuses_escape_from_root() -> None:
    """The pre-write backstop: a generated path resolving outside the estate root
    is refused before any byte lands."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        escapee = emit.GeneratedFile(tmp_path.parent / "escaped.sql", "select 1\n")
        try:
            emit.write_files([escapee], root=tmp_path)
        except ValueError as exc:
            assert "outside the estate root" in str(exc), f"expected the refusal named, got: {exc}"
        else:
            raise AssertionError("expected ValueError for a path outside the estate root")
        assert not (tmp_path.parent / "escaped.sql").exists(), "nothing may be written"


# --- orphan-signal binding regression --------------------------------------------

def test_removed_declaration_turns_the_orphan_signal_on() -> None:
    """A scratch estate emits the full fixture declaration (write mode), then the
    SAME declaration with one vault_entity removed is emitted again in --check mode.
    The removed entity's generated files (bridge/stage/satellite) become real
    orphans on disk -- not a synthetic message, an actual on-disk mismatch between
    current declarations and previously-generated output.

    This binds emit.py's --check exit code AND its ORPHANS=<n> marker line to a real
    orphan, through the exact public entry point (main(), CLI argv) a validator
    invokes. An emit.py edit that stops incrementing ORPHANS, or that reintroduces
    the fail-open (--check exiting 0 with orphans pending), fails this test -- the
    emitter's signal and any validator reading it can no longer drift apart silently
    because both are pinned to the one behaviour this test exercises.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        domains_dir = tmp_path / "domains"
        domains_dir.mkdir()
        (domains_dir / "fixture.yml").write_text(
            yaml.safe_dump(FIXTURE_DOMAIN, sort_keys=False), encoding="utf-8"
        )
        decls_dir = tmp_path / "declarations"
        decls_dir.mkdir()
        _write_declaration(decls_dir, "toysrc.yml", _fixture_declaration())
        _write_structure_minimum(tmp_path)

        # Full generate, write mode: the on-disk tree a real estate would have before
        # any declaration is ever removed.
        exit_code, out, _err = _run_main_capturing(["--estate-root", str(tmp_path)])
        assert exit_code == 0, f"expected a clean first generate, got {exit_code}: {out}"
        orphan_lines = [line for line in out.splitlines() if line.startswith("ORPHANS=")]
        assert orphan_lines and orphan_lines[-1] == "ORPHANS=0", (
            f"expected a clean first generate to report zero orphans, got: {out}"
        )

        # Remove the "beta" vault_entity: a real declaration edit. The bridge-coverage
        # gate only soft-warns for the now-unfed entity_resolution.beta branch (see
        # validate_er_branch_coverage), so this stays otherwise clean -- the only
        # effect is that beta's bridge/stage/satellite models are no longer declared.
        trimmed = _fixture_declaration()
        trimmed["tables"]["things"]["vault_entities"] = [
            v for v in trimmed["tables"]["things"]["vault_entities"] if v["name"] != "beta"
        ]
        _write_declaration(decls_dir, "toysrc.yml", trimmed)

        exit_code, out, _err = _run_main_capturing(["--check", "--estate-root", str(tmp_path)])

        orphan_lines = [line for line in out.splitlines() if line.startswith("ORPHANS=")]
        assert orphan_lines, f"expected an ORPHANS=<n> marker line, got: {out}"
        orphan_count = int(orphan_lines[-1].split("=", 1)[1])
        assert orphan_count > 0, f"expected a positive orphan count, got: {out}"
        assert exit_code == 1, (
            f"expected --check to exit non-zero on a real pending orphan "
            f"(the fail-open regression), got {exit_code}: {out}"
        )


TESTS = [
    test_missing_bridge_columns_raise_named_value_error,
    test_complete_bridge_columns_pass,
    test_committed_declarations_still_load_clean,
    test_fixture_domain_loads_and_renders_through_same_code_path,
    test_source_landing_renders_source_node_tests_and_staging_source_call,
    test_mixed_seed_and_source_landings_render_separate_tested_nodes,
    test_landing_rejects_ignored_fields_and_source_raw_model,
    test_duplicate_source_landing_fails_loudly_with_both_declarations,
    test_canonical_mapping_missing_openim_root_warns_and_counts_one,
    test_canonical_mapping_nonexistent_openim_root_warns_and_counts_one,
    test_canonical_mapping_omitted_business_keys_uses_pre_f6_default,
    test_canonical_mapping_omitted_attributes_uses_pre_f6_default,
    test_default_check_run_summary_carries_warnings_count,
    test_bigquery_deny_list_is_non_empty,
    test_bigquery_lint_rejects_snowflake_construct_in_hand_authored_model,
    test_dialect_gate_rejects_any_registered_adapter_with_empty_rules,
    test_emit_lints_every_declared_deployment_adapter,
    test_strict_openim_without_root_exits_non_zero,
    test_removed_declaration_turns_the_orphan_signal_on,
    test_jinja_injection_in_raw_model_rejected,
    test_display_name_with_space_rejected,
    test_domain_path_outside_models_rejected,
    test_committed_domain_paths_pass,
    test_res_config_bad_iterations_rejected,
    test_res_config_dangling_convergence_test_rejected,
    test_committed_res_configs_pass,
    test_shared_source_priority_for_same_entity_rejected,
    test_committed_source_priorities_pass,
    test_write_files_refuses_escape_from_root,
]


def main() -> int:
    failures = 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
        except Exception:  # noqa: BLE001 - report and continue, exit code carries the signal
            failures += 1
            print(f"FAIL {name}")
            traceback.print_exc()
        else:
            print(f"PASS {name}")
    total = len(TESTS)
    print(f"{total - failures}/{total} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
