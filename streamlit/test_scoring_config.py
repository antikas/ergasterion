"""Offline fixture tests for streamlit_app.py's pure seed-config logic."""

import ast
from pathlib import Path


FUNCTIONS = {
    "quote_snowflake_identifier", "relation_fqn", "schema_names",
    "scoring_config_from_rows", "weight_metric_label", "select_scoring_config",
}
FALLBACK = {
    "weights": {"string": 0.4, "sector": 0.2, "date": 0.2, "value": 0.2},
    "threshold_auto": 0.85,
    "threshold_review": 0.65,
}


def load_functions():
    app_path = Path(__file__).with_name("streamlit_app.py")
    module = ast.parse(app_path.read_text(encoding="utf-8"))
    nodes = [
        node for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS
    ]
    namespace = {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(app_path), "exec"), namespace)
    return namespace


def row(entity_type, string=0.4, sector=0.2, date=0.2, value=0.2, auto=0.85, review=0.65):
    return {
        "ENTITY_TYPE": entity_type,
        "WEIGHT_STRING": string,
        "WEIGHT_SECTOR": sector,
        "WEIGHT_DATE": date,
        "WEIGHT_VALUE": value,
        "THRESHOLD_AUTO": auto,
        "THRESHOLD_REVIEW": review,
    }


class SnowparkRowLike:
    def __init__(self, values):
        self.values = values

    def as_dict(self):
        return self.values


def assert_invalid(transform, rows, expected):
    try:
        transform(rows)
    except ValueError as exc:
        assert expected in str(exc)
    else:
        raise AssertionError("expected invalid scoring config")


def assert_value_error(callback, expected):
    try:
        callback()
    except ValueError as exc:
        assert expected in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_schema_derivation_and_uniform_rows():
    funcs = load_functions()
    assert funcs["schema_names"](" DEV ")["raw"] == "DEV_RAW"
    assert funcs["schema_names"]("dev")["raw"] == "DEV_RAW"
    assert funcs["quote_snowflake_identifier"]("dev") == '"DEV"'
    assert funcs["quote_snowflake_identifier"]('"MixedCase"') == '"MixedCase"'
    assert funcs["quote_snowflake_identifier"]('"My ""DB"""') == '"My ""DB"""'
    assert funcs["relation_fqn"]('"MixedCase"', "dev_raw", "entity_resolution_scoring_config") == (
        '"MixedCase"."DEV_RAW"."ENTITY_RESOLUTION_SCORING_CONFIG"'
    )
    assert_value_error(lambda: funcs["schema_names"](""), "non-empty")
    assert_value_error(lambda: funcs["schema_names"]("dev;drop"), "unsafe")
    assert_value_error(lambda: funcs["quote_snowflake_identifier"]('"unterminated'), "unmatched")
    config = funcs["scoring_config_from_rows"]([row("fund"), row("gp")])
    assert config["is_uniform"] is True
    assert config["weights"] == FALLBACK["weights"]
    assert config["threshold_auto"] == 0.85
    assert list(config["by_entity_type"]) == ["fund", "gp"]


def test_row_casing_snowpark_rows_and_strict_validation():
    transform = load_functions()["scoring_config_from_rows"]
    lower_case = {key.lower(): value for key, value in row("fund").items()}
    config = transform([SnowparkRowLike(lower_case)])
    assert config["is_uniform"] is True
    assert_invalid(transform, [row("Fund"), row("fund")], "duplicate entity_type")
    missing = row("fund")
    del missing["WEIGHT_VALUE"]
    assert_invalid(transform, [missing], "seed metrics")
    assert_invalid(transform, [row("fund", string=None)], "invalid weight_string")
    assert_invalid(transform, [row("fund", string="not-a-number")], "non-numeric weight_string")
    unexpected = row("fund")
    unexpected["WEIGHT_CITY"] = 0.1
    assert_invalid(transform, [unexpected], "seed metrics")
    collision = row("fund")
    collision["weight_string"] = 0.4
    assert_invalid(transform, [collision], "normalized metric-key collision")


def test_weight_bounds_totals_and_tolerance():
    transform = load_functions()["scoring_config_from_rows"]
    assert_invalid(transform, [row("fund", string=1.1)], "outside [0, 1]")
    assert_invalid(transform, [row("fund", string=-0.1)], "outside [0, 1]")
    assert_invalid(transform, [row("fund", string=0.3, sector=0.3, date=0.3, value=0.3)], "sum to 1")
    precise = transform([row("fund", string=0.1, sector=0.2, date=0.3, value=0.4000000005)])
    assert precise["weights"]["value"] == 0.4000000005


def test_nonuniform_weights_are_deterministic_but_thresholds_must_match():
    funcs = load_functions()
    transform = funcs["scoring_config_from_rows"]
    config = transform([
        row("fund", string=0.5, sector=0.1),
        row("customer", string=0.3, sector=0.3),
    ])
    assert config["is_uniform"] is False
    assert list(config["by_entity_type"]) == ["customer", "fund"]
    funcs["SCORING_CONFIG"] = config
    funcs["WEIGHTS"] = config["weights"]
    assert funcs["weight_metric_label"]("string") == "String (customer: 0.3, fund: 0.5)"
    assert_invalid(transform, [row("fund"), row("gp", auto=0.9)], "thresholds must be uniform")


def test_lookup_unavailable_and_invalid_data_are_distinct():
    select = load_functions()["select_scoring_config"]

    def unavailable():
        raise RuntimeError("table unavailable")

    config, status, detail = select(unavailable, FALLBACK)
    assert status == "unavailable"
    assert "table unavailable" in detail
    assert config["weights"] == FALLBACK["weights"]

    config, status, detail = select(lambda: [row("fund", review=0.9, auto=0.85)], FALLBACK)
    assert status == "invalid"
    assert "invalid review/auto thresholds" in detail
    assert config["threshold_auto"] == FALLBACK["threshold_auto"]


if __name__ == "__main__":
    test_schema_derivation_and_uniform_rows()
    test_row_casing_snowpark_rows_and_strict_validation()
    test_weight_bounds_totals_and_tolerance()
    test_nonuniform_weights_are_deterministic_but_thresholds_must_match()
    test_lookup_unavailable_and_invalid_data_are_distinct()
    print("streamlit scoring-config tests passed")
