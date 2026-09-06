{#-
  The three macros a generated vault store places (the data_vault shape,
  ergasterion/shapes/data_vault, rendered by
  ergasterion/translators/dbt_patterns/shape_relations.py).

  Everything else a vault relation is made of is generated SQL. These three
  are not, because each needs something only the running build knows:

    * dpf_vault_hash needs the adapter's own hash construction and its own
      string type, so it dispatches through the two macros that already own
      them (dpf_hash_hex and dpf_type in macros/cross_db.sql) rather than
      carrying a second copy of either;
    * dpf_vault_unstored_key and dpf_vault_new_versions need to know
      whether this run is the first one and what the store already holds.
      A generated model cannot decide either: is_incremental() and {{ this }}
      are facts about the run, not about the declaration.

  Both suppression macros render `true` on the run that creates the store,
  so the first build stores everything the composition derives and every
  later build adds only what is not already there.
-#}


{#- One identity key or change fingerprint over `columns`, in the order the
    caller gives them. Each value is cast to text and a null takes a
    sentinel, so a null and the empty string never hash alike and
    concat_ws never silently drops a column. The order is the hash input:
    two relations naming the same columns in the same order always produce
    the same key, which is what lets a link and its hubs agree without
    reading each other. -#}
{% macro dpf_vault_hash(columns) -%}
    {%- set parts = [] -%}
    {%- for column in columns -%}
        {%- do parts.append("coalesce(cast(" ~ column ~ " as " ~ dpf_type('string') ~ "), '^^')") -%}
    {%- endfor -%}
    {{ return('upper(' ~ dpf_hash_hex("concat_ws('||', " ~ parts | join(', ') ~ ")") ~ ')') }}
{%- endmacro %}


{#- The predicate a hub or a link keeps a candidate row by: the store does
    not already carry this identity key. An identity is stored once and
    keeps the instant it was first seen at, whatever later runs deliver. -#}
{% macro dpf_vault_unstored_key(alias, key_column) -%}
{%- if not is_incremental() -%}
true
{%- else -%}
not exists (
        select 1
        from {{ this }} as dpf_stored
        where dpf_stored.{{ key_column }} = {{ alias }}.{{ key_column }}
    )
{%- endif -%}
{%- endmacro %}


{#- The predicate a satellite keeps a candidate version by.

    Two clauses. The first is the satellite's declared kind:

      * `current` keeps a payload whose fingerprint differs from the
        version in force for that key under the same basis version, which
        is what an ordinary satellite does -- it records changes. A key with
        no version stored under this basis has none in force, so the
        comparison is against nothing and the candidate is kept: that is
        the run after a re-baseline, and it lands exactly one version per
        key under the new basis;
      * `history` keeps every snapshot whose key, basis version,
        fingerprint and instant the store does not already carry, which is
        what a history satellite does -- it records observations, unchanged
        payloads included.

    The second clause is the store's own watermark: nothing lands behind
    the latest instant already stored for that key under the same basis
    version. A key with nothing stored under this basis has no watermark,
    so coalesce falls back to the candidate's own instant and the whole
    history offered for it is considered.

    A candidate whose key, basis version and instant match a stored row is
    written by the model's declared unique key, which replaces that row
    rather than adding beside it: a correction at an instant already stored
    corrects it in place. -#}
{% macro dpf_vault_new_versions(alias, key_column, fingerprint_column, effective_column, basis_column, kind) -%}
{%- if not is_incremental() -%}
true
{%- elif kind == 'history' -%}
not exists (
        select 1
        from {{ this }} as dpf_stored
        where dpf_stored.{{ key_column }} = {{ alias }}.{{ key_column }}
          and dpf_stored.{{ basis_column }} = {{ alias }}.{{ basis_column }}
          and dpf_stored.{{ fingerprint_column }} = {{ alias }}.{{ fingerprint_column }}
          and dpf_stored.{{ effective_column }} = {{ alias }}.{{ effective_column }}
    )
    and {{ dpf_vault_watermark(alias, key_column, effective_column, basis_column) }}
{%- else -%}
{{ alias }}.{{ fingerprint_column }} is distinct from (
        select dpf_latest.{{ fingerprint_column }}
        from {{ this }} as dpf_latest
        where dpf_latest.{{ key_column }} = {{ alias }}.{{ key_column }}
          and dpf_latest.{{ basis_column }} = {{ alias }}.{{ basis_column }}
        order by dpf_latest.{{ effective_column }} desc
        limit 1
    )
    and {{ dpf_vault_watermark(alias, key_column, effective_column, basis_column) }}
{%- endif -%}
{%- endmacro %}


{#- The watermark clause both satellite kinds are held to. Written once,
    called by both arms above, so the two kinds can never drift apart on
    what "behind the store" means. -#}
{% macro dpf_vault_watermark(alias, key_column, effective_column, basis_column) -%}
{{ alias }}.{{ effective_column }} >= coalesce(
        (
            select max(dpf_watermark.{{ effective_column }})
            from {{ this }} as dpf_watermark
            where dpf_watermark.{{ key_column }} = {{ alias }}.{{ key_column }}
              and dpf_watermark.{{ basis_column }} = {{ alias }}.{{ basis_column }}
        ),
        {{ alias }}.{{ effective_column }}
    )
{%- endmacro %}
