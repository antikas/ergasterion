#!/usr/bin/env bash
# Shared named-placeholder renderer for the live and offline demo lanes.
#
# Usage (after sourcing this file):
#   dpf_render_query TEMPLATE NAME VALUE [NAME VALUE ...]
#
# Values are SQL identifiers only. Substitution is explicit and literal: no eval,
# envsubst, shell expansion of template contents, or implicit environment lookup.

dpf_render_query() {
    if [ "$#" -lt 3 ] || [ $((($# - 1) % 2)) -ne 0 ]; then
        echo "QUERY RENDER ERROR: expected TEMPLATE followed by NAME VALUE pairs" >&2
        return 2
    fi

    local template_path="$1"
    shift
    if [ ! -f "${template_path}" ]; then
        echo "QUERY RENDER ERROR: template not found: ${template_path}" >&2
        return 2
    fi

    local rendered
    rendered="$(<"${template_path}")"

    local name value token
    while [ "$#" -gt 0 ]; do
        name="$1"
        value="$2"
        shift 2

        case "${name}" in
            CATALOG|CALC_SCHEMA|MARTS_SCHEMA|RESOLUTION_SCHEMA|RAW_SCHEMA|CANONICAL_SCHEMA) ;;
            *)
                echo "QUERY RENDER ERROR: unknown placeholder name: ${name}" >&2
                return 2
                ;;
        esac
        if [[ ! "${value}" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
            echo "QUERY RENDER ERROR: invalid SQL identifier for ${name}: ${value}" >&2
            return 2
        fi

        token="\${${name}}"
        if [[ "${rendered}" != *"${token}"* ]]; then
            echo "QUERY RENDER ERROR: placeholder ${token} is not present in ${template_path}" >&2
            return 2
        fi
        rendered="${rendered//"${token}"/${value}}"
    done

    if [[ "${rendered}" =~ (\$\{[A-Za-z_][A-Za-z0-9_]*\}) ]]; then
        echo "QUERY RENDER ERROR: unresolved or unknown placeholder: ${BASH_REMATCH[1]}" >&2
        return 2
    fi

    printf '%s' "${rendered}"
}
