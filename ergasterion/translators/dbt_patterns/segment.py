"""What an occurrence that needs a relation of its own contributes to a
product's rendering.

Most patterns render as one common table expression inside the product's
model (``ergasterion.translators.dbt_patterns.steps``). Three do not,
because each owes evidence about the rows reaching it, and evidence has to
be a relation something else can read:

  * Data Filtering owes every exclusion an audit by predicate and count, so
    the rows it filters must survive as a relation the filter log counts
    against;
  * Data Validation owes a quarantine relation naming the rule each failing
    row broke, so the rows reaching it, with the violation each rule
    computed, must survive as a relation the quarantine reads;
  * Data Curation owes resolution evidence, so the resolved records, their
    entity and their tier must survive as a relation the pending-key
    relation reads.

Each of the three therefore cuts the model's chain in two. Everything
rendered so far, plus whatever the occurrence itself needs to compute,
becomes an auxiliary model under the product's namespace; the companion
relations read it; and the product's chain resumes from it. One shape
covers all three, so the rendering loop never branches on which pattern cut
the chain.
"""

from __future__ import annotations

from dataclasses import dataclass

from ergasterion.translators.dbt_patterns.schema_doc import GeneratedTest
from ergasterion.translators.dbt_patterns.steps import Cte


@dataclass(frozen=True)
class Companion:
    """One model rendered beside an auxiliary model, reading it: a filter
    log, a quarantine relation, a pending-key relation."""

    suffix: str
    model_config: str
    ctes: tuple[Cte, ...]
    columns: tuple[str, ...]
    final_cte: str
    purpose: str
    description: str


@dataclass(frozen=True)
class Segment:
    """One occurrence's cut of a product's chain.

    ``ctes`` and ``columns`` finish the auxiliary model the cut closes and
    ``final_cte`` names the one its projection reads;
    ``companions`` are the relations rendered beside it; ``resume_columns``
    is what the product's chain reads back from it, and ``resume_ctes``
    are the common table expressions the occurrence itself renders on top
    of that read. ``tests`` are the generated dbt tests the occurrence
    places, on whichever model each belongs to.
    """

    suffix: str
    model_config: str
    ctes: tuple[Cte, ...]
    columns: tuple[str, ...]
    final_cte: str
    purpose: str
    description: str
    resume_columns: tuple[str, ...]
    resume_ctes: tuple[Cte, ...] = ()
    companions: tuple[Companion, ...] = ()
    tests: tuple[GeneratedTest, ...] = ()
    test_columns: tuple[str, ...] = ()


__all__ = ["Companion", "Segment"]
