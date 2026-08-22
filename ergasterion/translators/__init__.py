"""Target-specific translators that convert a resolved Ergasterion execution
plan into runnable platform artefacts.

This package depends on ``ergasterion.framework``; the framework never imports
it. Concrete translators, such as a local-ingestion translator and a dbt
translator, implement the stable ``Translator`` interface in ``base.py``.
"""

from ergasterion.translators.base import Translator

__all__ = ["Translator"]
