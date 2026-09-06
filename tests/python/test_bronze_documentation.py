"""Documentation-block manifest and public-claim pins.

Two jobs, one test file.

The manifest job: every fenced code block in the documented surface below is
classified in ``tests/fixtures/bronze_documentation_blocks.json``, every
``offline_runnable`` block actually runs and exits zero, every ``manual`` block
carries checked prerequisites and side effects, and every relative Markdown link
in that surface resolves to a real file.

The claim job: the documents that state what this repository proves say the same
thing, in the same words, and none of them claims a third platform as a target or
implies evidence of a live deployment. A document is the only place a reader meets
those claims, so a drift here is a false claim shipped, not a cosmetic slip.

Manual blocks that need a network, an external account, administrator access or a
destructive action are never executed here. The test asserts only that they are
correctly classified and documented.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import unittest
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "bronze_documentation_blocks.json"

# The documents that state what this repository proves. Each one makes the
# two-adapter claim in its own voice, and each one is somewhere a reader can land
# first, so the claim is pinned in all of them rather than in one canonical place.
ADAPTER_CLAIM_DOCS = [
    "README.md",
    "RUNBOOK.md",
    "docs/architecture/README.md",
    "demo/README.md",
]

# The exact claim. DuckDB is executed; BigQuery is generated and gated offline.
# Both halves are pinned positively so a document cannot quietly drop the second
# one and leave a reader assuming both adapters are equally proved.
ADAPTER_CLAIM_TERMS = [
    "DuckDB executes",
    "BigQuery is a generation target with offline evidence",
]

# No document may present a third platform as a target of this repository. The
# engine's adapter axis is open, and a document may say so; naming a specific
# further platform as something this estate targets is the claim being refused
# (owner ruling R10). Checked over every documented file, not only the four above.
THIRD_TARGET_TERMS = [
    "snowflake",
    "databricks",
    "redshift",
    "synapse",
    "athena",
    "teradata",
    "clickhouse",
    "postgres",
    "sql server",
]

# Claims the documents used to make and must not make again: each one either
# implies runtime evidence that does not exist, or hedges an offline boundary into
# something vaguer than it is.
#
# The last two are assembled from their words rather than written out, because they
# are also on the publication gate's own forbidden list and this file ships with the
# product: a test that exists to keep a phrase out of the documents must not be the
# thing that carries it in.
RETIRED_CLAIMS = [
    "deploys the same estate",
    "unproven at runtime",
    "runtime execution is unproven",
    "does not prove runtime execution",
    "runtime proof",
    "adapter-development material",
    " ".join(("proven", "live")),
    " ".join(("verified", "live")),
]

FENCE_RE = re.compile(r"^```(\S*)\s*$")
# Markdown link/image targets: ![alt](target) or [text](target), target may carry a
# trailing "title" in quotes which this pattern excludes by stopping at whitespace/paren.
LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")

ALLOWED_EXTERNAL_HOSTS = {
    "github.com",
    "openinvestmentmodel.org",
    "bitol-io.github.io",
}


def _load_manifest() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _extract_fences(path: Path) -> list[tuple[str, list[str]]]:
    """Returns [(language, body_lines), ...] in document order."""

    lines = path.read_text(encoding="utf-8").splitlines()
    fences: list[tuple[str, list[str]]] = []
    i = 0
    while i < len(lines):
        match = FENCE_RE.match(lines[i])
        if match:
            language = match.group(1)
            body: list[str] = []
            j = i + 1
            while j < len(lines) and lines[j].strip() != "```":
                body.append(lines[j])
                j += 1
            fences.append((language, body))
            i = j + 1
        else:
            i += 1
    return fences


def _block_ids_for_file(relpath: str) -> dict[str, list[str]]:
    fences = _extract_fences(REPO_ROOT / relpath)
    return {f"{relpath}#{idx}": body for idx, (_lang, body) in enumerate(fences, start=1)}


def _extract_links(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    return LINK_RE.findall(text)


def _flat(text: str) -> str:
    """One line, single-spaced: a pinned phrase must survive line wrapping."""

    return re.sub(r"\s+", " ", text)


class DocumentationBlocksTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.manifest = _load_manifest()
        cls.files = cls.manifest["files"]
        cls.blocks = cls.manifest["blocks"]
        cls.discovered: dict[str, list[str]] = {}
        for relpath in cls.files:
            cls.discovered.update(_block_ids_for_file(relpath))

    def test_manifest_covers_every_discovered_fence_and_no_stale_entries(self) -> None:
        discovered_ids = set(self.discovered)
        manifest_ids = set(self.blocks)
        missing = discovered_ids - manifest_ids
        stale = manifest_ids - discovered_ids
        self.assertEqual(missing, set(), f"fenced blocks not classified in the manifest: {sorted(missing)}")
        self.assertEqual(stale, set(), f"manifest entries with no matching fenced block: {sorted(stale)}")

    def test_every_manifest_file_exists(self) -> None:
        for relpath in self.files:
            self.assertTrue(
                (REPO_ROOT / relpath).is_file(),
                f"the manifest names a document that is not in the tree: {relpath}",
            )

    def test_every_block_has_a_known_kind(self) -> None:
        known_kinds = set(self.manifest["kinds"])
        for block_id, entry in self.blocks.items():
            self.assertIn(entry["kind"], known_kinds, block_id)

    def test_manual_blocks_carry_prerequisites_and_side_effects(self) -> None:
        for block_id, entry in self.blocks.items():
            if entry["kind"] != "manual":
                continue
            self.assertTrue(entry.get("prerequisites"), f"{block_id}: manual block missing prerequisites")
            self.assertTrue(entry.get("side_effects"), f"{block_id}: manual block missing side_effects")

    def test_offline_runnable_blocks_actually_run(self) -> None:
        ran = []
        for block_id, entry in self.blocks.items():
            if entry["kind"] != "offline_runnable":
                continue
            body = self.discovered[block_id]
            for line in body:
                command = line.strip()
                if not command or command.startswith("#"):
                    continue
                ran.append((block_id, command))
                result = self._run_command(command)
                self.assertEqual(
                    result.returncode, 0,
                    f"{block_id} ({command!r}) exited {result.returncode}:\n"
                    f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
                )
        self.assertGreater(len(ran), 0, "no offline_runnable command actually ran")

    @staticmethod
    def _run_command(command: str) -> subprocess.CompletedProcess:
        if command in ("ergasterion --help", "python -m ergasterion --help"):
            return subprocess.run(
                [sys.executable, "-m", "ergasterion", "--help"],
                cwd=REPO_ROOT, capture_output=True, text=True,
            )
        if command.startswith("bash "):
            script_invocation = command[len("bash "):]
            parts = script_invocation.split()
            env = os.environ.copy()
            env["PY"] = sys.executable
            if sys.platform == "win32":
                wslenv = [entry for entry in env.get("WSLENV", "").split(":") if entry]
                wslenv = [entry for entry in wslenv if entry.split("/", 1)[0] != "PY"]
                wslenv.append("PY/p")
                env["WSLENV"] = ":".join(wslenv)
            return subprocess.run(
                ["bash", *parts], cwd=REPO_ROOT, capture_output=True, text=True, env=env,
            )
        raise AssertionError(f"no runner registered for offline_runnable command: {command!r}")

    def test_relative_markdown_links_resolve(self) -> None:
        broken = []
        external = []
        for relpath in self.files:
            doc_path = REPO_ROOT / relpath
            for target in _extract_links(doc_path):
                if target.startswith("#"):
                    continue  # same-document anchor
                parsed = urlparse(target)
                if parsed.scheme in ("http", "https"):
                    external.append((relpath, target, parsed.netloc))
                    continue
                # Local relative link: strip any #fragment, resolve against the doc's own
                # directory (Markdown link targets are relative to the linking file).
                local_target = target.split("#", 1)[0]
                if not local_target:
                    continue
                resolved = (doc_path.parent / local_target).resolve()
                if not resolved.exists():
                    broken.append((relpath, target))
        self.assertEqual(broken, [], f"broken relative Markdown links: {broken}")
        for relpath, target, netloc in external:
            self.assertIn(
                netloc, ALLOWED_EXTERNAL_HOSTS,
                f"{relpath}: external link host not allowlisted: {target}",
            )

    def test_every_claim_document_states_the_two_adapter_boundary(self) -> None:
        for relpath in ADAPTER_CLAIM_DOCS:
            corpus = _flat((REPO_ROOT / relpath).read_text(encoding="utf-8"))
            for term in ADAPTER_CLAIM_TERMS:
                self.assertIn(
                    term, corpus,
                    f"{relpath} does not state the adapter boundary: {term!r} is absent. "
                    "DuckDB is executed and BigQuery is generated and gated offline; a "
                    "document that states one half and not the other lets a reader assume "
                    "both are equally proved.",
                )

    def test_no_document_claims_a_third_platform_as_a_target(self) -> None:
        for relpath in self.files:
            corpus = _flat((REPO_ROOT / relpath).read_text(encoding="utf-8")).lower()
            for term in THIRD_TARGET_TERMS:
                self.assertNotIn(
                    term, corpus,
                    f"{relpath} names {term!r}. This estate declares two adapters, DuckDB "
                    "and BigQuery; a document may say the adapter axis is open, and may "
                    "not present a further platform as a target of this repository.",
                )

    def test_no_document_claims_live_deployment_evidence(self) -> None:
        for relpath in self.files:
            corpus = _flat((REPO_ROOT / relpath).read_text(encoding="utf-8")).lower()
            for term in RETIRED_CLAIMS:
                self.assertNotIn(term, corpus, f"{relpath}: retired claim present: {term!r}")

    def test_project_context_pins_the_evidence_positioning(self) -> None:
        context_path = REPO_ROOT / "AGENTS.md"
        if not context_path.exists():
            # The public projection does not carry the shared project context; this pin
            # applies to the source tree that does.
            self.skipTest("shared project context is not part of the public projection")
        context = context_path.read_text(encoding="utf-8")
        self.assertIn("DuckDB is the executable reference target", context)
        self.assertIn("Public claims must match positive evidence", context)
        self.assertIn("Do not imply live deployment evidence", context)

    def test_documents_follow_the_hard_vocabulary_rules(self) -> None:
        for relpath in self.files:
            text = (REPO_ROOT / relpath).read_text(encoding="utf-8")
            self.assertNotIn("\u2013", text, f"{relpath}: en dash")
            self.assertNotIn("\u2014", text, f"{relpath}: em dash")
            non_ascii = sorted({ch for ch in text if ord(ch) > 127})
            self.assertEqual(
                non_ascii, [],
                f"{relpath}: non-ASCII character(s) {non_ascii!r}; the documented surface is "
                "plain ASCII so a terminal, a diff and a projection all render it the same way",
            )


if __name__ == "__main__":
    unittest.main()
