"""Bronze documentation block manifest: every fenced code block in the Bronze-facing
documentation surface is classified, every offline_runnable block actually runs, every
manual block carries checked prerequisites and side effects, and every relative Markdown
link in the same surface resolves to a real file.

Scope: the private validator map names this test for the public documentation set below:
the whole-product overview, runbook, source-description guide, demo guides, architecture
overview, and Bronze deep dive. Manual blocks that need a network, external account,
administrator access, or destructive action are never executed here. The test only
asserts that they are correctly classified and documented.
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
WAREHOUSE_EVOLUTION_DOCS = [
    "README.md",
    "RUNBOOK.md",
    "docs/architecture/README.md",
    "demo/README.md",
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


class BronzeDocumentationBlocksTest(unittest.TestCase):
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

    def test_warehouse_evolution_terms_and_operator_claims_are_documented(self) -> None:
        corpus = "\n".join((REPO_ROOT / path).read_text(encoding="utf-8") for path in WAREHOUSE_EVOLUTION_DOCS)
        corpus = re.sub(r"\s+", " ", corpus)
        required = [
            "hashdiff basis",
            "evolution ledger",
            "extension",
            "re-baseline",
            "estate migration requirement",
            "effective column",
            "staging increment block",
            "consumption watermark",
            "delta window",
            "replay suppression",
            "same-effective-time correction",
            "post-extension column is captured only after a declared re-baseline",
            "one named remedy: widen",
            "the lookback for one run",
            "or run a bounded backfill",
            "silent update loss",
            "roughly doubles the stored source history",
            "Split satellites by change rate",
            "Every sibling source feeding the same entity maps the new column",
        ]
        for text in required:
            self.assertIn(text, corpus)

    def test_warehouse_evolution_commands_are_real_cli_commands(self) -> None:
        top = subprocess.run(
            [sys.executable, "-m", "ergasterion", "evolve", "--help"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(top.returncode, 0, top.stderr)
        self.assertIn("rebaseline", top.stdout)
        self.assertIn("audit-window", top.stdout)

        rebaseline = subprocess.run(
            [sys.executable, "-m", "ergasterion", "evolve", "rebaseline", "--help"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(rebaseline.returncode, 0, rebaseline.stderr)
        for option in ("--begin", "--complete", "--clear", "--abort"):
            self.assertIn(option, rebaseline.stdout)

        audit = subprocess.run(
            [sys.executable, "-m", "ergasterion", "evolve", "audit-window", "--help"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(audit.returncode, 0, audit.stderr)
        self.assertIn("--sample", audit.stdout)

    def test_warehouse_evolution_docs_keep_runtime_scope_honest(self) -> None:
        corpus = "\n".join((REPO_ROOT / path).read_text(encoding="utf-8") for path in WAREHOUSE_EVOLUTION_DOCS)
        self.assertIn("DuckDB is the executable reference implementation", corpus)
        self.assertIn("Snowflake and BigQuery are implemented", corpus)
        self.assertIn("dbt parsing", corpus)
        retired_claims = [
            "provides working targets for DuckDB, Snowflake, and BigQuery",
            "Live Snowflake validation",
            "live Snowflake demonstration",
            "deploys the same estate",
            "deploys and executes the Snowflake",
            "unproven at runtime",
            "runtime execution is unproven",
            "does not prove runtime execution",
            "runtime proof",
            "adapter-development material",
        ]
        for text in retired_claims:
            self.assertNotIn(text, corpus)

    def test_project_context_pins_warehouse_validation_positioning(self) -> None:
        context_path = REPO_ROOT / ".claude" / "CLAUDE.md"
        if not context_path.exists():
            self.assertTrue(
                (REPO_ROOT / "LICENSE").read_text(encoding="utf-8").startswith("MIT License"),
                "the private source must carry its Claude project context",
            )
            return
        context = context_path.read_text(encoding="utf-8")
        prose = " ".join(context.split())
        self.assertIn("DuckDB is the executable reference implementation", context)
        self.assertIn("Snowflake and BigQuery are implemented", context)
        self.assertIn("must neither claim a live Snowflake or BigQuery deployment", prose)
        self.assertIn("nor volunteer that one has not occurred", prose)

    def test_warehouse_evolution_docs_follow_hard_vocabulary_rules(self) -> None:
        for relpath in WAREHOUSE_EVOLUTION_DOCS:
            text = (REPO_ROOT / relpath).read_text(encoding="utf-8")
            self.assertNotIn("\u2013", text, relpath)
            self.assertNotIn("\u2014", text, relpath)
            self.assertIsNone(re.search(r"(?<!Bronze )\bcarry migration\b", text), relpath)
            self.assertIsNone(re.search(r"(?<!Bronze )\breset migration\b", text), relpath)


if __name__ == "__main__":
    unittest.main()
