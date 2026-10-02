"""Unit tests for the release-stats renderer (.github/scripts/stats.py).

Run from the repository root:

    python3 -m unittest discover -s .github/scripts -p 'test_*.py' -v

The renderer is CI-only and rewrites README.md and .github/badges on a release
tag, so its parsing, formatting and marker handling are pinned here rather than
exercised only against the live repository.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import stats


def write_bucket(stats_dir, name, code=0, comment=0, blank=0, n_files=0):
    payload = {
        "SUM": {"code": code, "comment": comment, "blank": blank, "nFiles": n_files}
    }
    Path(stats_dir, f"{name}.json").write_text(json.dumps(payload), encoding="utf-8")


class SumsTests(unittest.TestCase):
    def test_parses_every_sum_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "lib.json")
            path.write_text(
                json.dumps(
                    {"SUM": {"code": 10, "comment": 20, "blank": 30, "nFiles": 4}}
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                stats.sums(path), {"code": 10, "comment": 20, "blank": 30, "nFiles": 4}
            )

    def test_missing_sum_and_fields_default_to_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "empty.json")
            path.write_text(json.dumps({"Go": {"code": 5}}), encoding="utf-8")
            self.assertEqual(stats.sums(path), {key: 0 for key in stats.KEYS})

    def test_null_fields_default_to_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "null.json")
            path.write_text(
                json.dumps(
                    {"SUM": {"code": None, "comment": 3, "blank": None, "nFiles": 1}}
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                stats.sums(path), {"code": 0, "comment": 3, "blank": 0, "nFiles": 1}
            )


class LoadBucketsTests(unittest.TestCase):
    def test_reads_the_five_buckets(self):
        with tempfile.TemporaryDirectory() as tmp:
            for index, name in enumerate(stats.BUCKETS):
                write_bucket(tmp, name, code=index + 1)
            buckets = stats.load_buckets(tmp)
            self.assertEqual(sorted(buckets), sorted(stats.BUCKETS))
            for index, name in enumerate(stats.BUCKETS):
                self.assertEqual(buckets[name]["code"], index + 1)


class TotalOfTests(unittest.TestCase):
    def test_sums_every_bucket_key(self):
        buckets = {
            "lib": {"code": 1, "comment": 2, "blank": 3, "nFiles": 1},
            "test": {"code": 10, "comment": 20, "blank": 30, "nFiles": 2},
            "bench": {"code": 100, "comment": 200, "blank": 300, "nFiles": 3},
            "tool": {"code": 1000, "comment": 2000, "blank": 3000, "nFiles": 4},
            "examples": {"code": 10000, "comment": 20000, "blank": 30000, "nFiles": 5},
        }
        self.assertEqual(
            stats.total_of(buckets),
            {"code": 11111, "comment": 22222, "blank": 33333, "nFiles": 15},
        )


class CoverageColorTests(unittest.TestCase):
    def test_exactly_full_coverage_is_bright_green(self):
        self.assertEqual(stats.coverage_color("100.0"), "brightgreen")

    def test_ninety_and_above_is_green(self):
        self.assertEqual(stats.coverage_color("90.0"), "green")
        self.assertEqual(stats.coverage_color("99.9"), "green")

    def test_below_ninety_is_red(self):
        self.assertEqual(stats.coverage_color("89.9"), "red")
        self.assertEqual(stats.coverage_color("0"), "red")

    def test_non_numeric_is_red(self):
        self.assertEqual(stats.coverage_color("n/a"), "red")


class RenderBadgesTests(unittest.TestCase):
    def setUp(self):
        self.buckets = {}
        codes = {"lib": 1, "test": 2, "bench": 3, "tool": 4, "examples": 5}
        for name, code in codes.items():
            self.buckets[name] = {"code": code, "comment": 0, "blank": 0, "nFiles": 0}

    def test_produces_exactly_the_seven_badges(self):
        badges = stats.render_badges("v1.2.3", "100.0", self.buckets)
        self.assertEqual(
            sorted(badges),
            [
                "coverage",
                "loc-bench",
                "loc-examples",
                "loc-library",
                "loc-tests",
                "loc-tooling",
                "loc-total",
            ],
        )

    def test_every_badge_carries_the_release_label(self):
        badges = stats.render_badges("v1.2.3", "100.0", self.buckets)
        for badge in badges.values():
            self.assertIn("v1.2.3", badge["label"])

    def test_coverage_badge_reports_coverage_and_color(self):
        badges = stats.render_badges("v1.2.3", "80.0", self.buckets)
        self.assertEqual(badges["coverage"]["message"], "80.0%")
        self.assertEqual(badges["coverage"]["color"], "red")

    def test_total_badge_sums_the_bucket_codes(self):
        badges = stats.render_badges("v1.2.3", "100.0", self.buckets)
        self.assertEqual(badges["loc-total"]["message"], f"{1 + 2 + 3 + 4 + 5}")
        self.assertEqual(badges["loc-examples"]["message"], "5")

    def test_badge_shape_is_shields_endpoint_schema(self):
        badge = stats.render_badges("v1.2.3", "100.0", self.buckets)["loc-library"]
        self.assertEqual(badge["schemaVersion"], 1)
        self.assertEqual(badge["cacheSeconds"], 300)
        self.assertEqual(badge["message"], "1")
        self.assertEqual(badge["color"], "blue")


class RenderBlockTests(unittest.TestCase):
    def setUp(self):
        self.buckets = {}
        codes = {"lib": 10, "test": 20, "bench": 30, "tool": 40, "examples": 50}
        for name, code in codes.items():
            self.buckets[name] = {
                "code": code,
                "comment": code * 2,
                "blank": code * 3,
                "nFiles": 1,
            }

    def test_block_is_marker_delimited(self):
        block = stats.render_block("v1.2.3", "100.0", self.buckets)
        self.assertTrue(block.startswith("<!-- stats:begin"))
        self.assertTrue(block.endswith("stats:end -->"))

    def test_block_lists_every_bucket_and_total(self):
        block = stats.render_block("v1.2.3", "100.0", self.buckets)
        for name in (
            "Library",
            "Tests",
            "Benchmarks",
            "Tooling",
            "Examples",
            "**Total**",
        ):
            self.assertIn(f"| {name} |", block)

    def test_total_row_sums_code_comment_blank_and_files(self):
        block = stats.render_block("v1.2.3", "100.0", self.buckets)
        self.assertIn("| **Total** | 150 | 300 | 450 | 5 |", block)

    def test_block_states_release_and_coverage(self):
        block = stats.render_block("v9.9.9", "87.5", self.buckets)
        self.assertIn("updated at **v9.9.9**", block)
        self.assertIn("Statement coverage **87.5%**", block)

    def test_block_is_deterministic(self):
        first = stats.render_block("v1.2.3", "100.0", self.buckets)
        second = stats.render_block("v1.2.3", "100.0", self.buckets)
        self.assertEqual(first, second)


class ReplaceBlockTests(unittest.TestCase):
    def setUp(self):
        self.buckets = {}
        for name in stats.BUCKETS:
            self.buckets[name] = {"code": 1, "comment": 1, "blank": 1, "nFiles": 1}
        self.block = stats.render_block("v1.2.3", "100.0", self.buckets)

    def test_replaces_only_the_marker_region(self):
        text = "before\n<!-- stats:begin --><!-- stats:end -->\nafter\n"
        new_text = stats.replace_block(text, self.block)
        self.assertTrue(new_text.startswith("before\n"))
        self.assertTrue(new_text.endswith("\nafter\n"))
        self.assertIn(self.block, new_text)

    def test_is_idempotent(self):
        text = "before\n<!-- stats:begin --><!-- stats:end -->\nafter\n"
        once = stats.replace_block(text, self.block)
        twice = stats.replace_block(once, self.block)
        self.assertEqual(once, twice)

    def test_missing_marker_exits(self):
        with self.assertRaises(SystemExit):
            stats.replace_block("no markers here", self.block)


class WriteOutputsTests(unittest.TestCase):
    def setUp(self):
        self.buckets = {}
        codes = {"lib": 1, "test": 2, "bench": 3, "tool": 4, "examples": 5}
        for name, code in codes.items():
            self.buckets[name] = {"code": code, "comment": 0, "blank": 0, "nFiles": 0}

    def _root(self, tmp, marker=True):
        root = Path(tmp, "repo")
        root.mkdir()
        block = "<!-- stats:begin --><!-- stats:end -->" if marker else "no marker"
        (root / "README.md").write_text(f"head\n{block}\ntail\n", encoding="utf-8")
        return root

    def _stats_dir(self, tmp):
        stats_dir = Path(tmp, "stats")
        stats_dir.mkdir()
        for name, bucket in self.buckets.items():
            write_bucket(
                stats_dir,
                name,
                code=bucket["code"],
                comment=bucket["comment"],
                blank=bucket["blank"],
                n_files=bucket["nFiles"],
            )
        return stats_dir

    def test_writes_seven_badges_and_the_readme_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            stats_dir = self._stats_dir(tmp)
            stats.write_outputs(root, stats_dir, "v1.0.0", "100.0")
            badges = sorted(p.name for p in (root / ".github" / "badges").iterdir())
            self.assertEqual(
                badges,
                [
                    "coverage.json",
                    "loc-bench.json",
                    "loc-examples.json",
                    "loc-library.json",
                    "loc-tests.json",
                    "loc-tooling.json",
                    "loc-total.json",
                ],
            )
            payload = json.loads(
                (root / ".github" / "badges" / "coverage.json").read_text()
            )
            self.assertEqual(payload["message"], "100.0%")
            readme = (root / "README.md").read_text(encoding="utf-8")
            self.assertIn("<!-- stats:begin — generated", readme)
            self.assertIn("updated at **v1.0.0**", readme)
            self.assertTrue(readme.startswith("head\n"))
            self.assertTrue(readme.endswith("tail\n"))

    def test_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp)
            stats_dir = self._stats_dir(tmp)
            stats.write_outputs(root, stats_dir, "v1.0.0", "100.0")
            first = (root / "README.md").read_text(encoding="utf-8")
            stats.write_outputs(root, stats_dir, "v1.0.0", "100.0")
            self.assertEqual(first, (root / "README.md").read_text(encoding="utf-8"))

    def test_missing_marker_writes_no_badges(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self._root(tmp, marker=False)
            stats_dir = self._stats_dir(tmp)
            with self.assertRaises(SystemExit):
                stats.write_outputs(root, stats_dir, "v1.0.0", "100.0")
            self.assertFalse((root / ".github" / "badges").exists())


class MainTests(unittest.TestCase):
    def test_reads_configuration_from_the_environment(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp, "repo")
            root.mkdir()
            (root / "README.md").write_text(
                "<!-- stats:begin --><!-- stats:end -->\n", encoding="utf-8"
            )
            stats_dir = Path(tmp, "stats")
            stats_dir.mkdir()
            for name in stats.BUCKETS:
                write_bucket(stats_dir, name, code=7)
            env = {
                "REPO_ROOT": str(root),
                "STATS_DIR": str(stats_dir),
                "LABEL": "v2.0.0",
                "TOTAL": "100.0",
            }
            with mock.patch.dict(os.environ, env):
                stats.main()
            self.assertTrue((root / ".github" / "badges" / "loc-total.json").exists())
            self.assertIn(
                "updated at **v2.0.0**",
                (root / "README.md").read_text(encoding="utf-8"),
            )


if __name__ == "__main__":
    unittest.main()
