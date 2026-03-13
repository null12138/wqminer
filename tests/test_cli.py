import unittest

from wqminer.cli import build_parser


class CLITest(unittest.TestCase):
    def test_fetch_full_parsing(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "fetch-fields",
                "--username",
                "user@example.com",
                "--password",
                "secret",
                "--region",
                "USA",
                "--full",
            ]
        )
        self.assertEqual(args.command, "fetch-fields")
        self.assertTrue(args.full)

    def test_validate_command_parsing(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "validate",
                "--username",
                "user@example.com",
                "--password",
                "secret",
                "--region",
                "USA",
                "--no-simulation",
            ]
        )
        self.assertEqual(args.command, "validate")
        self.assertEqual(args.username, "user@example.com")
        self.assertTrue(args.no_simulation)

    def test_scrape_playwright_state_parsing(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "scrape-templates",
                "--community-url",
                "https://example.com/community",
                "--playwright",
                "--playwright-state",
                "data/cache/community_storage_state.json",
            ]
        )
        self.assertEqual(args.command, "scrape-templates")
        self.assertTrue(args.playwright)
        self.assertEqual(args.playwright_state, "data/cache/community_storage_state.json")

    def test_community_login_parsing(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "community-login",
                "--state-file",
                "data/cache/community_storage_state.json",
                "--wait-seconds",
                "120",
            ]
        )
        self.assertEqual(args.command, "community-login")
        self.assertEqual(args.wait_seconds, 120)

    def test_harvest_once_parsing(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "harvest-once",
                "--username",
                "user@example.com",
                "--password",
                "secret",
                "--regions",
                "USA,GLB",
                "--playwright",
                "--playwright-headful",
            ]
        )
        self.assertEqual(args.command, "harvest-once")
        self.assertEqual(args.regions, "USA,GLB")
        self.assertTrue(args.playwright)
        self.assertTrue(args.playwright_headful)

    def test_gen_templates_iter_parsing(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "gen-templates-iter",
                "--llm-config",
                "llm.json",
                "--fields-file",
                "data/cache/data_fields_USA_1_TOP3000.json",
                "--count",
                "50",
                "--rounds",
                "4",
                "--max-fix-attempts",
                "2",
            ]
        )
        self.assertEqual(args.command, "gen-templates-iter")
        self.assertEqual(args.count, 50)
        self.assertEqual(args.rounds, 4)
        self.assertEqual(args.max_fix_attempts, 2)

    def test_build_syntax_manual_parsing(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "build-syntax-manual",
                "--operators-file",
                "results/harvest/operators_api.json",
                "--templates-file",
                "templates/scraped_templates.json",
                "--output-md",
                "docs/manual.md",
                "--output-json",
                "docs/manual.json",
            ]
        )
        self.assertEqual(args.command, "build-syntax-manual")
        self.assertEqual(args.output_md, "docs/manual.md")
        self.assertEqual(args.output_json, "docs/manual.json")

    def test_gen_swappable_parsing(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "gen-swappable",
                "--llm-config",
                "llm.json",
                "--template-count",
                "90",
                "--fills-per-template",
                "8",
                "--max-expressions",
                "500",
            ]
        )
        self.assertEqual(args.command, "gen-swappable")
        self.assertEqual(args.template_count, 90)
        self.assertEqual(args.fills_per_template, 8)
        self.assertEqual(args.max_expressions, 500)

    def test_submit_concurrent_parsing(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "submit-concurrent",
                "--username",
                "user@example.com",
                "--password",
                "secret",
                "--templates-file",
                "templates/x.json",
                "--concurrency",
                "3",
                "--max-submissions",
                "30",
            ]
        )
        self.assertEqual(args.command, "submit-concurrent")
        self.assertEqual(args.concurrency, 3)
        self.assertEqual(args.max_submissions, 30)


if __name__ == "__main__":
    unittest.main()
