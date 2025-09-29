# Repository Guidelines

## Project Structure & Module Organization
- `tools/`: NiFi XML loaders, classification heuristics, and conversion helpers; key modules include `tools/xml_tools.py` for parsing and `tools/conversion/` for Databricks-ready outputs.
- `streamlit_app/`: Streamlit workbench (`dashboard.py` plus `pages/`) exposing classification, variable discovery, script audit, and AI assist flows.
- `derived_classification_results/` & CSVs: cached batch outputs consumed by the UI and downstream notebooks.
- `nifi_pipeline_file/`: sanitized NiFi templates used for local experiments; keep additions redacted.
- `docs/`: migration plans, catalog notes, and analysis playbooks; update alongside major feature changes.
- `tests/`: pytest suites mirroring the runtime package layout.

## Build, Test, and Development Commands
- `./scripts/setup_dev.sh` – install hooks, UV, and local tooling for a fresh clone.
- `uv sync --group dev` – align dependencies with `uv.lock`.
- `uv run streamlit run streamlit_app` – start the multi-page migration dashboard.
- `uv run pytest [-k expr]` – execute automated tests; narrow scope with `-k` when iterating.
- `uv run mypy tools streamlit_app` – enforce typing across core modules.
- `uv run pre-commit run --all-files` – ensure Black/isort/Flake8/mypy parity with CI.

## Coding Style & Naming Conventions
- Python 3.13 target; 4-space indentation and Black’s 88-character limit (via pre-commit).
- Modules, functions, and files stay snake_case; prefer explicit type hints for new APIs.
- Keep UI copy concise; comment only when logic is non-obvious.

## Testing Guidelines
- Tests live under `tests/test_*.py` or `tests/*_test.py`; mirror production package paths.
- Use `@pytest.mark.slow` / `@pytest.mark.integration` for long-running suites.
- Regenerate derived artifacts with `uv run streamlit ...` or notebooks before committing regenerated fixtures.

## Commit & Pull Request Guidelines
- Commits: imperative tense, <72 characters, one logical change (e.g., `Add AI assist batching controls`).
- PRs: summary, testing notes (`uv run pytest` output), linked backlog issue, and screenshots/CLI captures for UI-affecting changes.
- Scrub `app.yaml`, notebooks, and overrides for credentials before review.

## Security & Configuration Tips
- Store secrets in local `.env`; never push Databricks tokens or unsanitized NiFi exports.
- Follow `docs/` sanitization checklists before adding new NiFi templates.
- Review generated CSV/JSON artifacts to confirm customer data is excluded before sharing.
