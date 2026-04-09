.PHONY: setup playwright playwright-deps playwright-install lint-check lint-fix run-medeia

setup:
	uv sync
	$(MAKE) playwright

playwright: playwright-deps playwright-install

playwright-deps:
	uv run python -m playwright install-deps chromium

playwright-install:
	uv run python -m playwright install chromium

lint-check:
	uv run ruff format --check src/
	uv run ruff check src/
	uv run mypy src/

lint-fix:
	uv run ruff format src/
	uv run ruff check --fix src/

run-medeia:
	@set -a; \
	[ -f .env ] && . ./.env; \
	set +a; \
	uv run python src/medeia.py $(ARGS)
