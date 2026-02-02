.PHONY: setup playwright playwright-deps playwright-install

setup:
	uv sync
	$(MAKE) playwright

playwright: playwright-deps playwright-install

playwright-deps:
	uv run python -m playwright install-deps chromium

playwright-install:
	uv run python -m playwright install chromium
