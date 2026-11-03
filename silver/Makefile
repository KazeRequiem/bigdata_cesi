# Raccourcis (Linux / WSL)
.PHONY: test zip demo

test:
	python -m pytest -q tests

zip:
	mkdir -p dist && cd src && zip -qr ../dist/silver.zip silver -x '*__pycache__*'

demo:
	rm -rf /tmp/demo && python tests/fixtures_bronze.py /tmp/demo/bronze && \
	PYTHONPATH=src SILVER_HMAC_KEY=demo BRONZE_ROOT=/tmp/demo/bronze SILVER_STORAGE=parquet \
	SILVER_LOCAL_ROOT=/tmp/demo/silver SILVER_REJECT_THRESHOLD=0.9 python silver_cli.py run --job all && \
	PYTHONPATH=src BRONZE_ROOT=/tmp/demo/bronze SILVER_STORAGE=parquet SILVER_LOCAL_ROOT=/tmp/demo/silver \
	python silver_cli.py qualite --out /tmp/demo/rapport_qualite.md
