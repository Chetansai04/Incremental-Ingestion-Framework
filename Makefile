PY ?= python3
export PYTHONPATH := src

.PHONY: install test demo bench clean
install:        ## only pytest is needed (the pipeline itself is stdlib-only)
	$(PY) -m pip install -r requirements.txt
test:
	$(PY) -m pytest -v
demo:           ## generate sample data, crash + restart, reconcile
	bash scripts/demo.sh
bench:          ## throughput on ~260k rows (takes ~1 min)
	$(PY) -m ingest benchmark --out sample_output/performance.json
clean:
	rm -rf sample_output/warehouse.db* .pytest_cache **/__pycache__
