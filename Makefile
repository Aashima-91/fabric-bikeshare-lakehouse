.PHONY: install run test test-offline notebooks clean
install:           ; pip install -r requirements.txt
run:               ; BIKESHARE_FORMAT=delta python run_local.py --reset            # needs internet once (Delta jars)
test:              ; BIKESHARE_FORMAT=delta python -m pytest -q                    # full suite on Delta Lake
test-offline:      ; BIKESHARE_FORMAT=parquet python -m pytest -q                  # Parquet stand-in, no jar download
clean:             ; rm -rf lakehouse_local .pytest_cache
