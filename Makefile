PYTHON_VERSION := 3.12.3
PYENV_ROOT     := $(HOME)/.pyenv
POETRY_BIN     ?= poetry
PROJECT_NAME   := sea-temperature
SRC		       := app

.PHONY: install-python install-poetry setup build-dev activate-shell add-kernel

install-python:
	# Install pyenv if it doesn't exist
	command -v pyenv >/dev/null 2>&1 || brew install pyenv
	# Install the desired Python version if not already installed
	pyenv install -s $(PYTHON_VERSION)
	# Set local Python version for this project (writes .python-version)
	pyenv local $(PYTHON_VERSION)

install-poetry:
	# Install pipx if it doesn't exist
	command -v pipx >/dev/null 2>&1 || brew install pipx
	# Install/upgrade Poetry globally via pipx
	pipx install --force "poetry==1.8.4"
	# Make Poetry create .venv inside the project (nice for VS Code)
	$(POETRY_BIN) config virtualenvs.in-project true

build-dev: install-python install-poetry
	$(POETRY_BIN) install

activate-shell:
	$(POETRY_BIN) shell

add-kernel-to-jupyter:
	$(POETRY_BIN) run python -m ipykernel install --user --name $(PROJECT_NAME) --display-name "$(PROJECT_NAME)"


format:
	$(POETRY_BIN) run isort $(SRC)
	$(POETRY_BIN) run black $(SRC)

check:
	$(POETRY_BIN) run isort $(SRC) -c
	$(POETRY_BIN) run black $(SRC) --check
	$(POETRY_BIN) run pylint $(SRC)


create-cron:
	fly machine run . \
	--app sea-temperature \
	--schedule daily \
	--entrypoint "python -m app.scripts.prewarm_tiles" \
	--vm-size shared-cpu-1x \
	--volume vol_vx2qxjw0dlqnz6wr:/data

download-historical-data:
	for m in $(seq -f "%02g" 1 12); do
		curl -O "https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DATA/temperature/netcdf/decav/0.25/woa23_decav_t${m}_04.nc"
	done
	wait
	echo "All downloaded"
	python extract_noaa_monthly.py

check-log:
	fly logs -a sea-temperature --no-tail | grep "Progress" | tail -3