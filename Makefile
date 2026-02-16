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