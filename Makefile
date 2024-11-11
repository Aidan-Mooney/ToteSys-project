# Makefile

SHELL := /bin/bash

create-environment:
	@echo ">>> Setting up Venv"
	python -m venv venv

install-requirements: create-environment
	@echo ">>> Installing requirements."
	source venv/bin/activate && pip install -r ./requirements.txt

define execute_in_env
	export PYTHONPATH=$(pwd) && source venv/bin/activate && $1
endef

install-dev-tools:
	$(call execute_in_env, pip install bandit safety ruff pytest)

security-checks:
	$(call execute_in_env, safety scan -r ./requirements.txt)
	$(call execute_in_env, bandit -lll */*.py *c/*.py)

check-pep8-compliance:
	$(call execute_in_env, ruff check src)
	$(call execute_in_env, ruff check test)

run-pytest:
	$(call execute_in_env, pytest -vvvrP)