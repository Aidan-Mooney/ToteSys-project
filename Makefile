# Makefile

SHELL := /bin/bash

define execute_in_env
	export PYTHONPATH=. && source venv/bin/activate && $1
endef

create-environment:
	@echo ">>> Setting up Venv"
	python -m venv venv

install-requirements: create-environment
	@echo ">>> Installing requirements."
	$(call execute_in_env, pip install -r ./requirements.depends.txt -r ./requirements.other.txt)
	$(call execute_in_env, pip install -r ./requirements.depends.txt -t dependencies/python)

install-dev-tools:
	@echo ">>> Installing Dev Tools"
	$(call execute_in_env, pip install bandit safety ruff pytest pytest-it pytest-cov moto)

security-checks:
	@echo ">>> Running security checks"
	# $(call execute_in_env, safety scan -r ./requirements.depends.txt -r ./requirements.other.txt)
	$(call execute_in_env, bandit -lll */*.py *c/*.py)

check-pep8-compliance:
	@echo ">>> Running ruff"
	$(call execute_in_env, ruff check src)
	$(call execute_in_env, ruff check test)

run-pytest:
	@echo ">>> Running pytest"
	$(call execute_in_env, pytest --it -vvvrP --cov=src --cov-fail-under=90 test/*)
