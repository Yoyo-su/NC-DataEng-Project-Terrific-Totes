PROJECT_NAME = FSCIFA-PROJECT
REGION = eu-west-2
PYTHON_INTERPRETER = python

IS_CI = $(CI)
SHELL := /bin/bash 
ACTIVATE_ENV := source venv/bin/activate

ifeq ($(IS_CI),true)
	PIP := pip
else
	PIP := $(ACTIVATE_ENV) && pip
endif

# Utility to run a command inside the virtual environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef
# Create a virtual environment (skip in CI)
create-environment:
ifeq ($(CI),true)
	@echo ">>> Skipping venv creation in CI."
else
	@echo ">>> Creating local virtual environment"
	python -m venv venv
endif
requirements: create-environment
ifeq ($(CI),true)
	$(PIP) install -r requirements.txt
else
	
	$(call execute_in_env, pip install -r requirements.txt)
endif # Dev setup tools: bandit, black, pytest-cov
dev-setup:
ifeq ($(CI),true)
	$(PIP) install bandit black pytest-cov flake8
else
	$(call execute_in_env, pip install bandit black pytest-cov flake8)
endif