
########################################################################################################################
# Project setup
########################################################################################################################

init_env : init_virtualenv load_direnv install precommit_install
	@echo "✅ Environment initialized and ready to use 🔥"

init_virtualenv :
	@echo "Initializing environment ..."
	@if pyenv virtualenvs | grep -q 'velib_env'; then \
		echo "Virtualenv 'velib_env' already exists"; \
	else \
		echo "Virtualenv 'velib_env' does not exist"; \
		echo "Creating virtualenv 'velib_env' ..."; \
		pyenv virtualenv 3.10.12 velib_env; \
	fi
	@pyenv local velib_env
	@echo "✅ Virtualenv 'velib_env' activated"

load_direnv:
	@echo "Loading direnv ..."
	@direnv allow
	@echo "✅ Direnv loaded"

precommit_install:
	@echo "Installing pre-commit hooks ..."
	@pre-commit install
	@echo "✅ Pre-commit hooks installed"

install :
	@echo "Installing dependencies ..."
	@pip install --upgrade -q pip
	@pip install -q -r requirements.txt
	@echo "✅ Dependencies installed"
	@echo "Installing local package velib_env ..."
	@tree src
	@pip install -q -e .

precommit_run_all:
    @echo "Running all pre-commit hooks on all files ..."
    @pre-commit run --all-files
    @echo "✅ All pre-commit hooks have been run on all files"