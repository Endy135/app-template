# Force the use of Bash shell to be able to use the same Makefile between environments
SHELL=/bin/bash

# Documentation
APIDOC_OPTIONS = -d 1 --no-toc --separate --force --private

.PHONY: doc test coverage notebook

help:
	@echo "======================================================================================="
	@echo "                            STELLANTIS DATA SCIENCE TEMPLATE"
	@echo "======================================================================================="
	@echo "Avalaible commands:"
	@echo "dist        : create a package for release with the version of the last git tag"
	@echo "snapshot    : create a package for development with the version set to 'snapshot'"
	@echo "doc         : generate package documentation"
	@echo "rename      : change the name of the package (ask for old name and new package name)"
	@echo "install     : install all packages specified in requirements.txt"
	@echo "notebook    : install dependencies and launch jupyter notebook (ask for notebook password)"
	@echo "test        : launch unit tests, code coverage"
	@echo "format      : reformat code using PEP8 and PEP256 rules"
	@echo "clean       : remove cache"
	@echo "distclean   : remove distribution & egg info file"

# PACKAGE COMMANDS
dist: distclean
	@echo "===================================="
	@echo "==  MAKE PACKAGE FOR DISTRIBUTION =="
	@echo "===================================="
	source script/set_virtualenv.sh .venv3 && \
	source script/app_profile.sh && \
	source script/set_version.sh && \
	python setup.py sdist

snapshot: distclean
	source script/set_virtualenv.sh .venv3 && \
	source script/app_profile.sh && \
	python setup.py sdist

doc:
	source script/app_profile.sh && \
	rm docs/source/generated/*  || true
	rm -r docs/build/html/* || true
	export PYTHONPATH=$$pwd:$$PYTHONPATH && \
	source script/app_profile.sh && \
	sphinx-apidoc $(APIDOC_OPTIONS) -o docs/source/generated/ $$UNXAPPLI/$$UNXPACKAGE && \
	sphinx-build -M html docs/source docs/build

# ENVIRONMENT COMMANDS
rename:
	sh ./script/change_name.sh

install: .venv3
	source script/app_profile.sh && \
	pip install $$PIP_OPTIONS -r requirements.txt

.venv3:
	source script/set_virtualenv.sh .venv3 && \
	source script/app_profile.sh && \
	pip install $$PIP_OPTIONS --upgrade pip pypandoc

# DEVELOPMENT COMMANDS
notebook: .install_jupyter
	source script/app_profile.sh && \
	sh ./script/notebook.sh start

.install_jupyter: install
	source script/app_profile.sh && \
	pip install $$PIP_OPTIONS jupyter && \
	jupyter nbextension enable --py --sys-prefix widgetsnbextension && \
	pip install $$PIP_OPTIONS jupyter_contrib_nbextensions && \
	pip install $$PIP_OPTIONS jupyter_nbextensions_configurator && \
	jupyter nbextensions_configurator enable --user && \
	jupyter contrib nbextension install --sys-prefix

test: install
	source script/app_profile.sh && \
	python -m pytest --cov=$$UNXPACKAGE --cov-report term-missing --junitxml=testreport.xml test

format:
	source conf/app_profile.sh &&\
	black $$UNXPACKAGE &&\
	black test

# CLEANING COMMANDS
clean:
	@echo Cleaning $(current_dir)...
	find . -name __pycache__ -print | xargs rm -rf 2>/dev/null
	find . -name '*.pyc' -delete
	rm -rf .pytest_cache/

distclean: clean
	source script/app_profile.sh && \
	rm -rf dist
	find . -name '*.egg-info' -exec rm -rf {} +
