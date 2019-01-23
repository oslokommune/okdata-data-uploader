SERVERLESS := $(shell command -v serverless 2> /dev/null)
ifndef SERVERLESS
$(error serverless not available please install)
endif

NPM := $(shell command -v npm 2> /dev/null)
ifndef NPM
$(error npm not available please install)
endif

PYTHON3_CMD ?= python3
PYTHON3 := $(shell command -v $(PYTHON3_CMD) 2> /dev/null)
ifndef PYTHON3
$(error $(PYTHON3_CMD) not available please install)
endif

VENV_NAME ?= env
VENV_ACTIVATE := . $(VENV_NAME)/bin/activate

requirements_in = $(wildcard *.in)
requirements_out := $(requirements_in:.in=.txt)

.PHONY: help
help:
	@echo "make requirements to generate requirements.txt files in correct order"
	@echo "make test to run tests"
	@echo "make fmt to automagically format source files"
	@echo "make deploy to deploy with serverless"

.PHONY: requirements
requirements: $(requirements_out)

%.txt: %.in
	$(VENV_ACTIVATE); \
			pip install pip-tools; \
			pip-compile --output-file $@ $<; \
			deactivate

requirements.txt: requirements-serverless.txt

requirements-dev.txt: requirements.txt

$(VENV_NAME): requirements-dev.txt
	$(PYTHON3) -m venv $(VENV_NAME)
	$(VENV_ACTIVATE); \
		pip install -r requirements-dev.txt; \
		deactivate

node_modules: package.json package-lock.json
	$(NPM) install

.PHONY: deploy
deploy: node_modules requirements-serverless.txt
	$(SERVERLESS) deploy
	$(SERVERLESS) downloadDocumentation --outputFileName=openapi.yaml

FMT_EXT := {json,yml,js,html}
.PHONY: fmt
fmt: node_modules $(VENV_NAME)
	./node_modules/.bin/prettier --write '*.$(FMT_EXT)' 'doc/**/*.$(FMT_EXT)' 'src/**/*.$(FMT_EXT)'
	$(VENV_ACTIVATE); \
		black src/; \
		deactivate

.PHONY: test
test: $(VENV_NAME)
	$(VENV_ACTIVATE); \
			pytest src/; \
			deactivate
