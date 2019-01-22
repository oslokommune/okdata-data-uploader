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

node_modules: package.json package-lock.json
	$(NPM) install

$(VENV_NAME):
	$(PYTHON3) -m venv env
	$(VENV_ACTIVATE); \
		pip install -r requirements.txt; \
		deactivate

.PHONY: deploy
deploy: node_modules
	$(SERVERLESS) deploy
	$(SERVERLESS) downloadDocumentation --outputFileName=openapi.yaml

FMT_EXT := {json,yml}
.PHONY: fmt
fmt: node_modules $(VENV_NAME)
	./node_modules/.bin/prettier --write '*.$(FMT_EXT)' 'doc/**/.$(FMT_EXT)' 
	$(VENV_ACTIVATE); \
		black src/; \
		deactivate
