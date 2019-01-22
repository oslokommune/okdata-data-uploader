SERVERLESS := $(shell command -v serverless 2> /dev/null)
ifndef SERVERLESS
$(error serverless not available please install)
endif

NPM := $(shell command -v npm 2> /dev/null)
ifndef NPM
$(error npm not available please install)
endif

node_modules: package.json package-lock.json
	$(NPM) install

.PHONY: deploy
deploy: node_modules
	$(SERVERLESS) deploy
	$(SERVERLESS) downloadDocumentation --outputFileName=openapi.yaml

FMT_EXT := {json,yml}
.PHONY: fmt
fmt: node_modules
	./node_modules/.bin/prettier --write '*.$(FMT_EXT)' 'doc/**/.$(FMT_EXT)' 
