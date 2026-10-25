.PHONY: test build build-mycel-lab build-provider-emulator fmt check-docs

PROVIDER_EMULATOR_IMAGE ?= local/mycel-provider-emulator:dev

test:
	go test ./...

build: build-mycel-lab

build-mycel-lab:
	go build ./cmd/mycel-lab

build-provider-emulator:
	docker build -f Dockerfile.emulator -t $(PROVIDER_EMULATOR_IMAGE) .

fmt:
	gofmt -w $$(find . -name '*.go')

check-docs:
	python3 scripts/checkDocs.py docs/ tests/reliability/
