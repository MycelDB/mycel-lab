.PHONY: test build build-mycel-lab fmt check-docs

test:
	go test ./...

build: build-mycel-lab

build-mycel-lab:
	go build ./cmd/mycel-lab

fmt:
	gofmt -w $$(find . -name '*.go')

check-docs:
	python3 scripts/checkDocs.py docs/ tests/reliability/
