.PHONY: test build build-mycel-lab fmt

test:
	go test ./...

build: build-mycel-lab

build-mycel-lab:
	go build ./cmd/mycel-lab

fmt:
	gofmt -w $$(find . -name '*.go')
