# go option
GO        ?= go
TAGS      :=
TESTS     := ./...
TESTFLAGS := -race
LDFLAGS   :=
GOFLAGS   :=
BINARIES  := sstables
VERSION   := v1.2.0

# Required for globs to work correctly
SHELL=/bin/bash

.DEFAULT_GOAL := unit-test

.PHONY: compile-proto
compile-proto:
	@echo
	@echo "==> Compiling Protobuf files <=="
	protoc --go_out=. --go_opt=paths=source_relative recordio/test_files/text_line.proto
	protoc --go_out=. --go_opt=paths=source_relative simpledb/proto/wal_mutation.proto
	protoc --go_out=. --go_opt=paths=source_relative wal/test_files/seq_number.proto
	protoc --go_out=. --go_opt=paths=source_relative examples/proto/hello_world.proto
	protoc --go_out=. --go_opt=paths=source_relative examples/proto/mutation.proto
	protoc --go_out=. --go_opt=paths=source_relative sstables/proto/sstable.proto

.PHONY: release
release:
	@echo
	@echo "==> Preparing the release $(VERSION) <=="
	go mod tidy
	git tag ${VERSION}

.PHONY: bench
bench:
	$(GO) test -v -benchmem -bench=. ./benchmark

.PHONY: unit-test
unit-test:
	@echo
	@echo "==> Building <=="
	$(GO) build -race $(TESTS)
	@echo "==> Running unit tests <=="
	$(GO) clean -testcache
	$(GO) test $(GOFLAGS) $(TESTS) $(TESTFLAGS)
    # separately test simpledb, because the race detector
    # increases the runtime of the end2end tests too much
	$(GO) test $(GOFLAGS) ./simpledb

.PHONY: generate-test-files
generate-test-files:
	@echo
	@echo "==> Generate Test Files <=="
	export generate_compatfiles=true && $(GO) test $(GOFLAGS) $(TESTS) -run .*TestGenerateTestFiles.*
