# go option
GO        ?= go
TAGS      :=
BUILDFLAGS := -race
TESTS     := ./...
TESTFLAGS := -race
LDFLAGS   :=
GOFLAGS   :=
BINARIES  := sstables
VERSION   := v1.3.0

# Required for globs to work correctly
SHELL=/bin/bash

.DEFAULT_GOAL := unit-test

.PHONY: compile-proto
compile-proto:
	@echo
	@echo "==> Compiling Protobuf files <=="
	protoc --go_out=. --go_opt=paths=source_relative recordio/test_files/text_line.proto
	protoc --go_out=. --go_opt=paths=source_relative simpledb/proto/wal_mutation.proto
	protoc --go_out=. --go_opt=paths=source_relative simpledb/proto/compaction_metadata.proto
	protoc --go_out=. --go_opt=paths=source_relative wal/test_files/seq_number.proto
	protoc --go_out=. --go_opt=paths=source_relative _examples/proto/hello_world.proto
	protoc --go_out=. --go_opt=paths=source_relative _examples/proto/mutation.proto
	protoc --go_out=. --go_opt=paths=source_relative sstables/proto/sstable.proto

.PHONY: release
release:
	@echo
	@echo "==> Preparing the release $(VERSION) <=="
	go mod tidy
	git tag ${VERSION}

.PHONY: bench
bench:
	$(GO) test -v -benchmem -bench=RecordIO ./benchmark
	$(GO) test -v -benchmem -bench=SSTable ./benchmark

.PHONY: bench-simpledb
bench-simpledb:
	$(GO) test -v -benchmem -bench=SimpleDB ./benchmark

.PHONY: unit-test
unit-test:
	@echo
	@echo "==> Building <=="
	$(GO) build $(BUILDFLAGS) -race $(TESTS)
	@echo "==> Running unit tests <=="
	$(GO) clean -testcache
	$(GO) test $(GOFLAGS) $(TESTFLAGS) $(TESTS)
    # separately test simpledb, because the race detector
    # increases the runtime of the end2end tests too much (10-20m)
    # the race-simpledb target can be used to test that
	$(GO) test -v --tags simpleDBe2e $(GOFLAGS) ./simpledb

.PHONY: race-simpledb
race-simpledb:
	@echo
	@echo "==> Running simpledb race tests <=="
	$(GO) clean -testcache
	$(GO) test -v -timeout 30m --tags simpleDBe2e $(GOFLAGS) ./simpledb $(TESTFLAGS)

.PHONY: crash-simpledb
crash-simpledb:
	@echo
	@echo "==> Running simpledb crash tests <=="
	$(GO) clean -testcache
	$(GO) test -v -timeout 30m --tags simpleDBcrash $(GOFLAGS) ./simpledb/_crash_tests $(TESTFLAGS)

.PHONY: linear-simpledb
race-simpledb:
	@echo
	@echo "==> Running simpledb linearizability tests <=="
	$(GO) clean -testcache
	$(GO) test -v -timeout 30m --tags simpleDBlinear $(GOFLAGS) ./simpledb $(TESTFLAGS)


.PHONY: generate-test-files
generate-test-files:
	@echo
	@echo "==> Generate Test Files <=="
	$(GO) clean -testcache
	export generate_compatfiles=true && $(GO) test $(GOFLAGS) $(TESTS) -run .*TestGenerateTestFiles.*

.PHONY: vet
vet:
	@echo
	@echo "==> Go vet <=="
	$(GO) vet $(TESTS)

.PHONY: full-test
full-test: vet unit-test race-simpledb crash-simpledb
