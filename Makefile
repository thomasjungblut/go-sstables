# go option
GO        ?= go
PKG       := $(shell glide novendor)
TAGS      :=
TESTS     := .
TESTFLAGS :=
LDFLAGS   :=
GOFLAGS   :=
BINARIES  := sstables

# Required for globs to work correctly
SHELL=/bin/bash

.PHONY: compile-proto
compile-proto:
	@echo
	@echo "==> Compiling Protobuf files <=="
	protoc --go_out=. recordio/test_files/text_line.proto

.PHONY: test
test: TESTFLAGS += -race -v

.PHONY: bench
bench:
	$(GO) test -v -benchmem -bench=. ./benchmark

.PHONY: unit-test
unit-test:
	@echo
	@echo "==> Running unit tests <=="
	$(GO) test $(GOFLAGS) -run $(TESTS) $(PKG) $(TESTFLAGS)

HAS_GLIDE := $(shell command -v glide;)
HAS_GIT := $(shell command -v git;)

.PHONY: bootstrap
bootstrap:
ifndef HAS_GLIDE
	go get -u github.com/Masterminds/glide
endif

ifndef HAS_GIT
	$(error You must install Git)
endif
	glide install --strip-vendor

