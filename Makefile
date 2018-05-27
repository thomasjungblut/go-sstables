# go option
GO        ?= go
PKG       := $(shell glide novendor)
TAGS      :=
TESTS     := .
TESTFLAGS := -race
LDFLAGS   :=
GOFLAGS   :=
BINARIES  := sstables

# Required for globs to work correctly
SHELL=/bin/bash

.DEFAULT_GOAL := unit-test

.PHONY: compile-proto
compile-proto:
	@echo
	@echo "==> Compiling Protobuf files <=="
	protoc --go_out=. recordio/test_files/text_line.proto
	protoc --go_out=. examples/proto/hello_world.proto
	protoc --go_out=. sstables/proto/sstable.proto

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

