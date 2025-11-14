# Makefile for building the Go c-shared native library for the current host only
# and packaging the Java JAR. Cross-compilation has been removed: this Makefile
# always builds the native lib for the host where the Makefile runs (as reported
# by `go env GOOS` / `go env GOARCH`).
#
# Usage (from bindings/java):
#   make native    # build native library for the current host and place it under resources
#   make jar       # package the JAR (expects native lib in src/main/resources/native/<os-arch>/)
#   make all       # native + jar
#   make clean
#
SHELL := /bin/bash

GO ?= go
MVN ?= mvn

# Determine repository/module root by searching for go.mod upward from CWD.
CWD := $(CURDIR)
REPO_ROOT := $(shell cd $(CWD) && \
  DIR=$$(pwd); \
  while [ ! -f "$$DIR/go.mod" -a "$$DIR" != "/" ]; do DIR=$$(dirname "$$DIR"); done; \
  if [ -f "$$DIR/go.mod" ]; then echo "$$DIR"; else echo "$(CWD)"; fi)

BINDINGS_DIR := $(REPO_ROOT)/bindings/java
GO_BUILD_TARGET_DIR := $(BINDINGS_DIR)/src/main/resources/

# Use host GOOS/GOARCH from `go env` to avoid cross-compilation.
HOST_GOOS := $(shell $(GO) env GOOS)
HOST_GOARCH := $(shell $(GO) env GOARCH)

.PHONY: all native jar clean help

all: native jar

# Build native for the current host only (no cross-compilation).
native:
	@echo "Building native library for host GOOS=$(HOST_GOOS) GOARCH=$(HOST_GOARCH)"
	@echo "Running go build from module root: $(REPO_ROOT)"
	@# determine library filename and resource subdir
	@case "$(HOST_GOOS)" in \
	  windows) LIBNAME="swiftpaxos.dll";; \
	  darwin) LIBNAME="libswiftpaxos.dylib";; \
	  *) LIBNAME="libswiftpaxos.so";; \
	esac; \
	if [ "$(HOST_GOOS)" = "linux" ] && [ "$(HOST_GOARCH)" = "amd64" ]; then SUBDIR="linux-x86-64"; \
	elif [ "$(HOST_GOOS)" = "linux" ] && [ "$(HOST_GOARCH)" = "arm64" ]; then SUBDIR="linux-arm64"; \
	elif [ "$(HOST_GOOS)" = "darwin" ] && [ "$(HOST_GOARCH)" = "amd64" ]; then SUBDIR="darwin-x86-64"; \
	elif [ "$(HOST_GOOS)" = "darwin" ] && [ "$(HOST_GOARCH)" = "arm64" ]; then SUBDIR="darwin-arm64"; \
	elif [ "$(HOST_GOOS)" = "windows" ] && [ "$(HOST_GOARCH)" = "amd64" ]; then SUBDIR="windows-x86-64"; \
	else SUBDIR="$(HOST_GOOS)-$(HOST_GOARCH)"; fi; \
	mkdir -p "$(GO_BUILD_TARGET_DIR)/$$SUBDIR"; \
	cd "$(REPO_ROOT)" && CGO_ENABLED=1 GOOS="$(HOST_GOOS)" GOARCH="$(HOST_GOARCH)" $(GO) build -v -buildmode=c-shared -o "$(abspath $(GO_BUILD_TARGET_DIR))/$$SUBDIR/$$LIBNAME" ./bindings/java || { echo "Build failed for $$HOST_GOOS/$$HOST_GOARCH"; exit 1; }

# Package the Java JAR (assumes native lib is available in resources)
jar:
	@echo "Packaging Java JAR (with dependencies)..."
	cd "$(BINDINGS_DIR)" && $(MVN) install

clean:
	@echo "Cleaning Java target and native resource directories..."
	$(MVN) clean
	-rm -rf "$(BINDINGS_DIR)/target"
	-rm -rf "$(abspath $(GO_BUILD_TARGET_DIR))"/*
	@echo "Clean complete."

help:
	@printf "Makefile targets:\n"
	@printf "  make native  - build native library for current host (no cross-compilation)\n"
	@printf "  make jar     - package Java JAR (mvn package)\n"
	@printf "  make all     - native + jar\n"
	@printf "  make clean   - remove generated artifacts\n\n"
	@printf "Host GOOS: %s\n" "$(HOST_GOOS)"
	@printf "Host GOARCH: %s\n" "$(HOST_GOARCH)"
