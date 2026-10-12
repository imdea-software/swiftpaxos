export GO111MODULE := on

# tag of the Docker image (make image TAG=...)
TAG ?= latest

.PHONY: all compile clean image

all: compile

compile:
	GOPATH=`pwd` go install github.com/imdea-software/swiftpaxos
	make -C bindings/java
clean:
	go clean

# Docker image <docker user>/swiftpaxos:$(TAG), see bin/image.sh
image:
	bin/image.sh $(TAG)
