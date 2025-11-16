export GO111MODULE := on

all: compile

compile:
	GOPATH=`pwd` go install github.com/imdea-software/swiftpaxos
	make -C bindings/java 
clean:
	go clean

