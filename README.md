SwiftPaxos: Fast Geo-Replicated State Machines
==========
[![Go Report Card](https://goreportcard.com/badge/github.com/imdea-software/swiftpaxos)](https://goreportcard.com/report/github.com/imdea-software/swiftpaxos)

This repository contains the prototype implementation of SwiftPaxos, a new state-machine replication protocol for geo-distributed systems.
SwiftPaxos is a _faster Paxos without compromises_.
In the best case, it executes a state-machine command in two message delays (one round-trip), and three otherwise.
SwiftPaxos was [presented](https://www.usenix.org/conference/nsdi24/presentation/ryabinin) at the 21st USENIX Symposium on Networked Systems Design and Implementation ([NSDI '24](https://www.usenix.org/conference/nsdi24)).

Installation
------------

    git clone https://github.com/imdea-software/swiftpaxos.git
    cd swiftpaxos
    go install github.com/imdea-software/swiftpaxos

Implemented protocols
---------------------

|  Protocol               | Comments                                          |
|-------------------------|---------------------------------------------------|
| SwiftPaxos              | See our NSDI'24 [paper](https://www.usenix.org/conference/nsdi24/presentation/ryabinin) for the full details.|
| Paxos                   | The classic Paxos protocol.                       |
| N<sup>2</sup>Paxos      | All-to-all variant of Paxos.                      |
| CURP                    | CURP implemented over N<sup>2</sup>Paxos.         |
| Fast Paxos              | Fast Paxos with uncoordinated collision recovery. |
| EPaxos                  | A [corrected][epaxos_correct] version of EPaxos.  |
| Janus                   | One-shot transactions over a sharded store, see [below](#janus). |

This software is based on the [Egalitarian Paxos](https://github.com/otrack/epaxos) code base, as well as the corrections made [here](https://github.com/otrack/epaxos).

Usage
-----
#### participants
There are three types of participants: *master*, *servers* and *clients*. 
The servers and clients implement the protocol logic. 
The master maintains the configuration of the system.

#### deployment configuration
To setup a run, the participants read deployment configuration file. 
See [aws.conf][config] for an example of configuration file for AWS EC2.

#### launching a participant

Master:
    
    swiftpaxos -run master -config conf.conf

Server:

    swiftpaxos -run server -config conf.conf -alias server_name

Client:

    swiftpaxos -run client -config conf.conf -alias client_name

#### command line options

    -alias alias
        An alias of this participant
    -config file
        Deployment config file (required)
    -latency file
        Latency config file
    -log file
        Path to the log file
    -protocol protocol
        Protocol to run. Overwrites protocol field of the config file
    -quorum file
        Quorum config file
    -run participant
        Run a participant

See [quorum.conf][quorum] and [latency.conf][latency] for an example of quorum and latency configuration files.

Java bindings
-----

To create them, one simply calls:

	make 
	
This compiles and installs the bindings in the local Maven repo (`~/.m2`).


Docker support
-----

Usage is as follows:

	./bin/image.sh # create the image $(whoami)/swiftpawos:latest
	docker run --rm -e NSERVERS=3 -e TYPE=master $(whoami)/swiftpaxos # launch the master node expecting three servers
	docker run --rm -e NSERVERS=3 -e TYPE=server -e MADDR=172.17.0.2 $(whoami)/swiftpaxos # create a server
	docker run --rm -e NSERVERS=3 -e TYPE=server -e MADDR=172.17.0.2 $(whoami)/swiftpaxos	
	docker run --rm -e NSERVERS=3 -e TYPE=server -e MADDR=172.17.0.2 $(whoami)/swiftpaxos
	

Janus
-----

`protocol: janus` runs [Janus](https://www.usenix.org/conference/osdi16/technical-sessions/presentation/mu) (OSDI'16),
a protocol for strictly serializable *one-shot* transactions over a sharded and replicated store.
It follows the failure-free path of the paper (Algorithms 1-5): PreAccept is sent to every replica of the shards
a transaction touches, and the fast path is taken only when *every* replica of every shard returns identical dependencies
(the fast quorum of Janus). Otherwise, once a majority of each shard answered, the union of the dependencies is fixed with
an Accept phase (majority of each shard). Committed transactions are executed in the order of the dependency graph,
strongly connected components in transaction id order; the dependencies of ancestors that do not touch a replica's shard
are obtained with an Inquire to the nearest replica involved in them.
The coordinator of a transaction runs on the replica the client contacts, rather than at the client as in the paper.
See [janus.conf](janus.conf) for an example.

**Sharding.** The parameter `replicasPerShard` (replicas section of the configuration) is the number of replicas of each shard.
It defaults to 0, which means that every replica replicates everything (a single shard).
The number of replicas must be a multiple of it.
Replica ids are given by the master in registration order, and replicas `[s*k, (s+1)*k)` form shard `s`.
A key lives in shard `key mod numberOfShards`; a SCAN is sent to all the shards, each one scanning its own keys
(results are concatenated in shard order).

**Transactions.** A transaction is a list of operations, each on a single key, among `PUT`, `GET` and `SCAN`
(the operations of `state/state.go`; the state machine is unchanged).
Clients submit it to their closest replica, which coordinates it:

    jc := janus.NewClient(c)           // c is a connected *client.Client (leaderless)
    vals, err := jc.Execute(janus.Txn{
        janus.Put(1, []byte("a")),
        janus.Get(2),
        janus.Scan(10, 5),
    })                                // vals[i] is the result of the i-th operation

When run with `-run client`, a Janus client executes `reqs` random transactions of `txnSize` operations
(`writes` % of them are PUTs, `conflicts` % of them target the hot key `key`).

**Limitations.** Recovery of a failed coordinator (Prepare phase, ballots, abandoned transactions) is not implemented:
a transaction whose coordinator crashes before committing blocks the transactions that depend on it.
A coordinator that does not hear from every replica gives up the fast path after a timeout and settles for a majority. Replicas keep the dependency graph of past transactions,
and the dependencies on SCANs, in memory (no garbage collection).

Flint
-----

To have an idea on how different replication protocols would compare, we wrote a tool named [Flint][flint]. 
Flint takes as input a set of AWS regions.
It computes the expected latencies and estimates how the protocols perform in such a deployment.

[config]: aws.conf
[epaxos_correct]: https://github.com/otrack/on-epaxos-correctness
[quorum]: quorum.conf
[latency]: latency.conf
[flint]: https://github.com/vonaka/flint
