SwiftPaxos
==========
[![Go Report Card](https://goreportcard.com/badge/github.com/imdea-software/swiftpaxos)](https://goreportcard.com/report/github.com/imdea-software/swiftpaxos)

The prototype implementation of SwiftPaxos, a new state machine
replication protocol for geo-replication.

Installation
------------

    git clone https://github.com/imdea-software/swiftpaxos.git
    cd swiftpaxos
    go install github.com/imdea-software/swiftpaxos

Implemented protocols
---------------------

|  Protocol               | Comments                                          |
|-------------------------|---------------------------------------------------|
| SwiftPaxos              | See NSDI'24 [paper](https://www.usenix.org/conference/nsdi24/presentation/ryabinin) for more details. This is a prototype implementation and some features are not implemented, e.g., rollback on recovery. |
| Paxos                   | The classic Paxos protocol.                       |
| N<sup>2</sup>Paxos      | All-to-all variant of Paxos.                      |
| CURP                    | CURP implemented over N<sup>2</sup>Paxos.         |
| Fast Paxos              | Fast Paxos with uncoordinated collision recovery. |
| EPaxos                  | A [corrected][epaxos_correct] version of EPaxos.  |

Usage
-----
#### participants
There are three types of participants: *master*, *servers* and
*clients*. Servers and clients implement protocol logic. Master
coordinates communications between clients and servers.

#### deployment configuration
To setup their execution, participants read deployment configuration
file. See [aws.conf][config] for an example of configuration file.

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

See [quorum.conf][quorum] and [latency.conf][latency] for the examples
of quorum and latency configuration files.

Flint
-----

To have a better understanding on how different protocols suppose to
compare to each other we designed a tool called [Flint][flint]. Flint
computes expected latencies for the selected set of AWS regions and
estimates the improvement over other protocols.

[config]: aws.conf
[epaxos_correct]: https://github.com/otrack/on-epaxos-correctness
[quorum]: quorum.conf
[latency]: latency.conf
[flint]: https://github.com/vonaka/flint