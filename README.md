SwiftPaxos
==========
[![Go Report Card](https://goreportcard.com/badge/github.com/imdea-software/swiftpaxos)](https://goreportcard.com/report/github.com/imdea-software/swiftpaxos)

The prototype implementation of SwiftPaxos, a new state machine replication protocol for geo-replication.

Installation
------------

    git clone https://github.com/imdea-software/swiftpaxos.git
    cd swiftpaxos
    go install github.com/imdea-software/swiftpaxos

Implemented protocols
---------------------

|  Name                   | Comments                                         |
|-------------------------|--------------------------------------------------|
| SwiftPaxos              | -                                                |
| Paxos                   | The classic Paxos protocol.                      |
| N<sup>2</sup>Paxos      | All-to-all variant of Paxos.           |
| CURP                    | CURP implemented over N<sup>2</sup>Paxos.         |
| Fast Paxos              | Fast Paxos with uncoordinated collision recovery. |
| EPaxos                  | A [corrected][epaxos_correct] version of EPaxos.  |

Usage
-----
#### participants
There are three types of participants: *master*, *servers* (also know as *replicas*) and *clients*.
Master coordinates communications between clients and servers. Servers and clients implement a provided protocol logic.

#### deployment configuration
To setup their execution, participants read deployment configuration file. See [aws.conf][config] for an example of configuration file.

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

 See [quorum.conf][quorum] and [latency.conf][latency] for the examples of quorum and latency configuration files.

[config]: aws.conf
[epaxos_correct]: https://github.com/otrack/on-epaxos-correctness
[quorum]: quorum.conf
[latency]: latency.conf