SwiftPaxos
==========

The prototype implementation of SwiftPaxos, a new state machine replication protocol for geo-replication.

Installation
------------

    go install github.com/imdea-software/swiftpaxos

Implemented protocols
---------------------

|  Name                   | Comments                                         |
|-------------------------|--------------------------------------------------|
| [SwiftPaxos][paxoi_src] | -                                                |
| [Paxos][paxos_src]      | The classic Paxos protocol.                      |
| [N<sup>2</sup>Paxos][n2paxos_src] | All-to-all variant of Paxos.           |
| [CURP][curp_src]       | CURP implemented over N<sup>2</sup>Paxos.         |
| [Fast Paxos][curp_src] | Fast Paxos with uncoordinated collision recovery. |
| [EPaxos][epaxos_src]   | A [corrected][epaxos_correct] version of EPaxos.  |

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

 See [quorum.conf][quorum] and [latency.conf][latency] for the examples of quorum and latency configuration files, respectively.

[config]: https://github.com/imdea-software/swiftpaxos/aws.conf
[epaxos_correct]: https://github.com/otrack/on-epaxos-correctness
[quorum]: https://github.com/imdea-software/swiftpaxos/quorum.conf
[latency]: https://github.com/imdea-software/swiftpaxos/latency.conf
[epaxos]: https://github.com/efficient/epaxos
[epaxos_fix]: https://github.com/vonaka/shreplic/commit/5e4dcb5736dd3c4d3e87aeb18f67c4371e3c429c
[paxos_src]: https://github.com/vonaka/shreplic/tree/master/paxos
[n2paxos_src]: https://github.com/vonaka/shreplic/tree/master/n2paxos
[epaxos_src]: https://github.com/vonaka/shreplic/tree/master/epaxos
[paxoi_src]: https://github.com/vonaka/shreplic/tree/master/paxoi
[curp_src]: https://github.com/vonaka/shreplic/tree/master/curp