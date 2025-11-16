#!/bin/bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# forward TERM/INT/QUIT to child
_forward() {
  sig="$1"
  kill -s "$sig" "$child" 2>/dev/null || true
}

trap '_forward TERM' TERM
trap '_forward INT' INT
trap '_forward QUIT' QUIT

if [ "${TYPE}" == "" ]; then
    echo "usage: define env variables, as listed below
    TYPE = [master,server]
    CONFIG
    ALIAS
    MADDR 
    NSERVERS
    "
    exit 0
fi

IFACE=$(ip route show default 2>/dev/null | awk '/default/ {print $5; exit}')
ADDR=$(ip -4 addr show dev "$IFACE" | awk '/inet /{print $2}' | cut -d/ -f1)

if [ "${TYPE}" == "master" ]; then
    echo "master mode: ${args}"
    args="-run master -config ${CONFIG} -nservers ${NSERVERS}"
    ${DIR}/swiftpaxos ${args} &
fi

if [ "${TYPE}" == "server" ]; then
    echo "server mode: ${args}"
    args="-run server -config ${CONFIG} -nservers ${NSERVERS} -maddr ${MADDR} -addr ${ADDR} -alias ${ADDR}"
    ${DIR}/swiftpaxos ${args} &
fi

child=$!

# reap the child and exit with its exit code
wait "$child"
rc=$?

# remove traps to avoid re-forwarding after child exit
trap - TERM INT QUIT
exit $rc
