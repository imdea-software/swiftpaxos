#!/usr/bin/env bash

if [ -z "$1" ]; then
    TAG=latest
else
    TAG="$1"
fi

DOCKER_USER=$(docker info |
    grep Username |
    awk '{print $2}')
if [ -z "${DOCKER_USER}" ]; then
    DOCKER_USER=$(whoami)
fi

DIR=$(dirname "$0")
IMAGE=${DOCKER_USER}/swiftpaxos:${TAG}
DOCKERFILE=${DIR}/../Dockerfile

# swiftpaxos version: last commit hash
git log -1 --format="%H" >${DIR}/../swiftpaxos-version

#
make compile

# build image
docker build \
    --no-cache \
    -t "${IMAGE}" -f "${DOCKERFILE}" .

# push image
# docker push "${IMAGE}"
