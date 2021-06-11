#!/bin/bash

set -eux

DAOS_TEST_SHARED_DIR=$(mktemp -d -p /mnt/share/)
trap 'rm -rf $DAOS_TEST_SHARED_DIR' EXIT

env

export DAOS_TEST_SHARED_DIR
export TEST_RPMS=true
export REMOTE_ACCT=jenkins
export WITH_VALGRIND="$WITH_VALGRIND

/usr/lib/daos/TESTING/ftest/ftest.sh "$TEST_TAG" "$TNODES" "$FTEST_ARG"
