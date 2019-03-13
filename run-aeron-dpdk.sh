#!/usr/bin/env bash

sudo \
 AERON_DIR=/dev/shm/aeron-barkerm-dpdk \
 AERON_DIR_DELETE_ON_START=1
 AERON_DIR_WARN_IF_EXISTS=0
 ${DEBUG} ./cmake-build-debug/binaries/aeronmd_dpdk