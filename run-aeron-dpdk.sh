#!/usr/bin/env bash

sudo                                    \
 AERON_DIR=/dev/shm/aeron-barkerm-dpdk  \
 AERON_DIR_DELETE_ON_START=1            \
 AERON_DIR_WARN_IF_EXISTS=0             \
 AERON_CONDUCTOR_IDLE_STRATEGY=sleeping \
 AERON_THREADING_MODE=shared            \
 AERON_DPDK_LOCAL_ADDRESS=192.168.178.201 \
 ${DEBUG} ./cmake-build-debug/binaries/aeronmd_dpdk