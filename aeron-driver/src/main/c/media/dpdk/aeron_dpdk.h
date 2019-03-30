//
// Created by barkerm on 3/10/18.
//

#ifndef AERON_AERON_DPDK_H
#define AERON_AERON_DPDK_H

/*
 * IP address to that Aeron DPDK will "bind" to.  Required as it will need to
 * loopback sends to this address internally.
 */
#define AERON_DPDK_LOCAL_ADDRESS_ENV_VAR "AERON_DPDK_LOCAL_ADDRESS"
#define AERON_DPDK_USE_HARDWARE_LOOPBACK_ENV_VAR "AERON_DPDK_USE_HARDWARE_LOOPBACK"
#define AERON_DPDK_USER_ENV_VAR "AERON_DPDK_USER"
#define AERON_DPDK_GROUP_ENV_VAR "AERON_DPDK_GROUP"

void aeron_dpdk_init_eal(int argc, char** argv);

struct aeron_dpdk_stct;
typedef struct aeron_dpdk_stct aeron_dpdk_t;

int aeron_dpdk_init(aeron_dpdk_t** context);

#define DPDK_DEBUG_ENABLED 1
#define DPDK_DEBUG(fmt, ...)    \
    do                          \
    {                           \
        if (DPDK_DEBUG_ENABLED) \
        {                       \
            fprintf(stderr, "%s:%d:%s(): " fmt, __FILE__, \
                __LINE__, __func__, __VA_ARGS__); \
        }                       \
    }                           \
    while (0)

#endif //AERON_AERON_DPDK_H
