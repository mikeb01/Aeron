//
// Created by barkerm on 3/10/18.
//

#ifndef AERON_AERON_DPDK_H
#define AERON_AERON_DPDK_H

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
