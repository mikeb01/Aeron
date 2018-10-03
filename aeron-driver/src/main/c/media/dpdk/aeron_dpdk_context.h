//
// Created by barkerm on 3/10/18.
//

#ifndef AERON_AERON_DPDK_CONTEXT_H
#define AERON_AERON_DPDK_CONTEXT_H

void aeron_dpdk_init_eal(int argc, char** argv);

struct aeron_dpdk_context_stct;
typedef struct aeron_dpdk_context_stct aeron_dpdk_context_t;

int aeron_dpdk_init(aeron_dpdk_context_t** context);

#endif //AERON_AERON_DPDK_CONTEXT_H
