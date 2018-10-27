//
// Created by barkerm on 3/10/18.
//

#ifndef AERON_AERON_DPDK_CONTEXT_H
#define AERON_AERON_DPDK_CONTEXT_H

void aeron_dpdk_init_eal(int argc, char** argv);

struct aeron_dpdk_stct;
typedef struct aeron_dpdk_stct aeron_dpdk_t;

int aeron_dpdk_init(aeron_dpdk_t** context);

uint16_t aeron_dpdk_get_port_id(aeron_dpdk_t* context);
struct rte_mempool* aeron_dpdk_get_mempool(aeron_dpdk_t* context);

int aeron_dpdk_unhandled_packet(aeron_dpdk_t* aeron_dpdk, const uint8_t* pkt_data, uint32_t pkt_len);

size_t aeron_dpdk_handle_other_protocols(aeron_dpdk_t* aeron_dpdk);

aeron_spsc_rb_t* aeron_dpdk_get_sender_udp_recv_q(aeron_dpdk_t* aeron_dpdk);

#endif //AERON_AERON_DPDK_CONTEXT_H
