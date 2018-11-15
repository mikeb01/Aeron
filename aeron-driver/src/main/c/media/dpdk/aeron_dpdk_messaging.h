//
// Created by barkerm on 3/10/18.
//

#ifndef AERON_AERON_DPDK_MESSAGING_H
#define AERON_AERON_DPDK_MESSAGING_H

void aeron_dpdk_init_eal(int argc, char** argv);

struct aeron_dpdk_stct;
typedef struct aeron_dpdk_stct aeron_dpdk_t;

int aeron_dpdk_init(aeron_dpdk_t** context);

uint16_t aeron_dpdk_get_port_id(aeron_dpdk_t* context);
struct rte_mempool* aeron_dpdk_get_mempool(aeron_dpdk_t* context);
struct in_addr aeron_dpdk_get_local_addr(aeron_dpdk_t* context);

int aeron_dpdk_unhandled_packet(aeron_dpdk_t* aeron_dpdk, const uint8_t* pkt_data, uint32_t pkt_len);

size_t aeron_dpdk_handle_other_protocols(aeron_dpdk_t* aeron_dpdk);

aeron_spsc_rb_t* aeron_dpdk_get_sender_udp_recv_q(aeron_dpdk_t* aeron_dpdk);
aeron_spsc_rb_t* aeron_dpdk_get_receiver_udp_send_q(aeron_dpdk_t* aeron_dpdk);

struct ether_addr* aeron_dpdk_arp_lookup(aeron_dpdk_t* aeron_dpdk, uint32_t addr_in);

int aeron_dpdk_sendmsg(
    aeron_dpdk_t* aeron_dpdk,
    const struct sockaddr_in* bind_addr,
    const struct msghdr *message);

int aeron_dpdk_sendmmsg(
    aeron_dpdk_t* aeron_dpdk,
    const struct sockaddr_in* bind_addr,
    const struct mmsghdr *msgvec,
    size_t vlen);

int aeron_dpdk_sendmsg_for_receiver(
    aeron_dpdk_t* aeron_dpdk,
    const struct sockaddr_in* bind_addr,
    const struct msghdr *message);

int aeron_dpdk_send_ipv4(aeron_dpdk_t* aeron_dpdk, const char* packet, uint16_t ip_total_len);

#endif //AERON_AERON_DPDK_MESSAGING_H
