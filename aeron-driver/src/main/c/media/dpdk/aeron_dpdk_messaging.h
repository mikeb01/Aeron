//
// Created by barkerm on 3/10/18.
//

#ifndef AERON_AERON_DPDK_MESSAGING_H
#define AERON_AERON_DPDK_MESSAGING_H

struct aeron_dpdk_stct;
typedef struct aeron_dpdk_stct aeron_dpdk_t;

uint16_t aeron_dpdk_get_port_id(aeron_dpdk_t* context);
struct rte_mempool* aeron_dpdk_get_mempool(aeron_dpdk_t* context);
struct in_addr aeron_dpdk_get_local_addr(const aeron_dpdk_t* context);
bool aeron_dpdk_is_local_addr(const aeron_dpdk_t* context, const struct in_addr* addr);

int aeron_dpdk_unhandled_packet(aeron_dpdk_t* aeron_dpdk, const uint8_t* pkt_data, uint32_t pkt_len);

size_t aeron_dpdk_handle_other_protocols(aeron_dpdk_t* aeron_dpdk);

aeron_spsc_rb_t* aeron_dpdk_get_sender_udp_recv_q(aeron_dpdk_t* aeron_dpdk);
aeron_spsc_rb_t* aeron_dpdk_get_receiver_udp_send_q(aeron_dpdk_t* aeron_dpdk);
aeron_spsc_rb_t* aeron_dpdk_get_recv_loopback(aeron_dpdk_t* aeron_dpdk);

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

static inline aeron_rb_write_result_t aeron_dpdk_write_sendmsg_rb(aeron_spsc_rb_t* rb, struct msghdr* message)
{
    assert(1 == message->msg_iovlen);
    assert(NULL != message->msg_control);
    assert(0 != message->msg_controllen);

    struct iovec vec[6];
    vec[0].iov_len = sizeof(message->msg_namelen);
    vec[0].iov_base = &message->msg_namelen;
    vec[1].iov_len = message->msg_namelen;
    vec[1].iov_base = message->msg_name;
    vec[2].iov_len = sizeof(message->msg_iov[0].iov_len);
    vec[2].iov_base = &message->msg_iov[0].iov_len;
    vec[3].iov_len = message->msg_iov[0].iov_len;
    vec[3].iov_base = message->msg_iov[0].iov_base;
    vec[4].iov_len = sizeof(message->msg_controllen);
    vec[4].iov_base = &message->msg_controllen;
    vec[5].iov_len = message->msg_controllen;
    vec[5].iov_base = message->msg_control;

    return aeron_spsc_rb_writev(rb, 0, vec, 6);
}

#endif //AERON_AERON_DPDK_MESSAGING_H
