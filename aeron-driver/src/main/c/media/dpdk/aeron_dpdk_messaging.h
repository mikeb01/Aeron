//
// Created by barkerm on 3/10/18.
//

#ifndef AERON_AERON_DPDK_MESSAGING_H
#define AERON_AERON_DPDK_MESSAGING_H

struct aeron_dpdk_stct;
typedef struct aeron_dpdk_stct aeron_dpdk_t;

bool aeron_dpdk_is_local_addr(const aeron_dpdk_t* context, const struct in_addr* addr);

aeron_spsc_rb_t* aeron_dpdk_get_send_loopback(aeron_dpdk_t* aeron_dpdk);
aeron_spsc_rb_t* aeron_dpdk_get_recv_loopback(aeron_dpdk_t* aeron_dpdk);

typedef enum aeron_dpdk_handler_result
{
    PROCESSED, NO_MATCHING_TARGET
}
aeron_dpdk_handler_result_t;

typedef enum aeron_dpdk_loopback_msg_type_enum
{
    ARP = 0, UDP_FOR_SENDER = 1, UDP_FOR_RECEIVER = 2
}
aeron_dpdk_loopback_msg_type_t;

typedef aeron_dpdk_handler_result_t (*aeron_dpdk_handle_message_t)(struct msghdr*, void*);

int aeron_dpdk_sendmsg(
    aeron_dpdk_t* aeron_dpdk,
    const struct sockaddr_in* bind_addr,
    const struct msghdr *message);

int aeron_dpdk_sendmmsg(
    aeron_dpdk_t* aeron_dpdk,
    const struct sockaddr_in* bind_addr,
    const struct mmsghdr *msgvec,
    size_t vlen);

int aeron_dpdk_poll_receiver_messages(
    aeron_dpdk_t* dpdk_context,
    aeron_dpdk_handle_message_t local_message_handler,
    void* clientd,
    uint32_t* total_bytes);

int aeron_dpdk_poll_sender_messages(
    aeron_dpdk_t* dpdk_context,
    aeron_dpdk_handle_message_t local_message_handler,
    void* clientd,
    uint32_t* total_bytes);

static inline aeron_rb_write_result_t aeron_dpdk_write_sendmsg_rb(
    aeron_spsc_rb_t* rb, struct msghdr* message, aeron_dpdk_loopback_msg_type_t msg_type)
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

    return aeron_spsc_rb_writev(rb, msg_type, vec, 6);
}

#endif //AERON_AERON_DPDK_MESSAGING_H
