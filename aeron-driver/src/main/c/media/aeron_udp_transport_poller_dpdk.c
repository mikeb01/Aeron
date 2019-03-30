/*
 * Copyright 2014-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <unistd.h>
#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_ip.h>
#include <rte_udp.h>
#include <sys/socket.h>
#include <concurrent/aeron_spsc_rb.h>

#include "aeron_driver_context.h"
#include "util/aeron_arrayutil.h"
#include "aeron_alloc.h"
#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_transport_poller.h"

#ifdef USE_DPDK
#include "media/dpdk/aeron_dpdk_messaging.h"
#endif

int aeron_udp_transport_poller_init(
    aeron_driver_context_t* driver_context,
    aeron_udp_transport_poller_t* poller)
{
    poller->transports.array = NULL;
    poller->transports.length = 0;
    poller->transports.capacity = 0;

    // Assign driver context to poller so that dpdk context can be reached.
    poller->dpdk_context = driver_context->dpdk_context;

    return 0;
}

int aeron_udp_transport_poller_close(aeron_udp_transport_poller_t *poller)
{
    aeron_free(poller->transports.array);

    return 0;
}

int aeron_udp_transport_poller_add(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport)
{
    int ensure_capacity_result = 0;
    size_t index = poller->transports.length;

    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, poller->transports, aeron_udp_channel_transport_entry_t);
    if (ensure_capacity_result < 0)
    {
        return -1;
    }

    poller->transports.array[index].transport = transport;

    poller->transports.length++;

    return 0;
}

int aeron_udp_transport_poller_remove(aeron_udp_transport_poller_t *poller, aeron_udp_channel_transport_t *transport)
{
    int index = -1, last_index = (int)poller->transports.length - 1;

    for (int i = last_index; i >= 0; i--)
    {
        if (poller->transports.array[i].transport == transport)
        {
            index = i;
            break;
        }
    }

    if (index >= 0)
    {
        aeron_array_fast_unordered_remove(
            (uint8_t *)poller->transports.array,
            sizeof(aeron_udp_channel_transport_entry_t),
            (size_t)index,
            (size_t)last_index);

        poller->transports.length--;
    }

    return 0;
}

static bool is_matching_transport(uint32_t addr, uint16_t port, struct sockaddr_in* storage)
{
    return storage->sin_addr.s_addr == addr && storage->sin_port == port;
}

typedef struct loopback_client_data_stct
{
    void* clientd;
    aeron_udp_transport_recv_func_t recv_func;
    aeron_udp_transport_poller_t* poller;
}
loopback_clientd_t;

/**
 *
 * @param poller
 * @param message Internally this code expects the destination
 * address to be stored in the msghdr.msg_name this is to find the
 * local listener.  The control will contain the source address to
 * be passed to the recv callback to allow messages to be sent
 * back to the originator.
 * @param clientd
 * @return
 */
static int handle_received_message(
    struct msghdr* message,
    loopback_clientd_t* clientd)
{
    int dispatch_count = 0;
    struct sockaddr_in* dst_addr_in = message->msg_name;
    struct sockaddr_in* src_addr_in = message->msg_control;

    aeron_udp_transport_poller_t* poller = clientd->poller;

    assert(message->msg_namelen == sizeof(struct sockaddr_in));
    assert(src_addr_in != NULL);
    assert(src_addr_in->sin_family == PF_INET);
    assert(message->msg_controllen == sizeof(struct sockaddr_in));
    assert(dst_addr_in != NULL);
    assert(dst_addr_in->sin_family == PF_INET);
    assert(message->msg_iovlen == 1);

    const int last_index = (int)poller->transports.length - 1;
    for (int j = last_index; j >= 0; j--)
    {
        aeron_udp_channel_transport_t* transport = poller->transports.array[j].transport;
        if (is_matching_transport(
            dst_addr_in->sin_addr.s_addr, dst_addr_in->sin_port,
            (struct sockaddr_in*) &transport->bind_addr))
        {
            clientd->recv_func(
                clientd->clientd, transport->dispatch_clientd,
                message->msg_iov->iov_base, message->msg_iov->iov_len,
                (struct sockaddr_storage*) src_addr_in);

            // TODO: break???
            dispatch_count++;
        }
    }

    return dispatch_count;
}

// TODO: Move to core DPDK code??
static int process_ethernet_packet(
    aeron_udp_transport_poller_t *poller,
    const uint8_t* pkt_data, const uint32_t pkt_len,
    aeron_udp_transport_recv_func_t recv_func, void *clientd)
{
    struct ether_hdr* eth_hdr = (struct ether_hdr*) pkt_data;
    const uint16_t frame_type = rte_be_to_cpu_16(eth_hdr->ether_type);
    struct ipv4_hdr* ip_hdr;

    switch (frame_type)
    {
        case ETHER_TYPE_IPv4:

            ip_hdr = (struct ipv4_hdr*) ((uint8_t*) eth_hdr + sizeof(struct ether_hdr));

            if (IPPROTO_UDP == ip_hdr->next_proto_id)
            {
                int ipv4_hdr_len = (ip_hdr->version_ihl & IPV4_HDR_IHL_MASK) * IPV4_IHL_MULTIPLIER;
                struct udp_hdr* udp_hdr = (struct udp_hdr*) ((uint8_t*) ip_hdr + ipv4_hdr_len);
                uint8_t* msg_data = ((uint8_t*) udp_hdr) + sizeof(struct udp_hdr);
                const size_t msg_len = ip_hdr->total_length - (sizeof(struct udp_hdr) + ipv4_hdr_len);

                struct sockaddr_in src_addr_in;
                src_addr_in.sin_family = AF_INET;
                src_addr_in.sin_port = udp_hdr->src_port;
                src_addr_in.sin_addr.s_addr = ip_hdr->src_addr;

                struct sockaddr_in dst_addr_in;
                dst_addr_in.sin_family = AF_INET;
                dst_addr_in.sin_port = udp_hdr->dst_port;
                dst_addr_in.sin_addr.s_addr = ip_hdr->dst_addr;

                // TODO: Sanity check the packet length and the data length.

                struct msghdr message;
                message.msg_name = &dst_addr_in;
                message.msg_namelen = sizeof(dst_addr_in);
                struct iovec vec;
                vec.iov_base = msg_data;
                vec.iov_len = msg_len;
                message.msg_iov = &vec;
                message.msg_iovlen = 1;
                message.msg_control = &src_addr_in;
                message.msg_controllen = sizeof(src_addr_in);

                loopback_clientd_t loopback_clientd;
                loopback_clientd.poller = poller;
                loopback_clientd.clientd = clientd;
                loopback_clientd.recv_func = recv_func;

                const int msg_handled_by_receiver = handle_received_message(&message, &loopback_clientd);
                if (!msg_handled_by_receiver)
                {
                    // Forward all unhandled UDP to the sender.
                    aeron_spsc_rb_t* sender_loopback_q = aeron_dpdk_get_sender_udp_recv_q(poller->dpdk_context);
                    aeron_dpdk_write_sendmsg_rb(sender_loopback_q, &message);
                }
            }
            else
            {
                // Maybe a counter for unhandled IP protocol.
            }

            break;

        default:
            // Look specifically for ARP here.
            aeron_dpdk_unhandled_packet(poller->dpdk_context, pkt_data, pkt_len);
            break;
    }

    return 0;
}


static int poll_network(
    aeron_udp_transport_poller_t* poller,
    aeron_udp_transport_recv_func_t recv_func,
    void* clientd,
    uint32_t* total_bytes)
{
    const uint16_t num_mbufs = 32;
    struct rte_mbuf* mbufs[num_mbufs];

    const uint16_t port_id = aeron_dpdk_get_port_id(poller->dpdk_context);

    uint16_t num_pkts = rte_eth_rx_burst(port_id, 0, mbufs, num_mbufs);

    for (int i = 0; i < num_pkts; i++)
    {
        struct rte_mbuf* m = mbufs[i];
        const uint8_t* pkt_data = rte_pktmbuf_mtod(m, uint8_t*);
        const uint32_t pkt_len = rte_pktmbuf_pkt_len(m);

        process_ethernet_packet(poller, pkt_data, pkt_len, recv_func, clientd);
        (*total_bytes) += pkt_len;
    }

    return num_pkts;
}

static void poll_loopback_handler(int32_t msg_type, const void* data, size_t len, void* clientd)
{
    // TODO: Sanity check length

    loopback_clientd_t* loopback_clientd = (loopback_clientd_t*) clientd;

    uint8_t* namelen_p = (uint8_t*) data;
    uint8_t* name_p = namelen_p + sizeof(socklen_t);

    socklen_t namelen = *(socklen_t*) namelen_p;

    uint8_t* datalen_p = name_p + namelen;
    uint8_t* data_p = datalen_p + sizeof(size_t);

    struct sockaddr_in* in_sockaddr = (struct sockaddr_in*) name_p;
    assert(in_sockaddr->sin_family == AF_INET); // Only IPv4 supported (check elsewhere)

    size_t datalen = *(size_t*) datalen_p;

    uint8_t* controllen_p = data_p + datalen;
    uint8_t* control_p = controllen_p + sizeof(size_t);

    size_t controllen = *(size_t*) controllen_p;
    
    struct msghdr message;
    message.msg_namelen = namelen;
    message.msg_name = name_p;
    struct iovec vec;
    vec.iov_len = datalen;
    vec.iov_base = data_p;
    message.msg_iov = &vec;
    message.msg_iovlen = 1;
    message.msg_controllen = controllen;
    message.msg_control = controllen == 0 ? NULL : control_p;

    DPDK_DEBUG("Handling loopback: %s:%d\n", inet_ntoa(in_sockaddr->sin_addr), ntohs(in_sockaddr->sin_port));

    handle_received_message(&message, loopback_clientd);
}

static int poll_loopback(
    aeron_spsc_rb_t* loopback_q,
    aeron_udp_transport_poller_t* poller,
    aeron_udp_transport_recv_func_t recv_func,
    void* clientd,
    uint32_t* total_bytes)
{
    loopback_clientd_t loopback_clientd;
    loopback_clientd.clientd = clientd;
    loopback_clientd.recv_func = recv_func;
    loopback_clientd.poller = poller;

    const int64_t pre_read_head = loopback_q->descriptor->head_position;
    size_t num_msgs = aeron_spsc_rb_read(loopback_q, poll_loopback_handler, &loopback_clientd, 20);
    const int64_t post_read_head = loopback_q->descriptor->head_position;

    *total_bytes += (post_read_head - pre_read_head);

    return (int) num_msgs;
}

int aeron_udp_transport_poller_poll(
    aeron_udp_transport_poller_t *poller,
    struct mmsghdr *msgvec,
    size_t vlen,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
    uint32_t total_len = 0;
    int num_pkts;

    num_pkts = poll_network(poller, recv_func, clientd, &total_len);
    aeron_spsc_rb_t* recv_q = aeron_dpdk_get_recv_loopback(poller->dpdk_context);
    num_pkts += poll_loopback(recv_q, poller, recv_func, clientd, &total_len);

    msgvec[0].msg_len = total_len;
    for (size_t i = 1; i < vlen; i++)
    {
        msgvec[i].msg_len = 0;
    }

    return num_pkts;
}

int aeron_udp_transport_poller_poll_for_sender(
    aeron_udp_transport_poller_t *poller,
    struct mmsghdr *msgvec,
    size_t vlen,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
    // deal with arp.
    size_t work_done = aeron_dpdk_handle_other_protocols(poller->dpdk_context);
    
    // deal with messages for sender (e.g. sm/nak...)
    aeron_spsc_rb_t* to_send_q = aeron_dpdk_get_sender_udp_recv_q(poller->dpdk_context);

    uint32_t total_bytes = 0;

    // TODO: need to forward to unhandled packets onto the network.
    work_done += poll_loopback(to_send_q, poller, recv_func, clientd, &total_bytes);

    return (int) work_done;
}