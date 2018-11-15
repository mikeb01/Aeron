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

bool is_matching_transport(uint32_t addr, uint16_t port, struct sockaddr_in* storage)
{
    return storage->sin_addr.s_addr == addr && storage->sin_port == port;
}

static int process_ethernet_packet(
    aeron_udp_transport_poller_t *poller,
    const uint8_t* pkt_data, const uint32_t pkt_len,
    aeron_udp_transport_recv_func_t recv_func, void *clientd)
{
    struct ether_hdr* eth_hdr = (struct ether_hdr*) pkt_data;
    const uint16_t frame_type = rte_be_to_cpu_16(eth_hdr->ether_type);
    struct ipv4_hdr* ip_hdr;

    struct sockaddr_storage msg_name;
    memset(&msg_name, 1, sizeof(msg_name));

    switch (frame_type)
    {
        case ETHER_TYPE_IPv4:

            ip_hdr = (struct ipv4_hdr*) ((uint8_t*) eth_hdr + sizeof(struct ether_hdr));

            if (IPPROTO_UDP == ip_hdr->next_proto_id)
            {
                int ipv4_hdr_len = (ip_hdr->version_ihl & IPV4_HDR_IHL_MASK) * IPV4_IHL_MULTIPLIER;
                struct udp_hdr* udp_hdr = (struct udp_hdr*) ((uint8_t*) ip_hdr) + ipv4_hdr_len;
                uint8_t* msg_data = ((uint8_t*) udp_hdr) + sizeof(struct udp_hdr);
                const size_t msg_len = ip_hdr->total_length - (sizeof(struct udp_hdr) + ipv4_hdr_len);

                struct sockaddr_in* in_addr = (struct sockaddr_in*) &msg_name;
                in_addr->sin_family = AF_INET;
                in_addr->sin_port = udp_hdr->src_port;
                in_addr->sin_addr.s_addr = ip_hdr->src_addr;

                // TODO: Sanity check the packet length and the data length.

                const int last_index = (int)poller->transports.length - 1;
                for (int j = last_index; j >= 0; j--)
                {
                    aeron_udp_channel_transport_t* transport = poller->transports.array[j].transport;
                    if (is_matching_transport(
                        ip_hdr->dst_addr, udp_hdr->dst_port,
                        (struct sockaddr_in*) &transport->bind_addr))
                    {
                        recv_func(
                            clientd, transport->dispatch_clientd,
                            msg_data, msg_len,
                            &msg_name);
                    }
                }
            }
            else
            {
                aeron_dpdk_unhandled_packet(poller->dpdk_context, pkt_data, pkt_len);
            }

            break;

        default:
            aeron_dpdk_unhandled_packet(poller->dpdk_context, pkt_data, pkt_len);
            break;
    }

    return 0;
}

int aeron_udp_transport_poller_poll(
    aeron_udp_transport_poller_t *poller,
    struct mmsghdr *msgvec,
    size_t vlen,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
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
    }

    return num_pkts;
}

typedef struct sender_poll_ctx_stct
{
    aeron_udp_transport_poller_t* poller;
    aeron_udp_transport_recv_func_t recv_func;
    void* clientd;
}
sender_poll_ctx_t;

void handle_sender_udp_inbound(int32_t type, const void* data, size_t data_len, void* clientd)
{
    sender_poll_ctx_t* sender_poll_ctx = clientd;

    process_ethernet_packet(
        sender_poll_ctx->poller,
        data, (uint32_t) data_len,
        sender_poll_ctx->recv_func, sender_poll_ctx->clientd);
}

void handle_receiver_udp_outbound(int32_t type, const void* data, size_t data_len, void* clientd)
{
    aeron_dpdk_send_ipv4((aeron_dpdk_t*) clientd, data, (uint16_t) data_len);
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
    aeron_spsc_rb_t* sender_udp_recv_q = aeron_dpdk_get_sender_udp_recv_q(poller->dpdk_context);

    sender_poll_ctx_t poll_ctx =
    {
        .poller = poller,
        .recv_func = recv_func,
        .clientd = clientd
    };

    work_done += aeron_spsc_rb_read(sender_udp_recv_q, handle_sender_udp_inbound, &poll_ctx, 100);

    // deal with receiver messages that need to be sent
    aeron_spsc_rb_t* receiver_udp_send_q = aeron_dpdk_get_receiver_udp_send_q(poller->dpdk_context);
    work_done += aeron_spsc_rb_read(receiver_udp_send_q, handle_receiver_udp_outbound, poller->dpdk_context, 100);

    return (int) work_done;
}