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

typedef struct poller_client_data_stct
{
    void* clientd;
    aeron_udp_transport_recv_func_t recv_func;
    aeron_udp_transport_poller_t* poller;
}
poller_clientd_t;

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
static aeron_dpdk_handler_result_t handle_received_message(
    struct msghdr* message,
    void* clientd)
{
    int dispatch_count = 0;
    struct sockaddr_in* dst_addr_in = message->msg_name;
    struct sockaddr_in* src_addr_in = message->msg_control;

    poller_clientd_t* const poller_clientd = (poller_clientd_t*) clientd;
    aeron_udp_transport_poller_t* poller = poller_clientd->poller;

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
            poller_clientd->recv_func(
                poller_clientd->clientd, transport->dispatch_clientd,
                message->msg_iov->iov_base, message->msg_iov->iov_len,
                (struct sockaddr_storage*) src_addr_in);

            // TODO: break???
            dispatch_count++;
        }
    }

    return dispatch_count == 0 ? NO_MATCHING_TARGET : PROCESSED;
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

    poller_clientd_t poller_clientd;
    poller_clientd.recv_func = recv_func;
    poller_clientd.poller = poller;
    poller_clientd.clientd = clientd;

    num_pkts = aeron_dpdk_poll_receiver_messages(
        poller->dpdk_context, handle_received_message, &poller_clientd, &total_len);

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
    poller_clientd_t poller_clientd;
    poller_clientd.recv_func = recv_func;
    poller_clientd.poller = poller;
    poller_clientd.clientd = clientd;
    uint32_t total_bytes = 0;

    aeron_dpdk_poll_sender_messages(poller->dpdk_context, handle_received_message, &poller_clientd, &total_bytes);

    return 0;
}