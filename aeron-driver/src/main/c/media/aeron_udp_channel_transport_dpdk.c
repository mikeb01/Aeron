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
#include <sys/socket.h>
#include <string.h>
#include <net/if.h>
#include <fcntl.h>
#include <netinet/ip.h>
#include <errno.h>
#include <rte_mbuf.h>
#include <rte_ethdev.h>
#include <rte_ip.h>
#include <rte_udp.h>
#include "util/aeron_error.h"
#include "util/aeron_netutil.h"
#include "aeron_driver_context.h"
#include "aeron_udp_channel_transport.h"

#ifdef USE_DPDK
#include "media/dpdk/aeron_dpdk_messaging.h"
#endif

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

int aeron_udp_channel_transport_init(
    aeron_udp_channel_transport_t *transport,
    aeron_driver_context_t *driver_context,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf)
{
    bool is_ipv6, is_multicast;

    transport->fd = -1;

    is_ipv6 = (AF_INET6 == bind_addr->ss_family);
    is_multicast = aeron_is_addr_multicast(bind_addr);

    if (is_multicast)
    {
        aeron_set_err(EINVAL, "multicast not supported on dpdk (yet): %s", strerror(EINVAL));
        return -1;
    }
    else if (is_ipv6)
    {
        aeron_set_err(EINVAL, "ipv6 not supported on dpdk (yet): %s", strerror(EINVAL));
        return -1;
    }
    else
    {
        memcpy(&transport->bind_addr, bind_addr, sizeof(struct sockaddr_in));
        transport->fd = 0;
    }

    transport->aeron_dpdk = driver_context->dpdk_context;

    return transport->fd;
}

int aeron_udp_channel_transport_close(aeron_udp_channel_transport_t *transport)
{
    return 0;
}

int aeron_udp_channel_transport_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
    return 0;
}

static int sendmsg_rb(
    aeron_spsc_rb_t* loopback_q,
    struct msghdr* message,
    struct sockaddr_storage* src_addr,
    size_t src_addr_len)
{
    // Copy the message and fake it to look like it was received over a loopback.
    struct msghdr output_message;
    memcpy(&output_message, message, sizeof(output_message));
    // I.e. the msg_name when it get received should match the bind address of the
    // publication's transport.
    output_message.msg_controllen = src_addr_len;
    output_message.msg_control = src_addr;

    aeron_rb_write_result_t retval = aeron_dpdk_write_sendmsg_rb(loopback_q, &output_message);

    if (0 != retval)
    {
        return -1;
    }

    return 0;
}

static int sendmmsg_rb(
    aeron_spsc_rb_t* loopback_q,
    struct mmsghdr* msgvec,
    size_t vlen,
    struct sockaddr_storage* src_addr,
    size_t src_addr_len)
{
    for (size_t i = 0; i < vlen; i++)
    {
        struct msghdr* message = &msgvec[i].msg_hdr;

        if (-1 == sendmsg_rb(loopback_q, message, src_addr, src_addr_len))
        {
            return -1;
        }
    }

    return 0;
}

int aeron_udp_channel_transport_sendmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen)
{
    struct sockaddr_storage* destination = (struct sockaddr_storage*)msgvec[0].msg_hdr.msg_name;

    if (destination->ss_family != AF_INET)
    {
        // Only IPV4 supported.
        return -1;
    }
    else
    {
        struct in_addr dest_addr = ((struct sockaddr_in*) destination)->sin_addr;
        DPDK_DEBUG("sending mmsg to: %s\n", inet_ntoa(dest_addr));
        if (aeron_dpdk_is_local_addr(transport->aeron_dpdk, &dest_addr))
        {
            return sendmmsg_rb(
                aeron_dpdk_get_recv_loopback(transport->aeron_dpdk),
                msgvec, vlen,
                &transport->bind_addr, sizeof(struct sockaddr_in));
        }
        else
        {
            return aeron_dpdk_sendmmsg(
                transport->aeron_dpdk, (const struct sockaddr_in*) &transport->bind_addr, msgvec, vlen);
        }
    }
}

int aeron_udp_channel_transport_sendmsg(
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message)
{
    struct sockaddr_storage* destination = (struct sockaddr_storage*)message->msg_name;

    if (destination->ss_family != AF_INET)
    {
        // Only IPV4 supported.
        return -1;
    }
    else
    {
        struct sockaddr_in* const sockaddr_in = (struct sockaddr_in*) destination;
        struct in_addr dest_addr = sockaddr_in->sin_addr;
        if (aeron_dpdk_is_local_addr(transport->aeron_dpdk, &dest_addr))
        {
            DPDK_DEBUG("Sending via recv loopback to: %s:%d\n", inet_ntoa(dest_addr), ntohs(sockaddr_in->sin_port));
            return sendmsg_rb(
                aeron_dpdk_get_recv_loopback(transport->aeron_dpdk),
                message,
                &transport->bind_addr, sizeof(struct sockaddr_in));
        }
        else
        {
            DPDK_DEBUG("Sending via dpdk to: %s:%d\n", inet_ntoa(dest_addr), ntohs(sockaddr_in->sin_port));
            return aeron_dpdk_sendmsg(transport->aeron_dpdk, (const struct sockaddr_in*) &transport->bind_addr, message);
        }
    }
}

int aeron_udp_channel_transport_sendmsg_for_receiver(
    aeron_udp_channel_transport_t* transport,
    struct msghdr* message)
{
    struct sockaddr_in* in_addr = (struct sockaddr_in*) message->msg_name;
    DPDK_DEBUG("Sending msg for receiver: %s:%d\n", inet_ntoa(in_addr->sin_addr), ntohs(in_addr->sin_port));

    // Defensive copy to modify control...
    struct msghdr output_message;
    memcpy(&output_message, message, sizeof(output_message));
    output_message.msg_control = &transport->bind_addr;
    output_message.msg_controllen = sizeof(struct sockaddr_in);

    aeron_spsc_rb_t* sender_udp_recv_q = aeron_dpdk_get_sender_udp_recv_q(transport->aeron_dpdk);

    return aeron_dpdk_write_sendmsg_rb(sender_udp_recv_q, &output_message);
}

int aeron_udp_channel_transport_get_so_rcvbuf(aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf)
{
    // Just making something up as dpdk doesn't have socket buffers.
    *so_rcvbuf = 1 << 19;

    return 0;
}
