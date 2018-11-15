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
    }
    else if (is_ipv6)
    {
        aeron_set_err(EINVAL, "ipv6 not supported on dpdk (yet): %s", strerror(EINVAL));
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

int aeron_udp_channel_transport_sendmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen)
{
    return aeron_dpdk_sendmmsg(transport->aeron_dpdk, (const struct sockaddr_in*) &transport->bind_addr, msgvec, vlen);
}

int aeron_udp_channel_transport_sendmsg(
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message)
{
    return aeron_dpdk_sendmsg(transport->aeron_dpdk, (const struct sockaddr_in*) &transport->bind_addr, message);
}

int aeron_udp_channel_transport_sendmsg_for_receiver(
    aeron_udp_channel_transport_t* transport,
    struct msghdr* message)
{
    return aeron_dpdk_sendmsg_for_receiver(
        transport->aeron_dpdk, (const struct sockaddr_in*) &transport->bind_addr, message);

}

int aeron_udp_channel_transport_get_so_rcvbuf(aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf)
{
    return 0;
}
