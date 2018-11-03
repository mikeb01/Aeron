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

static void set_ipv4_udp_pkt(
    void* dst,
    const struct sockaddr_in* dst_addr,
    const struct ether_addr* dst_ether_addr,
    const struct sockaddr_in* src_addr,
    const struct ether_addr* src_ether_addr,
    const char* src_data,
    size_t src_data_len)
{
    const uint16_t ip_total_len = sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + src_data_len;

    struct ether_hdr* udp_eth = dst;

    udp_eth->d_addr = (*dst_ether_addr);
    udp_eth->s_addr = (*src_ether_addr);
    udp_eth->ether_type = rte_cpu_to_be_16(ETHER_TYPE_IPv4);

    struct ipv4_hdr* udp_ip = (struct ipv4_hdr*) ((char*) udp_eth + sizeof(struct ether_hdr));

    udp_ip->version_ihl = 0x45;
    udp_ip->type_of_service = 0;
    udp_ip->total_length = rte_cpu_to_be_16(ip_total_len);
    udp_ip->packet_id = rte_cpu_to_be_16(1);
    udp_ip->fragment_offset = rte_cpu_to_be_16(0);
    udp_ip->time_to_live = 64;
    udp_ip->next_proto_id = 17;
    udp_ip->hdr_checksum = rte_cpu_to_be_16(0);
    udp_ip->src_addr = src_addr->sin_addr.s_addr;
    udp_ip->dst_addr = dst_addr->sin_addr.s_addr;

    struct udp_hdr* udp_udp = (struct udp_hdr*) ((char*) udp_ip + sizeof(struct ipv4_hdr));

    udp_udp->src_port = src_addr->sin_port;
    udp_udp->dst_port = dst_addr->sin_port;
    udp_udp->dgram_len = rte_cpu_to_be_16(sizeof(struct udp_hdr) + strlen(src_data));
    udp_udp->dgram_cksum = rte_ipv4_phdr_cksum(udp_ip, PKT_TX_IPV4 | PKT_TX_UDP_CKSUM | PKT_TX_IP_CKSUM);

    char* udp_payload = (char*) udp_udp  + sizeof(struct udp_hdr);

    rte_memcpy(udp_payload, src_data, src_data_len);
}

static void set_mbuf(struct rte_mbuf* udp_pkt, size_t data_len)
{
    const uint16_t ip_total_len = sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + data_len;
    const uint16_t pkt_size = sizeof(struct ether_hdr) + ip_total_len;

    udp_pkt->data_len = pkt_size;
    udp_pkt->pkt_len = pkt_size;
    udp_pkt->l2_len = sizeof(struct ether_hdr);
    udp_pkt->l3_len = sizeof(struct ipv4_hdr);
    udp_pkt->l4_len = sizeof(struct udp_hdr);
    udp_pkt->ol_flags = PKT_TX_IPV4 | PKT_TX_UDP_CKSUM | PKT_TX_IP_CKSUM;
}

int aeron_udp_channel_transport_sendmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen)
{
    struct rte_mbuf* bufs[32];
    struct rte_mempool* mempool = aeron_dpdk_get_mempool(transport->aeron_dpdk);
    const uint16_t port_id = aeron_dpdk_get_port_id(transport->aeron_dpdk);

    uint16_t messages = (uint16_t) (vlen < 32 ? vlen : 32);

    for (size_t msg_i = 0; msg_i < messages; msg_i++)
    {
        struct msghdr* msg = &msgvec[msg_i].msg_hdr;
        const struct sockaddr_in* dest_addr = msg->msg_name;

        // Aeron only uses iov len 1.
        if (msg->msg_iovlen > 1)
        {
            return -EINVAL;
        }

        if (AF_INET != dest_addr->sin_family)
        {
            // We only support IPv4 ATM.
            return -1;
        }


        struct ether_addr* dest_ether_addr = aeron_dpdk_arp_lookup(transport->aeron_dpdk, dest_addr->sin_addr.s_addr);
        struct ether_addr src_ether_addr;
        rte_eth_macaddr_get(port_id, &src_ether_addr);

        if (NULL == dest_ether_addr)
        {
            // TOOD send arp request.
            return -1;
        }

        bufs[msg_i] = rte_pktmbuf_alloc(mempool);

        set_mbuf(bufs[msg_i], msg->msg_iov[0].iov_len);

        void* pkt = rte_pktmbuf_mtod(bufs[msg_i], void*);

        set_ipv4_udp_pkt(
            pkt, dest_addr, dest_ether_addr, (struct sockaddr_in*) &transport->bind_addr, &src_ether_addr,
            msg->msg_iov[0].iov_base, msg->msg_iov[0].iov_len);
    }

    const uint16_t pkts_sent = rte_eth_tx_burst(port_id, 1, bufs, messages);

    return pkts_sent;
}

int aeron_udp_channel_transport_sendmsg(
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message)
{
    struct rte_mbuf* buf;
    struct rte_mempool* mempool = aeron_dpdk_get_mempool(transport->aeron_dpdk);
    const uint16_t port_id = aeron_dpdk_get_port_id(transport->aeron_dpdk);
    const struct sockaddr_in* dest_addr = message->msg_name;

    // Aeron only uses iov len 1.
    if (message->msg_iovlen > 1)
    {
        return -EINVAL;
    }

    if (AF_INET != dest_addr->sin_family)
    {
        // We only support IPv4 ATM.
        return -1;
    }

    struct ether_addr* dest_ether_addr = aeron_dpdk_arp_lookup(transport->aeron_dpdk, dest_addr->sin_addr.s_addr);
    struct ether_addr src_ether_addr;
    rte_eth_macaddr_get(port_id, &src_ether_addr);

    if (NULL == dest_ether_addr)
    {
        // TOOD send arp request.
        return -1;
    }

    buf = rte_pktmbuf_alloc(mempool);

    set_mbuf(buf, message->msg_iov[0].iov_len);

    void* pkt = rte_pktmbuf_mtod(buf, void*);

    set_ipv4_udp_pkt(
        pkt, dest_addr, dest_ether_addr, (struct sockaddr_in*) &transport->bind_addr, &src_ether_addr,
        message->msg_iov[0].iov_base, message->msg_iov[0].iov_len);

    const uint16_t pkts_sent = rte_eth_tx_burst(port_id, 1, &buf, 1);

    return (int) (pkts_sent == 0 ? 0 : message->msg_iov[0].iov_len);
}

int aeron_udp_channel_transport_get_so_rcvbuf(aeron_udp_channel_transport_t *transport, size_t *so_rcvbuf)
{
    return 0;
}
