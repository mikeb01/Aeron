//
// Created by barkerm on 3/10/18.
//

#include <stdint.h>

#include <rte_eal.h>
#include <rte_debug.h>
#include <rte_ethdev.h>
#include <rte_udp.h>
#include <rte_malloc.h>
#include <netinet/in.h>
#include <rte_ip.h>
#include <aeron_alloc.h>
#include <rte_arp.h>

#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "concurrent/aeron_spsc_rb.h"

#include "aeron_dpdk_context.h"

void aeron_dpdk_init_eal(int argc, char** argv)
{
    if (rte_eal_init(argc, argv) < 0)
    {
        rte_panic("Cannot init EAL\n");
    }
}

struct aeron_dpdk_stct
{
    uint16_t port_id;
    struct rte_mempool* mbuf_pool;
    uint32_t local_ipv4_address;
    uint16_t subnet_mask;
    aeron_spsc_rb_t sender_udp_recv_q;

    struct aeron_dpdk_arp_stct
    {
        aeron_spsc_rb_t recv_q;
        aeron_int64_to_ptr_hash_map_t table;
    }
    arp;
};

static int alloc_rb(aeron_spsc_rb_t* rb, size_t buffer_size)
{
    void* arp_buffer;
    if (aeron_alloc(&arp_buffer, buffer_size) < 0)
    {
        return -1;
    }

    return aeron_spsc_rb_init(rb, arp_buffer, buffer_size);
}

int aeron_dpdk_init(aeron_dpdk_t** context)
{
    // TODO: Make all of this configurable
    uint16_t num_rxd = 1024;
    uint16_t num_txd = 1024;

    *context = (aeron_dpdk_t*) rte_zmalloc("aeron_dpdk_context", sizeof(aeron_dpdk_t), 0);

    if (NULL == *context)
    {
        fprintf(stderr, "FATAL: Failed to allocate context\n");
        return -1;
    }

    struct rte_mempool* mbuf_pool = rte_pktmbuf_pool_create(
        "MBUF_POOL", 8191 * 1, 250, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());

    if (NULL == mbuf_pool)
    {
        fprintf(stderr, "FATAL: Unable to allocate DPDK memory pool: %s\n", rte_strerror(rte_errno));
        return -1;
    }

    const uint16_t port_id = rte_eth_find_next(0);

    if (port_id >= RTE_MAX_ETHPORTS)
    {
        fprintf(stderr, "FATAL: Unable to find usable DPDK port\n");
        return -1;
    }

    struct rte_eth_dev_info info;
    rte_eth_dev_info_get(port_id, &info);

    struct rte_eth_conf port_conf = {
        .rxmode = {
            .max_rx_pkt_len = 1500,
            .split_hdr_size = 0,
            .offloads = DEV_RX_OFFLOAD_UDP_CKSUM | DEV_RX_OFFLOAD_IPV4_CKSUM,
        },
        .txmode = {
            .mq_mode = ETH_MQ_TX_NONE,
            .offloads = DEV_TX_OFFLOAD_UDP_CKSUM | DEV_TX_OFFLOAD_IPV4_CKSUM ,
        }
    };

    int rc = rte_eth_dev_configure(port_id, 1, 1, &port_conf);
    if (rc < 0)
    {
        fprintf(stderr, "FATAL: Failed to configure DPDK port: %s\n", strerror(-rc));
        return -1;
    }

    rc = rte_eth_dev_adjust_nb_rx_tx_desc(port_id, &num_rxd, &num_txd);
    if (rc < 0)
    {
        fprintf(stderr, "FATAL: Failed to adjust number of rx/tx descriptors: %s\n", strerror(-rc));
        return -1;
    }

    rc = rte_eth_rx_queue_setup(port_id, 0, num_rxd, rte_eth_dev_socket_id(port_id), NULL, mbuf_pool);
    if (rc < 0)
    {
        fprintf(stderr, "FATAL: Failed to set up rx queue: %s\n", strerror(-rc));
        return -1;
    }

    rc = rte_eth_tx_queue_setup(port_id, 0, num_txd, rte_eth_dev_socket_id(port_id), &info.default_txconf);
    if (rc < 0)
    {
        fprintf(stderr, "FATAL: Failed to set up tx queue: %s\n", strerror(-rc));
        return -1;
    }

    rc = rte_eth_dev_start(port_id);
    if (rc < 0)
    {
        fprintf(stderr, "FATAL: Failed to start DPDK port: %d, error:%s\n", port_id, strerror(-rc));
        return -1;
    }

    struct rte_eth_link status;
    rte_eth_link_get(port_id, &status);

    fprintf(
        stderr,
        "INFO: Using DPDK port: %d, driver: %s, rx_qs: %d, tx_qs: %d, speed: %dMbits/s, link: %s\n",
        port_id, info.driver_name, info.max_rx_queues, info.max_tx_queues, status.link_speed, status.link_status == 1 ? "UP" : "DOWN");

    rc = alloc_rb(&(*context)->arp.recv_q, 1 << 20);
    if (rc < 0)
    {
        fprintf(stderr, "Unable to allocate ring buffer for arp messages\n");
        return -1;
    }

    rc = alloc_rb(&(*context)->sender_udp_recv_q, 1 << 20);
    if (rc < 0)
    {
        fprintf(stderr, "Unable to allocate ring buffer for sender messages\n");
        return -1;
    }

    aeron_int64_to_ptr_hash_map_init(&(*context)->arp.table, 16, 0.6f);

    return 0;
}

uint16_t aeron_dpdk_get_port_id(aeron_dpdk_t* context)
{
    return context->port_id;
}

struct rte_mempool* aeron_dpdk_get_mempool(aeron_dpdk_t* context)
{
    return context->mbuf_pool;
}

aeron_spsc_rb_t* aeron_dpdk_get_sender_udp_recv_q(aeron_dpdk_t* aeron_dpdk)
{
    return &aeron_dpdk->sender_udp_recv_q;
}


int aeron_dpdk_unhandled_packet(aeron_dpdk_t* aeron_dpdk, const uint8_t* pkt_data, const uint32_t pkt_len)
{
    struct ether_hdr* eth_hdr = (struct ether_hdr*) pkt_data;
    const uint16_t frame_type = rte_be_to_cpu_16(eth_hdr->ether_type);
    struct ipv4_hdr* ip_hdr;

    int result = 0;

    switch (frame_type)
    {
        case ETHER_TYPE_ARP:
        {
            if (AERON_RB_SUCCESS != aeron_spsc_rb_write(&aeron_dpdk->arp.recv_q, 0, pkt_data, pkt_len))
            {
                result = -1;
            }
            break;
        }

        case ETHER_TYPE_IPv4:
        {
            struct sockaddr_storage msg_name;
            memset(&msg_name, 1, sizeof(msg_name));

            ip_hdr = (struct ipv4_hdr*) ((uint8_t*) eth_hdr + sizeof(struct ether_hdr));

            if (IPPROTO_UDP == ip_hdr->next_proto_id)
            {
                // TODO: check IP bindings to prevent unnecessary messages flowing via this queue.
//                int ipv4_hdr_len = (ip_hdr->version_ihl & IPV4_HDR_IHL_MASK) * IPV4_IHL_MULTIPLIER;
//                struct udp_hdr* udp_hdr = (struct udp_hdr*) ((uint8_t*) ip_hdr + ipv4_hdr_len);
//                uint8_t* msg_data = ((uint8_t*) udp_hdr) + sizeof(struct udp_hdr);
//                const size_t msg_len = ip_hdr->total_length - (sizeof(struct udp_hdr) + ipv4_hdr_len);
//
//                struct sockaddr_in* in_addr = (struct sockaddr_in*) &msg_name;
//                in_addr->sin_family = AF_INET;
//                in_addr->sin_port = udp_hdr->src_port;
//                in_addr->sin_addr.s_addr = ip_hdr->src_addr;

                aeron_spsc_rb_write(&aeron_dpdk->sender_udp_recv_q, 0, pkt_data, pkt_len);
            }
            else if (IPPROTO_IGMP)
            {
                // TODO: handle igmp protocol
            }

            break;
        }

        default:
            // Ignore
            break;
    }

    return result;
}

static bool is_zero(struct ether_addr addr)
{
    return (0 == addr.addr_bytes[0])
        && (0 == addr.addr_bytes[1])
        && (0 == addr.addr_bytes[2])
        && (0 == addr.addr_bytes[3])
        && (0 == addr.addr_bytes[4])
        && (0 == addr.addr_bytes[5]);
}

static const struct ether_addr ether_broadcast = {
    .addr_bytes = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
};


void send_arp_message(
    aeron_dpdk_t* aeron_dpdk,
    uint16_t arp_op,
    struct ether_addr dest_eth_addr,
    uint32_t dest_ip_addr)
{
    struct rte_mbuf* arp_pkt = rte_pktmbuf_alloc(aeron_dpdk->mbuf_pool);
    const size_t pkt_size = sizeof(struct ether_hdr) + sizeof(struct arp_hdr);

    arp_pkt->data_len = pkt_size;
    arp_pkt->pkt_len = pkt_size;

    struct ether_hdr* arp_eth = rte_pktmbuf_mtod(arp_pkt, struct ether_hdr*);
    rte_eth_macaddr_get(aeron_dpdk->port_id, &arp_eth->s_addr);  // Should we cache this
    arp_eth->d_addr = ether_broadcast;
    arp_eth->ether_type = rte_cpu_to_be_16(ETHER_TYPE_ARP);

    struct arp_hdr* arp_msg = (struct arp_hdr*) (rte_pktmbuf_mtod(arp_pkt, char*) + sizeof(struct ether_hdr));
    arp_msg->arp_hrd = rte_cpu_to_be_16(ARP_HRD_ETHER);
    arp_msg->arp_pro = rte_cpu_to_be_16(0x0800);
    arp_msg->arp_hln = 6;
    arp_msg->arp_pln = 4;
    arp_msg->arp_op = rte_cpu_to_be_16(arp_op);
    arp_msg->arp_data.arp_sha = arp_eth->s_addr;
    arp_msg->arp_data.arp_sip = aeron_dpdk->local_ipv4_address; // TODO: check byte ordering!!
    arp_msg->arp_data.arp_tha = dest_eth_addr;
    arp_msg->arp_data.arp_tip = dest_ip_addr;

    const uint16_t sent = rte_eth_tx_burst(aeron_dpdk->port_id, 0, &arp_pkt, 1);
    printf("Sent ARP request: %d\n", sent);

    rte_pktmbuf_free(arp_pkt);
}

static void arp_table_put(aeron_int64_to_ptr_hash_map_t* arp_table, uint32_t ip, struct ether_addr addr)
{
    struct ether_addr* allocated_copy;
    if (0 == aeron_alloc((void**) &allocated_copy, sizeof(struct ether_addr)))
    {
        memcpy(allocated_copy, &addr, sizeof(addr));
        aeron_int64_to_ptr_hash_map_put(arp_table, ip, allocated_copy);
    }
}

static void handle_arp_msg(int32_t type, const void* data, size_t len, void* clientd)
{
    struct ether_hdr* eth_hdr = (struct ether_hdr*) data;
    aeron_dpdk_t* aeron_dpdk = clientd;

    if (ETHER_TYPE_ARP == eth_hdr->ether_type)
    {
        // We assume that aeron will only queue messages of a valid length.

        struct arp_hdr* arp_hdr = (struct arp_hdr*) ((char*) eth_hdr + sizeof(struct ether_hdr));
        const uint16_t hw_type = rte_be_to_cpu_16(arp_hdr->arp_hrd);
        if (ARP_HRD_ETHER == hw_type)
        {
            uint32_t ip = arp_hdr->arp_data.arp_sip;
            struct ether_addr addr = arp_hdr->arp_data.arp_sha;
            arp_table_put(&aeron_dpdk->arp.table, ip, addr);

            if (arp_hdr->arp_op == ARP_OP_REQUEST &&
                arp_hdr->arp_data.arp_tip == aeron_dpdk->local_ipv4_address &&
                is_zero(arp_hdr->arp_data.arp_tha))
            {
                send_arp_message(aeron_dpdk, ARP_OP_REPLY, arp_hdr->arp_data.arp_sha, arp_hdr->arp_data.arp_sip);
            }
        }
    }
}

size_t aeron_dpdk_handle_other_protocols(aeron_dpdk_t* aeron_dpdk)
{
    return aeron_spsc_rb_read(&aeron_dpdk->arp.recv_q, handle_arp_msg, aeron_dpdk, 100);
}
