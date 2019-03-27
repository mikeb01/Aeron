//
// Created by barkerm on 3/10/18.
//

#define _GNU_SOURCE

#include <stdint.h>
#include <sys/socket.h>

#include <rte_eal.h>
#include <rte_debug.h>
#include <rte_ethdev.h>
#include <rte_udp.h>
#include <rte_malloc.h>
#include <netinet/in.h>
#include <rte_ip.h>
#include <aeron_alloc.h>
#include <rte_arp.h>
#include <arpa/inet.h>

#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "concurrent/aeron_spsc_rb.h"

#include "aeron_dpdk.h"

void aeron_dpdk_init_eal(int argc, char** argv)
{
    if (rte_eal_init(argc, argv) < 0)
    {
        rte_panic("Cannot init EAL\n");
    }
}

typedef struct aeron_dpdk_arp_table_entry_stct
{
    struct in_addr ip_address;
    struct ether_addr ethernet_address;
    int64_t last_query_timestamp_ms;
    bool resolved;
} aeron_dpdk_arp_table_entry_t;

struct aeron_dpdk_stct
{
    uint16_t port_id;
    struct rte_mempool* mbuf_pool;
    struct in_addr local_ipv4_address;
    uint16_t subnet_mask;
    aeron_spsc_rb_t sender_udp_recv_q;
    aeron_spsc_rb_t receiver_udp_send_q;
    aeron_spsc_rb_t loopback_udp_q;

    struct aeron_dpdk_arp_stct
    {
        aeron_spsc_rb_t recv_q;
        aeron_int64_to_ptr_hash_map_t index;
        aeron_dpdk_arp_table_entry_t* table;
        size_t table_len;
        size_t table_cap;
    }
    arp;
};


// TODO: This is bad, should use the shared reference to epoch clock.
static int64_t epoch_clock()
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) < 0)
    {
        return -1;
    }

    return (ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}


static int alloc_rb(aeron_spsc_rb_t* rb, size_t buffer_size)
{
    void* arp_buffer;
    size_t rb_length = aeron_spsc_rb_calculate_length(buffer_size);

    if (aeron_alloc(&arp_buffer, rb_length) < 0)
    {
        return -1;
    }

    return aeron_spsc_rb_init(rb, arp_buffer, rb_length);
}

int aeron_dpdk_init(aeron_dpdk_t** context)
{
    // TODO: Make all of this configurable
    uint16_t num_rxd = 1024;
    uint16_t num_txd = 1024;

    aeron_dpdk_t* _context = (aeron_dpdk_t*) rte_zmalloc("aeron_dpdk_context", sizeof(aeron_dpdk_t), 0);

    if (NULL == _context)
    {
        fprintf(stderr, "FATAL: Failed to allocate context\n");
        return -1;
    }

    char* value = NULL;

    if ((value = getenv(AERON_DPDK_LOCAL_ADDRESS_ENV_VAR)))
    {
        if (0 == inet_aton(value, &_context->local_ipv4_address))
        {
            fprintf(stderr, "FATAL: unable to parse local address: %s\n", value);
            abort();
        }
    }
    else
    {
        fprintf(stderr, "FATAL: No %s specified\n", AERON_DPDK_LOCAL_ADDRESS_ENV_VAR);
        abort();
    }

    struct rte_mempool* mbuf_pool = rte_pktmbuf_pool_create(
        "MBUF_POOL", 8191 * 1, 250, 0, RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());

    if (NULL == mbuf_pool)
    {
        fprintf(stderr, "FATAL: Unable to allocate DPDK memory pool: %s\n", rte_strerror(rte_errno));
        return -1;
    }

    _context->mbuf_pool = mbuf_pool;

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

    rc = alloc_rb(&(_context)->arp.recv_q, 1 << 20);
    if (rc < 0)
    {
        fprintf(stderr, "Unable to allocate ring buffer for arp messages: %d\n", rc);
        abort();
    }
    aeron_int64_to_ptr_hash_map_init(&(_context)->arp.index, 16, 0.6f);

    (_context)->arp.table = calloc(16, sizeof(aeron_dpdk_arp_table_entry_t));
    (_context)->arp.table_len = 0;
    (_context)->arp.table_cap = 16;

    rc = alloc_rb(&(_context)->sender_udp_recv_q, 1 << 20);
    if (rc < 0)
    {
        fprintf(stderr, "Unable to allocate ring buffer for messages inbound for sender\n");
        return -1;
    }

    rc = alloc_rb(&(_context)->receiver_udp_send_q, 1 << 20);
    if (rc < 0)
    {
        fprintf(stderr, "Unable to allocate ring buffer for message outbound for receiver\n");
        return -1;
    }

    rc = alloc_rb(&(_context)->loopback_udp_q, 1 << 20);
    if (rc < 0)
    {
        fprintf(stderr, "Unable to allocate ring buffer for loopback from sender to receiver\n");
        return -1;
    }

    *context = _context;

    return 0;
}

uint16_t aeron_dpdk_get_port_id(aeron_dpdk_t* context)
{
    return context->port_id;
}

struct in_addr aeron_dpdk_get_local_addr(const aeron_dpdk_t* context)
{
    return context->local_ipv4_address;
}


struct rte_mempool* aeron_dpdk_get_mempool(aeron_dpdk_t* context)
{
    return context->mbuf_pool;
}

aeron_spsc_rb_t* aeron_dpdk_get_sender_udp_recv_q(aeron_dpdk_t* aeron_dpdk)
{
    return &aeron_dpdk->sender_udp_recv_q;
}

aeron_spsc_rb_t* aeron_dpdk_get_receiver_udp_send_q(aeron_dpdk_t* aeron_dpdk)
{
    return &aeron_dpdk->receiver_udp_send_q;
}

aeron_spsc_rb_t* aeron_dpdk_get_recv_loopback(aeron_dpdk_t* aeron_dpdk)
{
    return &aeron_dpdk->loopback_udp_q;
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

static const struct ether_addr ether_zero = {
    .addr_bytes = {0, 0, 0, 0, 0, 0}
};

void send_arp_message(
    aeron_dpdk_t* aeron_dpdk,
    uint16_t arp_op,
    struct ether_addr dest_eth_addr,
    struct ether_addr target_eth_addr,
    uint32_t target_ip_addr)
{
    struct rte_mbuf* arp_pkt = rte_pktmbuf_alloc(aeron_dpdk->mbuf_pool);
    const size_t pkt_size = sizeof(struct ether_hdr) + sizeof(struct arp_hdr);

    arp_pkt->data_len = pkt_size;
    arp_pkt->pkt_len = pkt_size;

    struct ether_hdr* arp_eth = rte_pktmbuf_mtod(arp_pkt, struct ether_hdr*);
    rte_eth_macaddr_get(aeron_dpdk->port_id, &arp_eth->s_addr);  // Should we cache this
    arp_eth->d_addr = dest_eth_addr;
    arp_eth->ether_type = rte_cpu_to_be_16(ETHER_TYPE_ARP);

    struct arp_hdr* arp_msg = (struct arp_hdr*) (rte_pktmbuf_mtod(arp_pkt, char*) + sizeof(struct ether_hdr));
    arp_msg->arp_hrd = rte_cpu_to_be_16(ARP_HRD_ETHER);
    arp_msg->arp_pro = rte_cpu_to_be_16(0x0800);
    arp_msg->arp_hln = 6;
    arp_msg->arp_pln = 4;
    arp_msg->arp_op = rte_cpu_to_be_16(arp_op);
    arp_msg->arp_data.arp_sha = arp_eth->s_addr;
    arp_msg->arp_data.arp_sip = aeron_dpdk->local_ipv4_address.s_addr; // TODO: check byte ordering!!
    arp_msg->arp_data.arp_tha = target_eth_addr;
    arp_msg->arp_data.arp_tip = target_ip_addr;

    const uint16_t sent = rte_eth_tx_burst(aeron_dpdk->port_id, 0, &arp_pkt, 1);
    printf("Sent ARP request: %d\n", sent);

    rte_pktmbuf_free(arp_pkt);
}

static void arp_table_put(aeron_int64_to_ptr_hash_map_t* arp_table, uint32_t ip, struct ether_addr addr)
{
    aeron_dpdk_arp_table_entry_t* entry = aeron_int64_to_ptr_hash_map_get(arp_table, ip);

    if (NULL != entry)
    {
        entry->ethernet_address = addr;
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
            arp_table_put(&aeron_dpdk->arp.index, ip, addr);

            if (arp_hdr->arp_op == ARP_OP_REQUEST &&
                arp_hdr->arp_data.arp_tip == aeron_dpdk->local_ipv4_address.s_addr &&
                is_zero(arp_hdr->arp_data.arp_tha))
            {
                send_arp_message(
                    aeron_dpdk, ARP_OP_REPLY, arp_hdr->arp_data.arp_sha,
                    arp_hdr->arp_data.arp_sha, arp_hdr->arp_data.arp_sip);
            }
        }
    }
}

size_t aeron_dpdk_handle_other_protocols(aeron_dpdk_t* aeron_dpdk)
{
    return aeron_spsc_rb_read(&aeron_dpdk->arp.recv_q, handle_arp_msg, aeron_dpdk, 100);
}

struct ether_addr* aeron_dpdk_arp_lookup(aeron_dpdk_t* aeron_dpdk, uint32_t addr_in)
{
    aeron_dpdk_arp_table_entry_t* arp_table_entry =
        (aeron_dpdk_arp_table_entry_t*) aeron_int64_to_ptr_hash_map_get(&aeron_dpdk->arp.index, addr_in);

    if (NULL == arp_table_entry || !arp_table_entry->resolved)
    {
        return NULL;
    }

    return &arp_table_entry->ethernet_address;
}

void aeron_dpdk_arp_submit_query(aeron_dpdk_t* aeron_dpdk, uint32_t addr_in)
{
    aeron_dpdk_arp_table_entry_t* arp_table_entry =
        (aeron_dpdk_arp_table_entry_t*) aeron_int64_to_ptr_hash_map_get(&aeron_dpdk->arp.index, addr_in);

    if (NULL == arp_table_entry)
    {
        arp_table_entry = calloc(1, sizeof(aeron_dpdk_arp_table_entry_t));

        if (NULL == arp_table_entry)
        {
            DPDK_DEBUG("Failed to allocate arp table entry for: %d\n", addr_in);
            // Error
            return;
        }

        arp_table_entry->ip_address.s_addr = addr_in;
    }

    const int64_t now = epoch_clock();

    const bool sufficient_time_between_requests = now - arp_table_entry->last_query_timestamp_ms > 2000;

    struct in_addr addr;
    addr.s_addr = addr_in;

    DPDK_DEBUG(
        "Submitting query for: %s, sufficient_time_between_requests: %d\n",
        inet_ntoa(addr), sufficient_time_between_requests);

    if (sufficient_time_between_requests)
    {
        arp_table_entry->last_query_timestamp_ms = now;

        send_arp_message(aeron_dpdk, ARP_OP_REQUEST, ether_broadcast, ether_zero, addr_in);
    }
}

static void set_l2_for_ipv4_udp_pkt(
    struct ether_hdr* dst,
    const struct ether_addr* dst_ether_addr,
    const struct ether_addr* src_ether_addr)
{
    dst->d_addr = (*dst_ether_addr);
    dst->s_addr = (*src_ether_addr);
    dst->ether_type = rte_cpu_to_be_16(ETHER_TYPE_IPv4);
}

static void set_l3_for_ipv4_udp_pkt(
    struct ipv4_hdr* udp_ip,
    const struct sockaddr_in* dst_addr,
    const struct sockaddr_in* src_addr,
    uint16_t ip_total_len)
{
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
}

static void set_l4_for_ipv4_udp_pkt(
    struct udp_hdr* udp_udp,
    const struct sockaddr_in* dst_addr,
    const struct sockaddr_in* src_addr,
    size_t src_data_len,
    uint16_t checksum)
{
    udp_udp->src_port = src_addr->sin_port;
    udp_udp->dst_port = dst_addr->sin_port;
    udp_udp->dgram_len = rte_cpu_to_be_16(sizeof(struct udp_hdr) + src_data_len);
    udp_udp->dgram_cksum = checksum;
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
    struct ether_hdr* udp_eth = dst;
    struct ipv4_hdr* udp_ip = (struct ipv4_hdr*) ((char*) udp_eth + sizeof(struct ether_hdr));
    struct udp_hdr* udp_udp = (struct udp_hdr*) ((char*) udp_ip + sizeof(struct ipv4_hdr));

    set_l2_for_ipv4_udp_pkt(udp_eth, dst_ether_addr, src_ether_addr);

    const uint16_t ip_total_len = sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + src_data_len;
    set_l3_for_ipv4_udp_pkt(udp_ip, dst_addr, src_addr, ip_total_len);

    uint16_t checksum = rte_ipv4_phdr_cksum(udp_ip, PKT_TX_IPV4 | PKT_TX_UDP_CKSUM | PKT_TX_IP_CKSUM);
    set_l4_for_ipv4_udp_pkt(udp_udp, dst_addr, src_addr, src_data_len, checksum);

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

int aeron_dpdk_sendmsg(
    aeron_dpdk_t* aeron_dpdk,
    const struct sockaddr_in* bind_addr,
    const struct msghdr *message)
{
    struct rte_mbuf* buf;
    struct rte_mempool* mempool = aeron_dpdk_get_mempool(aeron_dpdk);
    const uint16_t port_id = aeron_dpdk_get_port_id(aeron_dpdk);
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

    struct ether_addr* dest_ether_addr = aeron_dpdk_arp_lookup(aeron_dpdk, dest_addr->sin_addr.s_addr);
    if (NULL == dest_ether_addr)
    {
        aeron_dpdk_arp_submit_query(aeron_dpdk, dest_addr->sin_addr.s_addr);
        return -1;
    }

    struct ether_addr src_ether_addr;
    rte_eth_macaddr_get(port_id, &src_ether_addr);

    buf = rte_pktmbuf_alloc(mempool);

    set_mbuf(buf, message->msg_iov[0].iov_len);

    void* pkt = rte_pktmbuf_mtod(buf, void*);

    set_ipv4_udp_pkt(
        pkt, dest_addr, dest_ether_addr, bind_addr, &src_ether_addr,
        message->msg_iov[0].iov_base, message->msg_iov[0].iov_len);

    const uint16_t pkts_sent = rte_eth_tx_burst(port_id, 1, &buf, 1);

    return (int) (pkts_sent == 0 ? 0 : message->msg_iov[0].iov_len);
}

int aeron_dpdk_sendmmsg(
    aeron_dpdk_t* aeron_dpdk,
    const struct sockaddr_in* bind_addr,
    const struct mmsghdr *msgvec,
    size_t vlen)
{
    struct rte_mbuf* bufs[32];
    struct rte_mempool* mempool = aeron_dpdk_get_mempool(aeron_dpdk);
    const uint16_t port_id = aeron_dpdk_get_port_id(aeron_dpdk);

    uint16_t messages = (uint16_t) (vlen < 32 ? vlen : 32);

    for (size_t msg_i = 0; msg_i < messages; msg_i++)
    {
        const struct msghdr* msg = &msgvec[msg_i].msg_hdr;
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

        struct ether_addr* dest_ether_addr = aeron_dpdk_arp_lookup(aeron_dpdk, dest_addr->sin_addr.s_addr);
        if (NULL == dest_ether_addr)
        {
            aeron_dpdk_arp_submit_query(aeron_dpdk, dest_addr->sin_addr.s_addr);
            return -1;
        }

        struct ether_addr src_ether_addr;
        rte_eth_macaddr_get(port_id, &src_ether_addr);

        bufs[msg_i] = rte_pktmbuf_alloc(mempool);

        set_mbuf(bufs[msg_i], msg->msg_iov[0].iov_len);

        void* pkt = rte_pktmbuf_mtod(bufs[msg_i], void*);

        set_ipv4_udp_pkt(
            pkt, dest_addr, dest_ether_addr, bind_addr, &src_ether_addr,
            msg->msg_iov[0].iov_base, msg->msg_iov[0].iov_len);
    }

    const uint16_t pkts_sent = rte_eth_tx_burst(port_id, 1, bufs, messages);

    return pkts_sent;
}

int aeron_dpdk_sendmsg_for_receiver(
    aeron_dpdk_t* aeron_dpdk,
    const struct sockaddr_in* bind_addr,
    const struct msghdr *message)
{
    const struct sockaddr_in* dst_addr = message->msg_name;
    const size_t src_data_len = message->msg_iov[0].iov_len;

    const uint8_t internal_msg[8192]; // TODO: this will need to be smaller than network MTU, but 8K should be enough.

    // Aeron only uses iov len 1.
    if (message->msg_iovlen > 1)
    {
        return -EINVAL;
    }

    if (AF_INET != dst_addr->sin_family)
    {
        // We only support IPv4 ATM.
        return -1;
    }

    const uint16_t ip_total_len = sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr) + src_data_len;

    struct ipv4_hdr* udp_ip = (struct ipv4_hdr*) ((char*) internal_msg + sizeof(struct ether_hdr));
    set_l3_for_ipv4_udp_pkt(udp_ip, dst_addr, bind_addr, ip_total_len);

    uint16_t checksum = rte_ipv4_phdr_cksum(udp_ip, PKT_TX_IPV4 | PKT_TX_UDP_CKSUM | PKT_TX_IP_CKSUM);
    struct udp_hdr* udp_udp = (struct udp_hdr*) ((char*) udp_ip + sizeof(struct ipv4_hdr));
    set_l4_for_ipv4_udp_pkt(udp_udp, dst_addr, bind_addr, src_data_len, checksum);

    char* udp_payload = (char*) udp_udp  + sizeof(struct udp_hdr);
    rte_memcpy(udp_payload, message->msg_iov[0].iov_base, src_data_len);

    const aeron_rb_write_result_t result = aeron_spsc_rb_write(&aeron_dpdk->receiver_udp_send_q, 0, internal_msg, ip_total_len);

    return (AERON_RB_SUCCESS == result ? (int) src_data_len : result);
}

int aeron_dpdk_send_ipv4(aeron_dpdk_t* aeron_dpdk, const char* packet, uint16_t ip_total_len)
{
    struct rte_mbuf* buf;
    struct rte_mempool* mempool = aeron_dpdk_get_mempool(aeron_dpdk);
    const uint16_t port_id = aeron_dpdk_get_port_id(aeron_dpdk);

    buf = rte_pktmbuf_alloc(mempool);
    struct ether_hdr* pkt = rte_pktmbuf_mtod(buf, struct ether_hdr*);

    set_mbuf(buf, ip_total_len - (sizeof(struct ipv4_hdr) + sizeof(struct udp_hdr)));

    struct ipv4_hdr* ipv4_hdr_src = (struct ipv4_hdr*) packet;

    struct ether_addr* dest_ether_addr = aeron_dpdk_arp_lookup(aeron_dpdk, ipv4_hdr_src->dst_addr);

    if (NULL == dest_ether_addr)
    {
        aeron_dpdk_arp_submit_query(aeron_dpdk, ipv4_hdr_src->dst_addr);
        return -1;
    }

    struct ether_addr src_ether_addr;
    rte_eth_macaddr_get(port_id, &src_ether_addr);

    set_l2_for_ipv4_udp_pkt(pkt, dest_ether_addr, &src_ether_addr);

    struct ipv4_hdr* ipv4_hdr_dst = (struct ipv4_hdr*) (char *) pkt + sizeof(struct ether_hdr);

    rte_memcpy(ipv4_hdr_dst, ipv4_hdr_src, ip_total_len);

    rte_eth_tx_burst(port_id, 0, &buf, 1);

    return 1;

}