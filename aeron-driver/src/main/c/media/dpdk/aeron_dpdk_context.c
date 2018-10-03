//
// Created by barkerm on 3/10/18.
//

#include <stdint.h>

#include <rte_eal.h>
#include <rte_debug.h>
#include <rte_ethdev.h>
#include <rte_malloc.h>


#include "aeron_dpdk_context.h"

void aeron_dpdk_init_eal(int argc, char** argv)
{
    if (rte_eal_init(argc, argv) < 0)
    {
        rte_panic("Cannot init EAL\n");
    }

}

struct aeron_dpdk_context_stct
{
    uint16_t port_id;
    struct rte_mempool* mbuf_pool;
    uint32_t local_ipv4_address;
    uint16_t subnet_mask;
};

int aeron_dpdk_init(aeron_dpdk_context_t** context)
{
    // TODO: Make all of this configurable
    uint16_t num_rxd = 1024;
    uint16_t num_txd = 1024;

    *context = (aeron_dpdk_context_t*) rte_zmalloc("aeron_dpdk_context", sizeof(aeron_dpdk_context_t), 0);

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

    return 0;
}
