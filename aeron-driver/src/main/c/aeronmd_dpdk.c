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

#include <stdlib.h>
#include <signal.h>
#include <time.h>
#include <stdbool.h>
#include <stdio.h>
#include <rte_eal.h>
#include <rte_debug.h>
#include <rte_ethdev.h>

#include "aeron_driver_context.h"
#include "concurrent/aeron_atomic.h"

volatile bool running = true;

void sigint_handler(int signal)
{
    AERON_PUT_ORDERED(running, false);
}

inline bool is_running()
{
    bool result;
    AERON_GET_VOLATILE(result, running);
    return result;
}

int aeron_dpdk_init()
{
    // TODO: Make all of this configurable
    uint16_t num_rxd = 1024;
    uint16_t num_txd = 1024;

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

int main(int argc, char **argv)
{
    int status = EXIT_FAILURE;

    int rc = rte_eal_init(argc, argv);
    if (rc < 0)
        rte_panic("Cannot init EAL\n");


    aeron_driver_context_t *context = NULL;
    aeron_driver_t *driver = NULL;

    signal(SIGINT, sigint_handler);

    if (aeron_dpdk_init() < 0)
    {
        goto cleanup;
    }

    if (aeron_driver_context_init(&context) < 0)
    {
        fprintf(stderr, "ERROR: context init (%d) %s\n", aeron_errcode(), aeron_errmsg());
        goto cleanup;
    }

    if (aeron_driver_init(&driver, context) < 0)
    {
        fprintf(stderr, "ERROR: driver init (%d) %s\n", aeron_errcode(), aeron_errmsg());
        goto cleanup;
    }

    if (aeron_driver_start(driver, true) < 0)
    {
        fprintf(stderr, "ERROR: driver start (%d) %s\n", aeron_errcode(), aeron_errmsg());
        goto cleanup;
    }

    while (is_running())
    {
        aeron_driver_main_idle_strategy(driver, aeron_driver_main_do_work(driver));
    }

    printf("Shutting down driver...\n");

    cleanup:

    aeron_driver_close(driver);
    aeron_driver_context_close(context);

    return status;
}

extern bool is_running();
