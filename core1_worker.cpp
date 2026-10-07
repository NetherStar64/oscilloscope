#include <stdio.h>
#include "pico/stdlib.h"
#include "pico/cyw43_arch.h"
#include "pico/multicore.h"
#include "config.h"
#include "wifipassword.h"

extern uint16_t sample_buffers[NUM_RING_BUFFERS][SAMPLE_BUFFER_SIZE];
uint32_t sample_count;

struct __attribute__((packed)) udpsample {
    uint32_t sample_count;
    u8_t sample_part;
    uint8_t padding[3];
    uint16_t data[SAMPLE_BUFFER_SPLIT];
};

void wifi_worker() {
    printf("hiii mrow\n");

    u8_t sample_buffer_index;
    cyw43_arch_lwip_begin();
    auto udp_conn = udp_new();
    ip_addr_t laptop;
    if (ipaddr_aton(VIEWER_IP, &laptop) == 0) {
        panic("ipaddr_aton failed");
    }
    s8_t conn_status = udp_connect(udp_conn, &laptop, VIEWER_PORT);
    if (conn_status != 0) {
        panic("Failed connect: %d", conn_status);
    }

    printf("Connected to UDP socket\n");
    cyw43_arch_lwip_end();

    static uint16_t sample_buffer_copy[SAMPLE_BUFFER_SIZE];
    while (true) {
        // Wait for new data
        sample_buffer_index = multicore_fifo_pop_blocking();
        // Memcopy for no tear
        memcpy(
            sample_buffer_copy,
            sample_buffers[sample_buffer_index], 
            sizeof(sample_buffer_copy));
            
        const u8_t num_packets = SAMPLE_BUFFER_SIZE / SAMPLE_BUFFER_SPLIT;

        for (u8_t i = 0; i<num_packets; i++) {
            cyw43_arch_lwip_begin();
            struct pbuf *p = pbuf_alloc(PBUF_TRANSPORT, sizeof(udpsample), PBUF_RAM);
            if (p==NULL) {
                // alloc failed grr
                break;
            }
            auto *pkt = static_cast<udpsample*>(p->payload);
            pkt->sample_count = sample_count;
            pkt->sample_part  = i;
            memcpy(
                pkt->data, 
                &sample_buffer_copy[i * SAMPLE_BUFFER_SPLIT], 
                SAMPLE_BUFFER_SPLIT * sizeof(uint16_t)
            );

            s8_t senderr = udp_send(udp_conn, p);
            pbuf_free(p);
            if (senderr == ERR_OK || senderr == ERR_MEM) {
                // We good
            } else {
                panic("udp_send fail %d", senderr);
            }
            cyw43_arch_lwip_end();
        }
        // Work done
        sample_count++;
    }
}