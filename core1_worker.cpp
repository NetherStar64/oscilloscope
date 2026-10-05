#include <stdio.h>
#include "pico/stdlib.h"
#include "pico/cyw43_arch.h"
#include "pico/multicore.h"
#include "config.h"

extern uint16_t sample_buffers[2][1024];
uint32_t sample_count;

struct udpsample {
    uint32_t sample_count;
    u8_t sample_part;
    uint8_t padding[3];
    uint16_t data[512];
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

    while (true) {
        sample_buffer_index = multicore_fifo_pop_blocking();
        // Work 
        // uint16_t i0 = sample_buffers[sample_buffer_index][0];
        // printf("Buffer Number %d (%d): [0] = %d\n", sample_count, sample_buffer_index, i0);
        
        const u8_t num_packets = SAMPLE_BUFFER_SIZE / SAMPLE_BUFFER_SPLIT;
        cyw43_arch_lwip_begin();
        for (u8_t i = 0; i<num_packets; i++) {
            struct pbuf *p = pbuf_alloc(PBUF_TRANSPORT, sizeof(udpsample), PBUF_RAM);
            if (p==NULL) {
                panic("pbuf_alloc fail");
            }
            auto *pkt = static_cast<udpsample*>(p->payload);
            pkt->sample_count = sample_count;
            pkt->sample_part  = i;
            memcpy(
                pkt->data, 
                &sample_buffers[sample_buffer_index][i * SAMPLE_BUFFER_SPLIT], 
                SAMPLE_BUFFER_SPLIT * sizeof(uint16_t)
            );

            s8_t senderr = udp_send(udp_conn, p);
            pbuf_free(p);
            if (senderr != 0) {
                panic("udp_send fail %d", senderr);
            }
        }
        cyw43_arch_lwip_end();
        // Work done, notify C0
        sample_count++;
        multicore_fifo_push_blocking(0x4141);
    }
};