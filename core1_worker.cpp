#include <stdio.h>
#include "pico/stdlib.h"
#include "pico/cyw43_arch.h"
#include "pico/multicore.h"
#include "core1_worker.h"
#include "config.h"
#include "wifipassword.h"
#include "pico/util/queue.h"
#include "lwip/udp.h"
#include "lwip/tcp.h"
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <cmath>

extern uint16_t sample_buffers[NUM_RING_BUFFERS][SAMPLE_BUFFER_SIZE];
extern queue_t sample_fifo;
extern volatile uint overflow_count;
uint32_t sample_count;

volatile CaptureMode capture_mode = CaptureMode::Stream;
volatile TriggerEdge trigger_edge = TriggerEdge::Rising;
volatile bool trigger_continuous = false;
volatile uint16_t trigger_level = 2048;
volatile uint32_t requested_sample_rate = 500000;
volatile uint32_t control_generation = 0;
volatile uint32_t requested_capture_length = NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE;

#define CONTROL_PORT 4445
#define CAPTURE_PORT 4446

struct __attribute__((packed)) udpsample {
    uint32_t sample_count;
    u8_t sample_part;
    uint8_t padding[3];
    uint16_t data[SAMPLE_BUFFER_SPLIT];
};

struct CaptureHeader {
    char magic[4];
    uint32_t sample_count;
    uint32_t buffer_capacity;
    uint32_t overflow_count;
};
static_assert(sizeof(CaptureHeader) == 16, "Capture TCP header must be 16 bytes");

struct CaptureTcpState {
    struct tcp_pcb *pcb;
    volatile bool connected;
    volatile bool failed;
    volatile uint32_t acknowledged;
};

static err_t capture_connected(void *arg, struct tcp_pcb *, err_t error) {
    auto *state = static_cast<CaptureTcpState *>(arg);
    if (error == ERR_OK) {
        state->connected = true;
    } else {
        state->failed = true;
    }
    return error;
}

static err_t capture_sent(void *arg, struct tcp_pcb *, u16_t length) {
    auto *state = static_cast<CaptureTcpState *>(arg);
    state->acknowledged += length;
    return ERR_OK;
}

static err_t capture_received(void *arg, struct tcp_pcb *pcb,
                              struct pbuf *packet, err_t) {
    auto *state = static_cast<CaptureTcpState *>(arg);
    if (packet == nullptr) {
        tcp_close(pcb);
        state->pcb = nullptr;
        return ERR_OK;
    }
    pbuf_free(packet);
    return ERR_OK;
}

static void capture_error(void *arg, err_t) {
    auto *state = static_cast<CaptureTcpState *>(arg);
    state->pcb = nullptr;
    state->failed = true;
}

static bool wait_for_tcp(CaptureTcpState &state, uint32_t target_ack) {
    const uint32_t deadline = time_us_32() + 15000000;
    while (!state.failed &&
           (!state.connected || state.acknowledged < target_ack) &&
           static_cast<int32_t>(time_us_32() - deadline) < 0) {
        sleep_ms(1);
    }
    return !state.failed && state.connected &&
           state.acknowledged >= target_ack;
}

static bool send_tcp_bytes(CaptureTcpState &state,
                           const uint8_t *bytes,
                           uint32_t length) {
    uint32_t offset = 0;
    while (offset < length) {
        if (state.failed) {
            return false;
        }
        const uint32_t chunk_length = std::min<uint32_t>(length - offset, 1024);
        const uint32_t target_ack = state.acknowledged + chunk_length;
        cyw43_arch_lwip_begin();
        const err_t write_result = state.pcb == nullptr
            ? ERR_CONN
            : tcp_write(state.pcb, bytes + offset, chunk_length,
                        TCP_WRITE_FLAG_COPY);
        if (write_result == ERR_OK) {
            tcp_output(state.pcb);
        }
        cyw43_arch_lwip_end();

        if (write_result == ERR_MEM) {
            sleep_ms(1);
            continue;
        }
        if (write_result != ERR_OK ||
            !wait_for_tcp(state, target_ack)) {
            return false;
        }
        offset += chunk_length;
    }
    return true;
}

static bool connect_capture_tcp(CaptureTcpState &state,
                                const ip_addr_t *destination) {
    if (state.pcb != nullptr && state.connected && !state.failed) {
        return true;
    }
    state = {};
    cyw43_arch_lwip_begin();
    state.pcb = tcp_new();
    if (state.pcb == nullptr) {
        cyw43_arch_lwip_end();
        printf("Failed to allocate capture TCP connection\n");
        return false;
    }
    tcp_arg(state.pcb, &state);
    tcp_err(state.pcb, capture_error);
    tcp_sent(state.pcb, capture_sent);
    tcp_recv(state.pcb, capture_received);
    const err_t connect_result = tcp_connect(
        state.pcb, destination, CAPTURE_PORT, capture_connected);
    if (connect_result != ERR_OK) {
        tcp_abort(state.pcb);
        state.pcb = nullptr;
        cyw43_arch_lwip_end();
        printf("Failed to connect to capture TCP server: %d\n", connect_result);
        return false;
    }
    cyw43_arch_lwip_end();

    if (!wait_for_tcp(state, 0)) {
        printf("Capture TCP connection timed out or failed\n");
        goto connection_failed;
    }
    return true;

connection_failed:
    cyw43_arch_lwip_begin();
    if (state.pcb != nullptr) {
        tcp_abort(state.pcb);
        state.pcb = nullptr;
    }
    cyw43_arch_lwip_end();
    return false;
}

static bool send_capture_tcp(CaptureTcpState &state,
                             const ip_addr_t *destination,
                             const uint16_t *samples,
                             uint32_t length,
                             uint32_t dropped_buffers,
                             uint32_t overflow_start) {
    if (!connect_capture_tcp(state, destination)) {
        return false;
    }

    const CaptureHeader header{
        {'O', 'S', 'C', 'P'},
        length,
        NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE,
        dropped_buffers,
    };
    uint32_t final_dropped_buffers = dropped_buffers;
    if (!send_tcp_bytes(state, reinterpret_cast<const uint8_t *>(&header),
                        sizeof(header)) ||
        !send_tcp_bytes(state, reinterpret_cast<const uint8_t *>(samples),
                        length * sizeof(uint16_t))) {
        goto failed;
    }

    final_dropped_buffers = overflow_count - overflow_start;
    if (final_dropped_buffers > dropped_buffers) {
        const CaptureHeader overflow_update{
            {'O', 'S', 'C', 'P'},
            0,
            NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE,
            final_dropped_buffers,
        };
        if (!send_tcp_bytes(state,
                            reinterpret_cast<const uint8_t *>(&overflow_update),
                            sizeof(overflow_update))) {
            goto failed;
        }
    }
    return true;

failed:
    cyw43_arch_lwip_begin();
    if (state.pcb != nullptr) {
        tcp_abort(state.pcb);
        state.pcb = nullptr;
    }
    cyw43_arch_lwip_end();
    printf("Capture TCP transfer failed\n");
    return false;
}

static void close_capture_tcp(CaptureTcpState &state) {
    cyw43_arch_lwip_begin();
    if (state.pcb != nullptr) {
        tcp_arg(state.pcb, nullptr);
        tcp_err(state.pcb, nullptr);
        tcp_sent(state.pcb, nullptr);
        tcp_recv(state.pcb, nullptr);
        if (tcp_close(state.pcb) != ERR_OK) {
            tcp_abort(state.pcb);
        }
        state.pcb = nullptr;
    }
    cyw43_arch_lwip_end();
    state = {};
}

static void control_received(void *, struct udp_pcb *, struct pbuf *packet,
                             const ip_addr_t *address, u16_t port) {
    char command[96];
    const u16_t length = packet->tot_len < sizeof(command) - 1
        ? packet->tot_len : sizeof(command) - 1;
    pbuf_copy_partial(packet, command, length, 0);
    command[length] = '\0';
    pbuf_free(packet);

    unsigned mode, rate, edge, continuous, capture_length;
    float voltage;
    if (sscanf(command, "%u %u %f %u %u %u", &mode, &rate, &voltage, &edge,
                &continuous, &capture_length) != 6 ||
        mode > 2 || rate < 8 || rate > 500000 || !std::isfinite(voltage) ||
        voltage < 0.0f || voltage > 6.6f || edge > 1 || continuous > 1 ||
        capture_length < SAMPLE_BUFFER_SIZE ||
        capture_length > NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE ||
        capture_length % SAMPLE_BUFFER_SIZE != 0) {
        printf("Invalid viewer control command\n");
        return;
    }

    requested_sample_rate = rate;
    trigger_level = static_cast<uint16_t>(voltage * 4095.0f / 6.6f);
    trigger_edge = static_cast<TriggerEdge>(edge);
    trigger_continuous = continuous != 0;
    requested_capture_length = capture_length;
    capture_mode = static_cast<CaptureMode>(mode);
    ++control_generation;
    printf("Viewer settings from %s:%u\n", ipaddr_ntoa(address), static_cast<unsigned>(port));
}

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

    auto *control = udp_new();
    if (control == nullptr || udp_bind(control, IP_ADDR_ANY, CONTROL_PORT) != ERR_OK) {
        panic("Failed to start viewer control socket");
    }
    udp_recv(control, control_received, nullptr);

    ip_addr_t viewer;
    if (ipaddr_aton(VIEWER_IP, &viewer) == 0) {
        panic("ipaddr_aton failed");
    }

    printf("Connected to UDP socket\n");
    cyw43_arch_lwip_end();

    static uint16_t sample_buffer_copy[SAMPLE_BUFFER_SIZE];
    static uint16_t capture_samples[NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE];
    CaptureTcpState capture_connection{};
    bool once_armed = true;
    bool capture_active = false;
    bool have_previous_sample = false;
    uint16_t previous_sample = 0;
    uint32_t capture_size = 0;
    uint32_t capture_overflow_start = 0;
    uint32_t observed_generation = control_generation;
    while (true) {
        // Wait for new data
        queue_remove_blocking(&sample_fifo, &sample_buffer_index);
        // Memcopy for no tear
        memcpy(
            sample_buffer_copy,
            sample_buffers[sample_buffer_index], 
            sizeof(sample_buffer_copy));

        const CaptureMode mode = capture_mode;
        const uint32_t generation = control_generation;
        if (generation != observed_generation) {
            once_armed = true;
            capture_active = false;
            capture_size = 0;
            have_previous_sample = false;
            observed_generation = generation;
        }

        if (mode == CaptureMode::Stream) {
            close_capture_tcp(capture_connection);
            const u8_t num_packets = SAMPLE_BUFFER_SIZE / SAMPLE_BUFFER_SPLIT;
            cyw43_arch_lwip_begin();
            for (u8_t i = 0; i < num_packets; i++) {
                struct pbuf *p = pbuf_alloc(PBUF_TRANSPORT, sizeof(udpsample), PBUF_RAM);
                if (p == nullptr) {
                    cyw43_arch_lwip_end();
                    break;
                }
                auto *pkt = static_cast<udpsample *>(p->payload);
                pkt->sample_count = sample_count;
                pkt->sample_part = i;
                memcpy(pkt->data, &sample_buffer_copy[i * SAMPLE_BUFFER_SPLIT],
                       SAMPLE_BUFFER_SPLIT * sizeof(uint16_t));
                const err_t send_error = udp_send(udp_conn, p);
                pbuf_free(p);
                if (send_error != ERR_OK && send_error != ERR_MEM) {
                    cyw43_arch_lwip_end();
                    panic("udp_send fail %d", send_error);
                }
            }
            cyw43_arch_lwip_end();
            ++sample_count;
            continue;
        }

        const uint32_t max_length = requested_capture_length;
        if (mode == CaptureMode::Once && once_armed && !capture_active) {
            capture_active = true;
            capture_overflow_start = overflow_count;
        }

        size_t start_index = 0;
        if (mode == CaptureMode::Trigger && !capture_active && once_armed) {
            const uint16_t level = trigger_level;
            const TriggerEdge edge = trigger_edge;
            for (size_t i = 0; i < SAMPLE_BUFFER_SIZE; ++i) {
                const uint16_t current_sample = sample_buffer_copy[i];
                if (have_previous_sample &&
                    ((edge == TriggerEdge::Rising &&
                      previous_sample < level && current_sample >= level) ||
                     (edge == TriggerEdge::Falling &&
                      previous_sample > level && current_sample <= level))) {
                    capture_active = true;
                    capture_overflow_start = overflow_count;
                    start_index = i;
                    break;
                }
                previous_sample = current_sample;
                have_previous_sample = true;
            }
        }

        if (capture_active) {
            const uint32_t available = SAMPLE_BUFFER_SIZE - start_index;
            const uint32_t copy_length = capture_size < max_length
                ? std::min(available, max_length - capture_size) : 0;
            memcpy(&capture_samples[capture_size],
                   &sample_buffer_copy[start_index],
                   copy_length * sizeof(uint16_t));
            capture_size += copy_length;
            previous_sample = sample_buffer_copy[SAMPLE_BUFFER_SIZE - 1];
            have_previous_sample = true;
        }

        if (capture_active && capture_size >= max_length) {
            const uint32_t dropped_buffers = overflow_count - capture_overflow_start;
            once_armed = false;
            capture_active = false;
            if (!send_capture_tcp(capture_connection, &viewer,
                                  capture_samples, capture_size,
                                  dropped_buffers, capture_overflow_start)) {
                printf("Capture was not delivered to viewer\n");
            }
            u8_t stale_buffer;
            while (queue_try_remove(&sample_fifo, &stale_buffer)) {
            }
            capture_size = 0;
            if (mode == CaptureMode::Trigger && trigger_continuous &&
                control_generation == generation) {
                once_armed = true;
                have_previous_sample = false;
            }
        }
    }
}