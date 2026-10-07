#include <stdio.h>
#include "pico/stdlib.h"
#include "pico/cyw43_arch.h"
#include "pico/multicore.h"
#include "core1_worker.h"
#include "config.h"
#include "wifipassword.h"
#include "pico/util/queue.h"
#include "hardware/clocks.h"
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
volatile uint8_t trigger_offset_percent = 50;
volatile bool acquisition_pause_requested = false;
volatile bool acquisition_paused = false;

#define CONTROL_PORT 4445
#define CAPTURE_PORT 4446
#define STATUS_PORT 4447

enum class ViewerStatus : uint8_t {
    TriggerHit = 1,
    DmaOverflow = 2,
    TriggerOnceState = 3,
};

static void send_viewer_status(struct udp_pcb *status_conn,
                               ViewerStatus event,
                               uint32_t value) {
    uint8_t message[9] = {'O', 'S', 'C', 'E', static_cast<uint8_t>(event)};
    memcpy(&message[5], &value, sizeof(value));
    cyw43_arch_lwip_begin();
    struct pbuf *packet = pbuf_alloc(PBUF_TRANSPORT, sizeof(message), PBUF_RAM);
    if (packet != nullptr) {
        memcpy(packet->payload, message, sizeof(message));
        const err_t result = udp_send(status_conn, packet);
        pbuf_free(packet);
        if (result != ERR_OK) {
            printf("Viewer status UDP send failed: %d\n", result);
        }
    } else {
        printf("Viewer status UDP packet allocation failed\n");
    }
    cyw43_arch_lwip_end();
}

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
    uint32_t trigger_index;
    uint32_t sample_rate;
};
static_assert(sizeof(CaptureHeader) == 24, "Capture TCP header must be 24 bytes");

struct CaptureTcpState {
    struct tcp_pcb *pcb;
    volatile bool connected;
    volatile bool failed;
    volatile uint32_t acknowledged;
    volatile uint32_t queued;
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

static bool wait_for_tcp(CaptureTcpState &state, uint32_t target_ack,
                         uint32_t previous_ack) {
    const uint32_t deadline = time_us_32() + 5000000;
    while (!state.failed &&
           (!state.connected ||
            (state.acknowledged < target_ack &&
             state.acknowledged <= previous_ack)) &&
           static_cast<int32_t>(time_us_32() - deadline) < 0) {
        sleep_us(100);
    }
    return !state.failed && state.connected &&
           (state.acknowledged >= target_ack ||
            state.acknowledged > previous_ack);
}

static bool send_tcp_bytes(CaptureTcpState &state,
                           const uint8_t *bytes,
                           uint32_t length,
                           bool copy_data) {
    uint32_t offset = 0;
    while (offset < length) {
        if (state.failed) {
            printf("Capture TCP connection failed at byte %u\n",
                   static_cast<unsigned>(offset));
            return false;
        }
        const uint32_t batch_start_ack = state.acknowledged;
        bool queued_data = false;
        cyw43_arch_lwip_begin();
        while (offset < length) {
            uint32_t chunk_length = 0;
            err_t write_result;
            if (state.pcb == nullptr) {
                write_result = ERR_CONN;
            } else {
                const uint16_t send_space = tcp_sndbuf(state.pcb);
                chunk_length = std::min<uint32_t>(
                    std::min<uint32_t>(length - offset, TCP_MSS), send_space);
                const uint8_t write_flags =
                    (copy_data ? TCP_WRITE_FLAG_COPY : 0) |
                    (offset + chunk_length < length
                        ? TCP_WRITE_FLAG_MORE : 0);
                write_result = chunk_length == 0
                    ? ERR_MEM
                    : tcp_write(state.pcb, bytes + offset, chunk_length,
                                write_flags);
                if (write_result == ERR_OK) {
                    state.queued += chunk_length;
                    queued_data = true;
                    offset += chunk_length;
                }
            }

            if (write_result == ERR_MEM) {
                break;
            }
            if (write_result != ERR_OK) {
                cyw43_arch_lwip_end();
                printf("Capture TCP write failed at byte %u: %d\n",
                       static_cast<unsigned>(offset), write_result);
                return false;
            }
        }

        const err_t output_result = state.pcb == nullptr
            ? ERR_CONN : tcp_output(state.pcb);
        cyw43_arch_lwip_end();
        if (output_result != ERR_OK) {
            printf("Capture TCP output failed: %d\n", output_result);
            return false;
        }

        const uint32_t total_queued = state.queued;
        const uint32_t target_ack = queued_data
            ? std::min<uint32_t>(total_queued,
                                 batch_start_ack + 8 * TCP_MSS)
            : batch_start_ack + 1;
        if (!wait_for_tcp(state, target_ack, batch_start_ack)) {
            printf("Capture TCP acknowledgement stalled at %u of %u bytes\n",
                   static_cast<unsigned>(state.acknowledged),
                   static_cast<unsigned>(target_ack));
            return false;
        }
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
    tcp_nagle_disable(state.pcb);
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

    if (!wait_for_tcp(state, 0, 0)) {
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

static uint32_t pack_capture_samples(uint16_t *samples, uint32_t sample_count) {
    auto *packed = reinterpret_cast<uint8_t *>(samples);
    uint32_t output_index = 0;
    uint32_t sample_index = 0;
    while (sample_index + 1 < sample_count) {
        const uint16_t first = samples[sample_index];
        const uint16_t second = samples[sample_index + 1];
        packed[output_index++] = static_cast<uint8_t>(first);
        packed[output_index++] = static_cast<uint8_t>(
            ((first >> 8) & 0x0f) | ((second & 0x0f) << 4));
        packed[output_index++] = static_cast<uint8_t>(second >> 4);
        sample_index += 2;
    }
    if (sample_index < sample_count) {
        const uint16_t last = samples[sample_index];
        packed[output_index++] = static_cast<uint8_t>(last);
        packed[output_index++] = static_cast<uint8_t>((last >> 8) & 0x0f);
    }
    return output_index;
}

static bool send_capture_tcp(CaptureTcpState &state,
                             const ip_addr_t *destination,
                             const uint8_t *samples,
                             uint32_t payload_length,
                             uint32_t length,
                             uint32_t sample_rate,
                             uint32_t dropped_buffers,
                             uint32_t overflow_start,
                             uint32_t trigger_index,
                             uint8_t retries_remaining = 1) {
    if (!connect_capture_tcp(state, destination)) {
        if (retries_remaining > 0) {
            printf("Retrying capture TCP connection\n");
            return send_capture_tcp(state, destination, samples, payload_length,
                                    length, sample_rate,
                                    dropped_buffers, overflow_start,
                                    trigger_index, retries_remaining - 1);
        }
        return false;
    }

    const CaptureHeader header{
        {'O', 'S', '1', '3'},
        length,
        NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE,
        dropped_buffers,
        trigger_index,
        sample_rate,
    };
    uint32_t final_dropped_buffers = dropped_buffers;
    // The paused acquisition path keeps this static capture buffer unchanged
    // until every zero-copy TCP segment has been acknowledged. Queue the
    // header with MORE so it can share the first output with the sample data.
    err_t header_result;
    cyw43_arch_lwip_begin();
    if (state.pcb == nullptr) {
        header_result = ERR_CONN;
    } else {
        header_result = tcp_write(
            state.pcb, &header, sizeof(header),
            TCP_WRITE_FLAG_COPY | TCP_WRITE_FLAG_MORE);
        if (header_result == ERR_OK) {
            state.queued += sizeof(header);
        }
    }
    cyw43_arch_lwip_end();
    if (header_result != ERR_OK ||
        !send_tcp_bytes(state, reinterpret_cast<const uint8_t *>(samples),
                        payload_length, false)) {
        if (header_result != ERR_OK) {
            printf("Capture TCP header write failed: %d\n", header_result);
        }
        goto failed;
    }

    final_dropped_buffers = overflow_count - overflow_start;
    if (final_dropped_buffers > dropped_buffers) {
        const CaptureHeader overflow_update{
            {'O', 'S', '1', '3'},
            0,
            NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE,
            final_dropped_buffers,
            trigger_index,
            sample_rate,
        };
        if (!send_tcp_bytes(state,
                            reinterpret_cast<const uint8_t *>(&overflow_update),
                            sizeof(overflow_update), true)) {
            goto failed;
        }
    }
    return true;

failed:
    cyw43_arch_lwip_begin();
    if (state.pcb != nullptr) {
        tcp_arg(state.pcb, nullptr);
        tcp_err(state.pcb, nullptr);
        tcp_sent(state.pcb, nullptr);
        tcp_recv(state.pcb, nullptr);
        tcp_abort(state.pcb);
        state.pcb = nullptr;
    }
    cyw43_arch_lwip_end();
    state = {};
    if (retries_remaining > 0) {
        printf("Retrying capture TCP transfer\n");
        return send_capture_tcp(state, destination, samples, payload_length,
                                length, sample_rate,
                                dropped_buffers, overflow_start,
                                trigger_index, retries_remaining - 1);
    }
    printf("Capture TCP transfer failed\n");
    return false;
}

static void set_acquisition_paused(bool paused) {
    acquisition_pause_requested = paused;
    while (acquisition_paused != paused) {
        sleep_ms(1);
    }
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

    unsigned mode, rate, edge, continuous, capture_length, offset_percent;
    float voltage;
    const uint32_t minimum_sample_rate =
        clock_get_hz(clk_adc) / 65537u + 1u;
    if (sscanf(command, "%u %u %f %u %u %u %u", &mode, &rate, &voltage, &edge,
                &continuous, &capture_length, &offset_percent) != 7 ||
        mode > 2 || rate < minimum_sample_rate || rate > 500000 ||
        !std::isfinite(voltage) ||
        voltage < 0.0f || voltage > 6.6f || edge > 1 || continuous > 1 ||
        capture_length < SAMPLE_BUFFER_SIZE ||
        capture_length > NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE ||
        capture_length % SAMPLE_BUFFER_SIZE != 0 || offset_percent > 100) {
        printf("Invalid viewer control command\n");
        return;
    }

    requested_sample_rate = rate;
    trigger_level = static_cast<uint16_t>(voltage * 4095.0f / 6.6f);
    trigger_edge = static_cast<TriggerEdge>(edge);
    trigger_continuous = continuous != 0;
    requested_capture_length = capture_length;
    trigger_offset_percent = offset_percent;
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
    auto *status_conn = udp_new();
    if (status_conn == nullptr ||
        udp_connect(status_conn, &viewer, STATUS_PORT) != ERR_OK) {
        panic("Failed to start viewer status socket");
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
    uint32_t pretrigger_size = 0;
    uint32_t pretrigger_write_index = 0;
    uint32_t capture_size = 0;
    uint32_t capture_sample_rate = requested_sample_rate;
    uint32_t capture_trigger_index = UINT32_MAX;
    uint32_t capture_overflow_start = 0;
    uint32_t last_notified_overflow_count = overflow_count;
    uint32_t observed_generation = control_generation;
    while (true) {
        // Wait for new data
        queue_remove_blocking(&sample_fifo, &sample_buffer_index);
        // Memcopy for no tear
        memcpy(
            sample_buffer_copy,
            sample_buffers[sample_buffer_index], 
            sizeof(sample_buffer_copy));

        const uint32_t current_overflow_count = overflow_count;
        if (current_overflow_count != last_notified_overflow_count) {
            send_viewer_status(status_conn, ViewerStatus::DmaOverflow,
                               current_overflow_count);
            last_notified_overflow_count = current_overflow_count;
        }

        const CaptureMode mode = capture_mode;
        const uint32_t generation = control_generation;
        if (generation != observed_generation) {
            once_armed = true;
            capture_active = false;
            capture_size = 0;
            have_previous_sample = false;
            pretrigger_size = 0;
            pretrigger_write_index = 0;
            capture_trigger_index = UINT32_MAX;
            observed_generation = generation;
            send_viewer_status(
                status_conn, ViewerStatus::TriggerOnceState,
                mode == CaptureMode::Trigger && !trigger_continuous ? 1 : 0);
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
            capture_sample_rate = requested_sample_rate;
        }

        size_t start_index = 0;
        if (mode == CaptureMode::Trigger && !capture_active && once_armed) {
            const uint16_t level = trigger_level;
            const TriggerEdge edge = trigger_edge;
            const uint32_t desired_pretrigger = std::min(
                max_length - 1,
                max_length * static_cast<uint32_t>(trigger_offset_percent) / 100);
            const uint32_t pretrigger_capacity = desired_pretrigger;
            for (size_t i = 0; i < SAMPLE_BUFFER_SIZE; ++i) {
                const uint16_t current_sample = sample_buffer_copy[i];
                const bool crossed = have_previous_sample &&
                    ((edge == TriggerEdge::Rising &&
                      previous_sample < level && current_sample >= level) ||
                     (edge == TriggerEdge::Falling &&
                      previous_sample > level && current_sample <= level));
                if (crossed && pretrigger_size >= desired_pretrigger) {
                    capture_active = true;
                    capture_overflow_start = overflow_count;
                    capture_sample_rate = requested_sample_rate;
                    capture_trigger_index = desired_pretrigger;
                    capture_size = desired_pretrigger;
                    if (!trigger_continuous) {
                        once_armed = false;
                        send_viewer_status(
                            status_conn, ViewerStatus::TriggerOnceState, 0);
                    }
                    flash_trigger_led();
                    send_viewer_status(status_conn, ViewerStatus::TriggerHit,
                                       desired_pretrigger);
                    if (desired_pretrigger > 0) {
                        std::rotate(capture_samples,
                                    capture_samples + pretrigger_write_index,
                                    capture_samples + desired_pretrigger);
                    }
                    start_index = i;
                    break;
                }
                if (pretrigger_capacity > 0) {
                    capture_samples[pretrigger_write_index] = current_sample;
                    pretrigger_write_index =
                        (pretrigger_write_index + 1) % pretrigger_capacity;
                    pretrigger_size =
                        std::min(pretrigger_size + 1, pretrigger_capacity);
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
            set_acquisition_paused(true);
            const uint32_t payload_length =
                pack_capture_samples(capture_samples, capture_size);
            if (!send_capture_tcp(capture_connection, &viewer,
                                  reinterpret_cast<const uint8_t *>(capture_samples),
                                  payload_length, capture_size, capture_sample_rate,
                                  dropped_buffers, capture_overflow_start,
                                  capture_trigger_index)) {
                printf("Capture was not delivered to viewer\n");
            }
            set_acquisition_paused(false);
            capture_size = 0;
            if (mode == CaptureMode::Trigger && trigger_continuous &&
                control_generation == generation) {
                once_armed = true;
                send_viewer_status(
                    status_conn, ViewerStatus::TriggerOnceState, 1);
                have_previous_sample = false;
                pretrigger_size = 0;
                pretrigger_write_index = 0;
                capture_trigger_index = UINT32_MAX;
            } else {
                capture_trigger_index = UINT32_MAX;
            }
        }
    }
}