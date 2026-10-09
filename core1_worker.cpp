#include <stdio.h>
#include "pico/stdlib.h"
#include "pico/stdio_usb.h"
#include "pico/multicore.h"
#include "tusb.h"
#include "core1_worker.h"
#include "config.h"
#include "pico/util/queue.h"
#include "hardware/watchdog.h"
#include "hardware/clocks.h"
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
volatile bool acquisition_stats_reset_requested = false;
volatile bool acquisition_init_reset_requested = false;
volatile bool soft_reset_requested = false;

static_assert(ADC_VOLTAGE_AVERAGE_SAMPLES > 0 &&
              ADC_VOLTAGE_AVERAGE_SAMPLES <= UINT16_MAX,
              "ADC voltage averaging window must fit the USB protocol");
static_assert(ADC_VOLTAGE_STREAM_HZ > 0,
              "ADC voltage stream frequency must be positive");

enum class ViewerStatus : uint8_t {
    TriggerHit = 1,
    DmaOverflow = 2,
    TriggerOnceState = 3,
    SoftResetComplete = 4,
    LivePacketsSent = 5,
    LivePacketErrors = 6,
    LiveBlocksProcessed = 7,
};

struct CaptureHeader {
    char magic[4];
    uint32_t sample_count;
    uint32_t buffer_capacity;
    uint32_t overflow_count;
    uint32_t trigger_index;
    uint32_t sample_rate;
};
static_assert(sizeof(CaptureHeader) == 24,
              "Capture USB header must be 24 bytes");

static bool write_usb_bytes(const void *bytes, size_t length) {
    if (!stdio_usb_connected()) {
        return false;
    }
    const auto *data = static_cast<const uint8_t *>(bytes);
    size_t offset = 0;
    uint32_t last_progress_us = time_us_32();
    while (offset < length) {
        if (!stdio_usb_connected()) {
            return false;
        }
        const uint32_t available = tud_cdc_write_available();
        if (available == 0) {
            tud_cdc_write_flush();
            if (static_cast<uint32_t>(time_us_32() - last_progress_us) >
                500000u) {
                return false;
            }
            tight_loop_contents();
            continue;
        }
        const uint32_t chunk_length = static_cast<uint32_t>(
            std::min<size_t>(length - offset, available));
        const uint32_t written =
            tud_cdc_write(data + offset, chunk_length);
        if (written == 0) {
            continue;
        }
        offset += written;
        last_progress_us = time_us_32();
    }
    tud_cdc_write_flush();
    return true;
}

static bool begin_usb_frame(uint8_t type, uint32_t payload_length) {
    uint8_t header[9] = {'O', 'S', 'C', 'F', type};
    memcpy(&header[5], &payload_length, sizeof(payload_length));
    return write_usb_bytes(header, sizeof(header));
}

static uint32_t pack_capture_samples(uint16_t *samples,
                                     uint32_t sample_count) {
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

static void send_viewer_status(ViewerStatus event, uint32_t value) {
    uint8_t message[9] = {'O', 'S', 'C', 'E',
                          static_cast<uint8_t>(event)};
    memcpy(&message[5], &value, sizeof(value));
    if (!begin_usb_frame(3, sizeof(message)) ||
        !write_usb_bytes(message, sizeof(message))) {
        return;
    }
}

static void send_voltage_reading(uint16_t average) {
    const uint8_t message[8] = {
        'O', 'S', 'C', 'V',
        static_cast<uint8_t>(average & 0xff),
        static_cast<uint8_t>(average >> 8),
        static_cast<uint8_t>(ADC_VOLTAGE_AVERAGE_SAMPLES & 0xff),
        static_cast<uint8_t>(ADC_VOLTAGE_AVERAGE_SAMPLES >> 8),
    };
    if (!begin_usb_frame(4, sizeof(message)) ||
        !write_usb_bytes(message, sizeof(message))) {
        return;
    }
}

static bool send_live_samples(uint32_t sample_number, uint16_t *samples) {
    constexpr uint32_t packed_length = SAMPLE_BUFFER_SIZE * 3 / 2;
    constexpr uint32_t payload_length = sizeof(sample_number) + packed_length;
    static uint8_t frame[9 + payload_length];
    frame[0] = 'O';
    frame[1] = 'S';
    frame[2] = 'C';
    frame[3] = 'F';
    frame[4] = 1;
    memcpy(&frame[5], &payload_length, sizeof(payload_length));
    memcpy(&frame[9], &sample_number, sizeof(sample_number));

    uint8_t *packed = &frame[9 + sizeof(sample_number)];
    uint32_t output_index = 0;
    for (uint32_t index = 0; index < SAMPLE_BUFFER_SIZE; index += 2) {
        const uint16_t first = samples[index];
        const uint16_t second = samples[index + 1];
        packed[output_index] = static_cast<uint8_t>(first);
        packed[output_index + 1] = static_cast<uint8_t>(
            ((first >> 8) & 0x0f) | ((second & 0x0f) << 4));
        packed[output_index + 2] = static_cast<uint8_t>(second >> 4);
        output_index += 3;
    }
    return write_usb_bytes(frame, sizeof(frame));
}

static bool send_capture_usb(const uint8_t *samples,
                             uint32_t payload_length,
                             uint32_t length,
                             uint32_t sample_rate,
                             uint32_t dropped_buffers,
                             uint32_t trigger_index) {
    const CaptureHeader header{
        {'O', 'S', '1', '3'},
        length,
        NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE,
        dropped_buffers,
        trigger_index,
        sample_rate,
    };
    if (!begin_usb_frame(2, sizeof(header) + payload_length) ||
        !write_usb_bytes(&header, sizeof(header)) ||
        !write_usb_bytes(samples, payload_length)) {
        return false;
    }
    return true;
}

static void set_acquisition_paused(bool paused) {
    acquisition_pause_requested = paused;
    while (acquisition_paused != paused) {
        sleep_ms(1);
    }
}

void process_usb_control() {
    static char command[96];
    static size_t command_length = 0;
    static bool command_overflow = false;
    while (true) {
        const int input = getchar_timeout_us(0);
        if (input == PICO_ERROR_TIMEOUT) {
            return;
        }
        if (input == '\r') {
            continue;
        }
        if (input != '\n') {
            if (command_length < sizeof(command) - 1) {
                command[command_length++] = static_cast<char>(input);
            } else {
                command_overflow = true;
            }
            continue;
        }
        if (command_overflow) {
            command_length = 0;
            command_overflow = false;
            continue;
        }
        command[command_length] = '\0';
        command_length = 0;
        if (strcmp(command, "OSCR") == 0) {
            soft_reset_requested = true;
            continue;
        }
        if (strcmp(command, "OSCH") == 0) {
            watchdog_reboot(0, 0, 0);
            continue;
        }

        unsigned mode, rate, edge, continuous, capture_length, offset_percent;
        float voltage;
        const uint32_t minimum_sample_rate =
            clock_get_hz(clk_adc) / 65537u + 1u;
        if (sscanf(command, "%u %u %f %u %u %u %u", &mode, &rate, &voltage,
                   &edge, &continuous, &capture_length, &offset_percent) != 7 ||
            mode > 2 || rate < minimum_sample_rate || rate > 500000 ||
            !std::isfinite(voltage) || voltage < 0.0f || voltage > 6.6f ||
            edge > 1 || continuous > 1 ||
            capture_length < SAMPLE_BUFFER_SIZE ||
            capture_length > NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE ||
            capture_length % SAMPLE_BUFFER_SIZE != 0 || offset_percent > 100) {
            continue;
        }

        requested_sample_rate = rate;
        trigger_level = static_cast<uint16_t>(voltage * 4095.0f / 6.6f);
        trigger_edge = static_cast<TriggerEdge>(edge);
        trigger_continuous = continuous != 0;
        requested_capture_length = capture_length;
        trigger_offset_percent = offset_percent;
        capture_mode = static_cast<CaptureMode>(mode);
        ++control_generation;
    }
}

void usb_worker() {
    uint8_t sample_buffer_index;
    static uint16_t sample_buffer_copy[SAMPLE_BUFFER_SIZE];
    static uint16_t capture_samples[NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE];
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
    uint32_t voltage_sum = 0;
    uint32_t voltage_sample_count = 0;
    uint16_t latest_voltage_average = 0;
    bool have_voltage_average = false;
    uint32_t last_voltage_send_us = time_us_32();
    uint32_t live_packets_sent = 0;
    uint32_t live_packet_errors = 0;
    uint32_t live_blocks_processed = 0;
    uint32_t last_live_stats_send_us = time_us_32();
    while (true) {
        // Wait for new data
        queue_remove_blocking(&sample_fifo, &sample_buffer_index);
        if (soft_reset_requested) {
            soft_reset_requested = false;
            set_acquisition_paused(true);
            memset(sample_buffers, 0, sizeof(sample_buffers));
            capture_mode = CaptureMode::Stream;
            trigger_edge = TriggerEdge::Rising;
            trigger_continuous = false;
            trigger_level = 2048;
            requested_sample_rate = 500000;
            requested_capture_length =
                NUM_RING_BUFFERS * SAMPLE_BUFFER_SIZE;
            trigger_offset_percent = 50;
            ++control_generation;
            observed_generation = control_generation;
            once_armed = true;
            capture_active = false;
            have_previous_sample = false;
            previous_sample = 0;
            pretrigger_size = 0;
            pretrigger_write_index = 0;
            capture_size = 0;
            capture_sample_rate = requested_sample_rate;
            capture_trigger_index = UINT32_MAX;
            capture_overflow_start = 0;
            voltage_sum = 0;
            voltage_sample_count = 0;
            latest_voltage_average = 0;
            have_voltage_average = false;
            acquisition_stats_reset_requested = true;
            while (acquisition_stats_reset_requested) {
                sleep_ms(1);
            }
            last_notified_overflow_count = 0;
            acquisition_init_reset_requested = true;
            set_acquisition_paused(false);
            send_viewer_status(ViewerStatus::SoftResetComplete, 0);
            continue;
        }
        // Memcopy for no tear
        memcpy(
            sample_buffer_copy,
            sample_buffers[sample_buffer_index], 
            sizeof(sample_buffer_copy));

        const uint32_t current_overflow_count = overflow_count;
        if (current_overflow_count != last_notified_overflow_count) {
            send_viewer_status(ViewerStatus::DmaOverflow,
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
            send_viewer_status(ViewerStatus::TriggerOnceState,
                mode == CaptureMode::Trigger && !trigger_continuous ? 1 : 0);
        }

        for (uint16_t sample : sample_buffer_copy) {
            voltage_sum += sample;
            if (++voltage_sample_count == ADC_VOLTAGE_AVERAGE_SAMPLES) {
                latest_voltage_average = static_cast<uint16_t>(
                    (voltage_sum + ADC_VOLTAGE_AVERAGE_SAMPLES / 2) /
                    ADC_VOLTAGE_AVERAGE_SAMPLES);
                voltage_sum = 0;
                voltage_sample_count = 0;
                have_voltage_average = true;
            }
        }
        const uint32_t now_us = time_us_32();
        if (have_voltage_average &&
            static_cast<uint32_t>(now_us - last_voltage_send_us) >=
                1000000u / ADC_VOLTAGE_STREAM_HZ) {
            send_voltage_reading(latest_voltage_average);
            last_voltage_send_us = now_us;
        }

        if (mode == CaptureMode::Stream) {
            if (send_live_samples(sample_count, sample_buffer_copy)) {
                ++live_packets_sent;
            } else {
                ++live_packet_errors;
            }
            ++live_blocks_processed;
            const uint32_t stats_now_us = time_us_32();
            if (static_cast<uint32_t>(stats_now_us - last_live_stats_send_us) >=
                1000000u) {
                send_viewer_status(ViewerStatus::LivePacketsSent,
                                   live_packets_sent);
                send_viewer_status(ViewerStatus::LivePacketErrors,
                                   live_packet_errors);
                send_viewer_status(ViewerStatus::LiveBlocksProcessed,
                                   live_blocks_processed);
                live_packets_sent = 0;
                live_packet_errors = 0;
                live_blocks_processed = 0;
                last_live_stats_send_us = stats_now_us;
            }
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
                        send_viewer_status(ViewerStatus::TriggerOnceState, 0);
                    }
                    flash_trigger_led();
                    send_viewer_status(ViewerStatus::TriggerHit,
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
            send_capture_usb(
                reinterpret_cast<const uint8_t *>(capture_samples),
                payload_length, capture_size, capture_sample_rate,
                dropped_buffers, capture_trigger_index);
            set_acquisition_paused(false);
            capture_size = 0;
            if (mode == CaptureMode::Trigger && trigger_continuous &&
                control_generation == generation) {
                once_armed = true;
                send_viewer_status(ViewerStatus::TriggerOnceState, 1);
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