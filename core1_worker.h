#pragma once
#include <stdint.h>

void usb_worker();
void process_usb_control();
void flash_trigger_led();

enum class CaptureMode : uint8_t {
    Stream = 0,
    Once = 1,
    Trigger = 2,
};

enum class TriggerEdge : uint8_t {
    Rising = 0,
    Falling = 1,
};

extern volatile CaptureMode capture_mode;
extern volatile TriggerEdge trigger_edge;
extern volatile bool trigger_continuous;
extern volatile uint16_t trigger_level;
extern volatile uint32_t requested_sample_rate;
extern volatile uint32_t control_generation;
extern volatile uint32_t requested_capture_length;
extern volatile uint8_t trigger_offset_percent;
extern volatile bool acquisition_pause_requested;
extern volatile bool acquisition_paused;
extern volatile bool acquisition_stats_reset_requested;
extern volatile bool acquisition_init_reset_requested;
extern volatile bool soft_reset_requested;