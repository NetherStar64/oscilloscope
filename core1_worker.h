#include <stdio.h>
#include "pico/cyw43_arch.h"


#pragma once
#include <stdint.h>

void wifi_worker();

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