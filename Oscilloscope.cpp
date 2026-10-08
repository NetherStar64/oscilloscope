#include <stdio.h>
#include "pico/stdlib.h"
#include "hardware/gpio.h"
#include "hardware/adc.h"
#include "hardware/clocks.h"
#include "hardware/dma.h"
#include "pico/cyw43_arch.h"
#include "hardware/pwm.h"
#include "pico/multicore.h"
#include "hardware/irq.h"
#include "pico/time.h"
#include <malloc.h>
#include "pico/util/queue.h"

#include "core1_worker.h"
#include "config.h"
#include "wifipassword.h"

#define LED_PIN 16
#define PWM_PIN 2

static int trigger_led_alarm;
static repeating_timer_t wifi_connect_timer;
static bool wifi_connect_led_on = false;

static bool blink_wifi_connect_led(repeating_timer_t *) {
    wifi_connect_led_on = !wifi_connect_led_on;
    gpio_put(LED_PIN, wifi_connect_led_on);
    return true;
}

static void turn_off_trigger_led(uint) {
    gpio_put(LED_PIN, 0);
}

void flash_trigger_led() {
    static uint32_t last_pulse_us = 0;
    const uint32_t now_us = time_us_32();
    if (static_cast<uint32_t>(now_us - last_pulse_us) < 16667) {
        return;
    }

    last_pulse_us = now_us;
    gpio_put(LED_PIN, 1);
    hardware_alarm_set_target(
        trigger_led_alarm, delayed_by_us(get_absolute_time(), 750));
}

volatile bool enable_div = false;
volatile bool firstsample = true;

uint16_t sample_buffers[NUM_RING_BUFFERS][SAMPLE_BUFFER_SIZE];
queue_t sample_fifo;
volatile uint8_t next_sample_buffer = 2; // 0 and 1 are first
volatile uint8_t dma_chan_buffer[2] = {0,1};

volatile uint overflow_count = 0;

static int dma_chan0;
static int dma_chan1;
static bool acquisition_stopped = false;

void dma_irq_handle_channel(int dma_chan_finished, u8_t finished_buf) {
    if (queue_try_add(&sample_fifo, &finished_buf)) {
        // :)
    } else {
        // Core 1 is a slow unc
        overflow_count++;
    }

    // Reset DMA
    const uint8_t dma_slot = (dma_chan_finished == dma_chan0) ? 0 : 1;
    dma_chan_buffer[dma_slot] = next_sample_buffer;
    dma_channel_set_transfer_count(dma_chan_finished, dma_encode_transfer_count(SAMPLE_BUFFER_SIZE), false);
    dma_channel_set_write_addr(dma_chan_finished, sample_buffers[next_sample_buffer], false);
    next_sample_buffer += 1;
    if (next_sample_buffer >= NUM_RING_BUFFERS) {
        next_sample_buffer = 0;
    }
}

void dma_irq_handler()  {
    if (dma_channel_get_irq0_status(dma_chan0)) {
        dma_irqn_acknowledge_channel(0, dma_chan0);
        dma_irq_handle_channel(dma_chan0, dma_chan_buffer[0]);
    }
    if (dma_channel_get_irq0_status(dma_chan1)) {
        dma_irqn_acknowledge_channel(0, dma_chan1);
        dma_irq_handle_channel(dma_chan1, dma_chan_buffer[1]);
    }
}

int main()
{
    stdio_init_all();
    printf("\n=====================\n");
    printf("Init Oscilloscope\n");

    gpio_init(LED_PIN);
    gpio_set_dir(LED_PIN, GPIO_OUT);
    gpio_put(LED_PIN, 0);
    cyw43_arch_init_with_country(WIFI_COUNTRY);
    cyw43_arch_enable_sta_mode();
    add_repeating_timer_ms(250, blink_wifi_connect_led, nullptr,
                           &wifi_connect_timer);

    int res = -1;
    for (int i = 0; i<3; i++) {
        res = cyw43_arch_wifi_connect_timeout_ms(SSID, WIFI_PASS, WIFI_SECURITY, 10000);
        if (res == 0) {
            printf("We got da WiFi\n");
            break;
        }
        printf("Wifi connect %d failed, retry\n", i);
        sleep_ms(2000);
    }
    if (res != 0) {
        panic("Didn't connect to Wifi %d\n", res);
    }

    cancel_repeating_timer(&wifi_connect_timer);
    wifi_connect_led_on = false;
    gpio_put(LED_PIN, 0);
    cyw43_wifi_pm(&cyw43_state, CYW43_NO_POWERSAVE_MODE);


    trigger_led_alarm = hardware_alarm_claim_unused(true);
    hardware_alarm_set_callback(trigger_led_alarm, turn_off_trigger_led);

    // PWM test signal: choose an integer divider so the period fits the
    // 16-bit counter, then round the period to the nearest 100 Hz cycle.
    gpio_set_function(PWM_PIN, GPIO_FUNC_PWM);
    uint slice_num = pwm_gpio_to_slice_num(PWM_PIN);
    uint chan = pwm_gpio_to_channel(PWM_PIN);
    constexpr uint32_t pwm_frequency_hz = 100;
    constexpr uint32_t pwm_max_period = UINT16_MAX + 1u;
    const uint32_t sys_clock_hz = clock_get_hz(clk_sys);
    const uint32_t pwm_divider =
        (sys_clock_hz + pwm_frequency_hz * pwm_max_period - 1) /
        (pwm_frequency_hz * pwm_max_period);
    const uint32_t pwm_period =
        (sys_clock_hz + pwm_divider * pwm_frequency_hz / 2) /
        (pwm_divider * pwm_frequency_hz);
    pwm_set_clkdiv(slice_num, static_cast<float>(pwm_divider));
    pwm_set_wrap(slice_num, static_cast<uint16_t>(pwm_period - 1));
    pwm_set_chan_level(
        slice_num, chan, static_cast<uint16_t>(pwm_period / 2));
    pwm_set_enabled(slice_num, true);

    adc_init();
    adc_gpio_init(26);
    adc_select_input(0);

    const float conversion_factor = 3.3f / (1 << 12);
    uint16_t result;
    float voltage;

    uint8_t sample_buffer_index = 0;
    queue_init(&sample_fifo, sizeof(uint8_t), NUM_RING_BUFFERS);

//     multicore_reset_core1();
    multicore_launch_core1(wifi_worker);

    dma_chan0 = dma_claim_unused_channel(true);
    dma_chan1 = dma_claim_unused_channel(true);

    dma_channel_config cfg0 = dma_channel_get_default_config(dma_chan0);
    channel_config_set_transfer_data_size(&cfg0, DMA_SIZE_16);
    channel_config_set_read_increment(&cfg0, false); // Static FIFO
    channel_config_set_write_increment(&cfg0, true);
    channel_config_set_dreq(&cfg0, DREQ_ADC);
    // static void channel_config_set_chain_to (dma_channel_config_t * c, uint chain_to)
    // As soon as tranfer_count = SAMPLE_BUFFER_SIZE, we trigger Channel 1
    channel_config_set_chain_to(&cfg0, dma_chan1);

    dma_channel_config cfg1 = dma_channel_get_default_config(dma_chan1);
    channel_config_set_transfer_data_size(&cfg1, DMA_SIZE_16);
    channel_config_set_read_increment(&cfg1, false);
    channel_config_set_write_increment(&cfg1, true);
    channel_config_set_dreq(&cfg1, DREQ_ADC);
    channel_config_set_chain_to(&cfg1, dma_chan0);


    // static void adc_fifo_setup (bool en, bool dreq_en, uint16_t dreq_thresh, bool err_in_fifo, bool byte_shift)
    adc_fifo_setup(true, true, 1, false, false);
    adc_set_clkdiv(0); // Max speed 500 ksps

    adc_fifo_drain();

    // Channel 0, when done we IRQ
    // static void dma_channel_configure (uint channel, const dma_channel_config_t * config, volatile void * write_addr, const volatile void * read_addr, uint32_t encoded_transfer_count, bool trigger)
    dma_channel_configure(dma_chan0, &cfg0, &sample_buffers[0], &adc_hw->fifo, dma_encode_transfer_count(SAMPLE_BUFFER_SIZE), false);
    // static void dma_irqn_set_channel_enabled (uint irq_index, uint channel, bool enabled)
    dma_irqn_set_channel_enabled(0, dma_chan0, true);
    
    // Channel 1
    dma_channel_configure(dma_chan1, &cfg1, &sample_buffers[1], &adc_hw->fifo, dma_encode_transfer_count(SAMPLE_BUFFER_SIZE), false);
    dma_irqn_set_channel_enabled(0, dma_chan1, true);

    
    irq_set_exclusive_handler(DMA_IRQ_0, dma_irq_handler);
    irq_set_enabled(DMA_IRQ_0, true);
    dma_channel_start(dma_chan0);
    adc_run(true);

    uint32_t procO = 0;
    uint32_t lastprint1 = time_us_32();
    uint32_t lastoverflowcount = 0;
    
    while (true) {
        if (acquisition_stats_reset_requested) {
            overflow_count = 0;
            procO = 0;
            lastoverflowcount = 0;
            acquisition_stats_reset_requested = false;
        }
        if (acquisition_pause_requested && !acquisition_stopped) {
            irq_set_enabled(DMA_IRQ_0, false);
            adc_run(false);
            dma_channel_abort(dma_chan0);
            dma_channel_abort(dma_chan1);
            dma_irqn_acknowledge_channel(0, dma_chan0);
            dma_irqn_acknowledge_channel(0, dma_chan1);
            adc_fifo_drain();
            uint8_t stale_buffer;
            while (queue_try_remove(&sample_fifo, &stale_buffer)) {
            }
            next_sample_buffer = 2;
            dma_chan_buffer[0] = 0;
            dma_chan_buffer[1] = 1;
            dma_channel_set_write_addr(dma_chan0, sample_buffers[0], false);
            dma_channel_set_transfer_count(
                dma_chan0, dma_encode_transfer_count(SAMPLE_BUFFER_SIZE), false);
            dma_channel_set_write_addr(dma_chan1, sample_buffers[1], false);
            dma_channel_set_transfer_count(
                dma_chan1, dma_encode_transfer_count(SAMPLE_BUFFER_SIZE), false);
            acquisition_stopped = true;
            acquisition_paused = true;
        } else if (!acquisition_pause_requested && acquisition_stopped) {
            if (acquisition_init_reset_requested) {
                adc_init();
                adc_gpio_init(26);
                adc_select_input(0);
                adc_fifo_setup(true, true, 1, false, false);
                const float adc_clock_hz =
                    static_cast<float>(clock_get_hz(clk_adc));
                adc_set_clkdiv(
                    adc_clock_hz / requested_sample_rate - 1.0f);
                acquisition_init_reset_requested = false;
            }
            adc_fifo_drain();
            irq_set_enabled(DMA_IRQ_0, true);
            dma_channel_start(dma_chan0);
            adc_run(true);
            acquisition_stopped = false;
            acquisition_paused = false;
        }

        static uint32_t applied_sample_rate = 500000;
        const uint32_t sample_rate = requested_sample_rate;
        if (sample_rate != applied_sample_rate) {
            const float adc_clock_hz = static_cast<float>(clock_get_hz(clk_adc));
            adc_set_clkdiv(adc_clock_hz / sample_rate - 1.0f);
            applied_sample_rate = sample_rate;
        }

        if (procO < overflow_count) {
            if (overflow_count-procO > 5) {
                printf("X");
            } else {
                for (int i = 0; i<(overflow_count-procO); i++) {
                    printf("O");
                }
            }
            procO = overflow_count;
        }
        const uint32_t now_us = time_us_32();
        if ((now_us - lastprint1) > 1000000) {
            // once per second
            if (lastoverflowcount < overflow_count) {
                printf("\n%u Overflows\n", (overflow_count-lastoverflowcount));
                lastoverflowcount = overflow_count;
            }
            lastprint1 = now_us;
        }
        sleep_ms(1000/120);
    }

}
