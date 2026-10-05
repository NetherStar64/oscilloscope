#include <stdio.h>
#include "pico/stdlib.h"
#include "hardware/gpio.h"
#include "hardware/adc.h"
#include "hardware/dma.h"
#include "pico/cyw43_arch.h"
#include "hardware/pwm.h"
#include "pico/multicore.h"
#include "core1_worker.h"
#include "config.h"
#include "wifipassword.h"

#define LED_PIN 16
#define DIV_TOGGLE_PIN 15
#define PWM_PIN 2

volatile bool enable_div = false;


uint16_t sample_buffers[2][SAMPLE_BUFFER_SIZE];


void toggle_div(uint gpio, uint32_t events) {
    static uint32_t last_toggle_us = 0;
    const uint32_t now_us = time_us_32();

    if ((uint32_t)(now_us - last_toggle_us) >= 300000) {
        enable_div = !enable_div;
        last_toggle_us = now_us;
    }
}


int main()
{
    stdio_init_all();
    printf("=====================\n");
    printf("Init Oscilloscope\n");

    cyw43_arch_init_with_country(WIFI_COUNTRY);
    cyw43_arch_enable_sta_mode();
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

    gpio_init(LED_PIN);
    gpio_set_dir(LED_PIN, GPIO_OUT);
    gpio_put(LED_PIN, 1);

    gpio_init(DIV_TOGGLE_PIN);
    gpio_set_dir(DIV_TOGGLE_PIN, GPIO_IN);
    gpio_pull_up(DIV_TOGGLE_PIN);
    gpio_set_irq_enabled_with_callback(15, GPIO_IRQ_EDGE_FALL, true, toggle_div);

    gpio_set_function(PWM_PIN, GPIO_FUNC_PWM);
    uint slice_num = pwm_gpio_to_slice_num(PWM_PIN);
    uint chan = pwm_gpio_to_channel(PWM_PIN);
    // 4. Set the clock divider to slow down the 125MHz base clock
    // 125,000,000 / 2.0 = 6,250,000 Hz internal counter frequency
    pwm_set_clkdiv(slice_num, 20.0f);
    // 5. Set the wrap value (period)
    // 6,250,000 Hz / 62,500 cycles = 100 Hz signal frequency
    pwm_set_wrap(slice_num, 62499);
    pwm_set_chan_level(slice_num, chan, 31250);
    pwm_set_enabled(slice_num, true);

    adc_init();
    adc_gpio_init(26);
    adc_select_input(0);

    const float conversion_factor = 3.3f / (1 << 12);
    uint16_t result;
    float voltage;

    uint8_t sample_buffer_index = 0;

//     multicore_reset_core1();
    multicore_launch_core1(wifi_worker);

    int dma_chan = dma_claim_unused_channel(true);
    dma_channel_config cfg = dma_channel_get_default_config(dma_chan);
    channel_config_set_transfer_data_size(&cfg, DMA_SIZE_16);
    channel_config_set_read_increment(&cfg, false);
    channel_config_set_write_increment(&cfg, true);
    channel_config_set_dreq(&cfg, DREQ_ADC);
    adc_fifo_setup(true, true, 1, false, false);
    adc_set_clkdiv(0);

    bool firstsample = true;

    while (true) {
        // result = adc_read();
        // if (enable_div) {
        //     voltage = result * conversion_factor *2;
        // } else {
        //     voltage = result * conversion_factor;
        // }
        // printf("Raw ADC Value: %04d, Div %s Voltage: %.3f V\n", result, enable_div? "X" : " ", voltage);
        adc_fifo_drain();
        adc_run(true);
        dma_channel_configure(dma_chan, &cfg, &sample_buffers[sample_buffer_index], &adc_hw->fifo, 1024, true);
        dma_channel_wait_for_finish_blocking(dma_chan);
        adc_run(false);
        
        if (!firstsample) {
            multicore_fifo_pop_blocking(); // Wait for done signal of Core 1
        } else {
            firstsample = false;
        }
        multicore_fifo_push_blocking(sample_buffer_index);
        // Swap buffer
        if (sample_buffer_index == 0) {
            sample_buffer_index = 1;
        } else {
            sample_buffer_index = 0;
        }
        
        tight_loop_contents();
    }

    
}


