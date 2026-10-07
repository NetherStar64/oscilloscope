#ifndef _LWIPOPTS_H
#define _LWIPOPTS_H

// Bare-metal / IRQ background mode (no FreeRTOS)
#define NO_SYS                      1

// Memory settings
#define MEM_ALIGNMENT               4
#define MEM_SIZE                    65536
#define MEMP_NUM_TCP_SEG            192
#define MEMP_NUM_ARP_QUEUE          10
#define PBUF_POOL_SIZE              32
#define PBUF_POOL_BUFSIZE           1536
#define TCP_MSS                     1460
#define TCP_SND_BUF                 (44 * TCP_MSS)
#define LWIP_TCP_RTO_TIME           1000

// Protocol enabling
#define LWIP_ARP                    1
#define LWIP_ETHERNET               1
#define LWIP_ICMP                   1
#define LWIP_RAW                    1
#define LWIP_DHCP                   1
#define LWIP_UDP                    1
#define LWIP_TCP                    1

// Disable BSD sockets (use lwIP lightweight raw/callback API)
#define LWIP_SOCKET                 0
#define LWIP_NETCONN                0

// Checksums handled in software
#define CHECKSUM_GEN_IP             1
#define CHECKSUM_GEN_UDP            1
#define CHECKSUM_GEN_TCP            1
#define CHECKSUM_CHECK_IP           1
#define CHECKSUM_CHECK_UDP          1
#define CHECKSUM_CHECK_TCP          1

#endif