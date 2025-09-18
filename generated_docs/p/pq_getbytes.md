# pq_getbytes

## Location
src/backend/libpq/pqcomm.c: 1062 - 1095

## Overview
Reads a specified number of bytes from the client connection into a provided buffer, handling partial reads and buffer management automatically.

## Definition
int pq_getbytes(char *s, size_t len)

## Detailed Description
pq_getbytes is designed for reading larger chunks of data from the client connection. It efficiently handles cases where the requested data spans multiple buffer fills by using memcpy for bulk copying and automatically calling pq_recvbuf() when the buffer is exhausted. The function operates in a loop, copying available data in chunks until the full requested amount is obtained. This approach minimizes system calls while ensuring all requested data is retrieved.

## Parameters / Member Variables
- `s`: Destination buffer where received bytes will be stored
- `len`: Number of bytes to read from the connection

## Dependencies
- Functions called/Symbols referenced:
  - pq_recvbuf
- Called from (representative examples):
  - secure_open_server
  - pq_getmessage
  - ProcessStartupPacket

## Notes and Other Information
- Returns 0 on success, EOF if unable to read the requested amount of data
- Efficiently handles partial buffer contents by copying in optimal chunks
- Asserts that PqCommReadingMsg is true to ensure proper message reading state
- Critical for reading protocol message bodies and startup packets
- Uses memcpy for efficient bulk data transfer from buffer to destination
- Automatically manages buffer pointer advancement and length tracking