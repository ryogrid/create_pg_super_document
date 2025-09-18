# pq_peekbyte

## Location
src/backend/libpq/pqcomm.c: 982 - 1002

## Overview
Examines the next byte from the client connection without consuming it, allowing for lookahead in protocol message parsing.

## Definition
int pq_peekbyte(void)

## Detailed Description
pq_peekbyte provides a non-destructive way to examine the next byte in the receive buffer without advancing the buffer pointer. This function is identical to pq_getbyte() except that it does not increment PqRecvPointer, allowing the same byte to be read again later. It's particularly useful for protocol parsing where the next byte's value determines how to process subsequent data. Like pq_getbyte, it automatically refills the buffer when necessary.

## Parameters / Member Variables
- No parameters (operates on global variables)

## Dependencies
- Functions called/Symbols referenced:
  - [pq_recvbuf](pq_recvbuf.md)
- Called from (representative examples):
  - [ProcessSSLStartup](../P/ProcessSSLStartup.md)

## Notes and Other Information
- Asserts that PqCommReadingMsg is true to ensure proper message reading state
- Returns the byte value as an unsigned char cast to int, or EOF on failure
- Does not advance the buffer pointer, allowing repeated peeks at the same byte
- Essential for protocol decision points where the next byte determines parsing strategy
- Commonly used in SSL/TLS handshake processing and protocol negotiation