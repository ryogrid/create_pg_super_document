# pq_getbyte

## Location
src/backend/libpq/pqcomm.c: 963 - 981

## Overview
Retrieves a single byte from the client connection, automatically refilling the receive buffer when necessary.

## Definition
int pq_getbyte(void)

## Detailed Description
pq_getbyte is the primary function for reading individual bytes from a PostgreSQL client connection. It operates on the global receive buffer and automatically calls pq_recvbuf() to refill the buffer when no data is available. The function advances the buffer pointer after reading each byte, ensuring sequential access to the incoming data stream. This function is essential for protocol message parsing where byte-by-byte reading is required.

## Parameters / Member Variables
- No parameters (operates on global variables)

## Dependencies
- Functions called/Symbols referenced:
  - pq_recvbuf
- Called from (representative examples):
  - CopyGetData
  - CheckSASLAuth
  - recv_password_packet
  - pg_GSS_recvauth
  - pg_SSPI_recvauth
  - HandleUploadManifestPacket
  - SocketBackend

## Notes and Other Information
- Asserts that PqCommReadingMsg is true to ensure proper message reading state
- Returns the byte value as an unsigned char cast to int, or EOF on failure
- Automatically manages buffer pointer advancement
- This is a non-static function, available throughout the PostgreSQL backend
- Critical for parsing protocol messages that require byte-level access