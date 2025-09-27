# pq_getbyte

## Location
[src/backend/libpq/pqcomm.c:963-981](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L963-L981)

## Overview
Retrieves a single byte from the client connection, automatically refilling the receive buffer when necessary.

## Definition
int pq_getbyte(void)

## Detailed Description
pq_getbyte is the primary function for reading individual bytes from a PostgreSQL client connection. It operates on the global receive buffer and automatically calls pq_recvbuf() to refill the buffer when no data is available. The function advances the buffer pointer after reading each byte, ensuring sequential access to the incoming data stream. This function is essential for protocol message parsing where byte-by-byte reading is required.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [pq_recvbuf](pq_recvbuf.md)
- Called from (representative examples):
  - [CopyGetData](../C/CopyGetData.md)
  - [CheckSASLAuth](../C/CheckSASLAuth.md)
  - [recv_password_packet](../r/recv_password_packet.md)
  - [pg_GSS_recvauth](pg_GSS_recvauth.md)
  - [pg_SSPI_recvauth](pg_SSPI_recvauth.md)
  - [HandleUploadManifestPacket](../H/HandleUploadManifestPacket.md)
  - [SocketBackend](../S/SocketBackend.md)

## Notes and Other Information
- Asserts that PqCommReadingMsg is true to ensure proper message reading state
- Returns the byte value as an unsigned char cast to int, or EOF on failure
- Automatically manages buffer pointer advancement
- This is a non-static function, available throughout the PostgreSQL backend
- Critical for parsing protocol messages that require byte-level access

## Simplified Source

```c
// Simplified version of pq_getbyte
int pq_getbyte(void) {
    // Ensure we're in message reading state
    Assert(PqCommReadingMsg);

    // Check if buffer needs refilling
    while (PqRecvPointer >= PqRecvLength) {
        // Attempt to receive more data into buffer
        if (pq_recvbuf()) {
            return EOF;  // Failed to receive data
        }
    }

    // Return next byte from buffer and advance pointer
    return (unsigned char) PqRecvBuffer[PqRecvPointer++];
}
```

Key simplifications made:
- Added descriptive comments for each logical step
- Clarified the buffer management logic (check if refill needed)
- Preserved the essential EOF error handling
- Maintained the unsigned char casting for proper byte handling
- Emphasized the automatic pointer advancement