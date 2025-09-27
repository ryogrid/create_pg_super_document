# pq_getbytes

## Location
[src/backend/libpq/pqcomm.c:1062-1095](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1062-L1095)

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
  - [pq_recvbuf](pq_recvbuf.md)
- Called from (representative examples):
  - [secure_open_server](../s/secure_open_server.md)
  - [pq_getmessage](pq_getmessage.md)
  - [ProcessStartupPacket](../P/ProcessStartupPacket.md)

## Notes and Other Information
- Returns 0 on success, EOF if unable to read the requested amount of data
- Efficiently handles partial buffer contents by copying in optimal chunks
- Asserts that PqCommReadingMsg is true to ensure proper message reading state
- Critical for reading protocol message bodies and startup packets
- Uses memcpy for efficient bulk data transfer from buffer to destination
- Automatically manages buffer pointer advancement and length tracking

## Simplified Source

```c
// Simplified version of pq_getbytes
int pq_getbytes(char *destination_buffer, size_t bytes_to_read) {
    size_t chunk_size;

    // Assert we're in message reading mode
    Assert(PqCommReadingMsg);

    // Read data in chunks until we have all requested bytes
    while (bytes_to_read > 0) {

        // Refill buffer if empty
        while (current_buffer_position >= buffer_end_position) {
            if (pq_recvbuf()) {  // Try to receive more data
                return EOF;      // Failed to get more data
            }
        }

        // Calculate how much we can copy from current buffer
        chunk_size = buffer_end_position - current_buffer_position;
        if (chunk_size > bytes_to_read) {
            chunk_size = bytes_to_read;
        }

        // Copy data from buffer to destination
        memcpy(destination_buffer, receive_buffer + current_buffer_position, chunk_size);

        // Update positions and counters
        current_buffer_position += chunk_size;
        destination_buffer += chunk_size;
        bytes_to_read -= chunk_size;
    }

    return 0;  // Success
}
```

Key simplifications made:
- Used more descriptive variable names (destination_buffer, bytes_to_read, chunk_size)
- Added explanatory comments for each major logic block
- Abstracted buffer pointer variables with clearer names
- Focused on the main execution path: refill buffer when needed, copy chunks, repeat
- Removed detailed pointer arithmetic complexity while preserving the core algorithm
- Emphasized the chunked reading strategy that handles partial buffer contents