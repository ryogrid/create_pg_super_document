# macaddr8_recv

## Location
src/backend/utils/adt/mac8.c: 254 - 286

## Overview
A PostgreSQL binary input function that converts external binary format MAC addresses (both EUI-48 and EUI-64) from network protocol messages into the internal macaddr8 structure.

## Definition
```c
Datum macaddr8_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
This function handles the binary protocol representation of MAC addresses received over PostgreSQL's network protocol. It reads raw bytes from a StringInfo buffer in Most Significant Byte (MSB) first order and constructs a macaddr8 structure. The function automatically detects whether the input is a 6-byte (EUI-48) or 8-byte (EUI-64) format based on the buffer length.

For 6-byte inputs, the function performs the standard EUI-48 to EUI-64 conversion by inserting the bytes 0xFF and 0xFE in the middle of the address (after the 3rd byte). This maintains compatibility with traditional MAC addresses while storing them in the extended 64-bit format.

The function expects the bytes in network byte order (big-endian) and reads them sequentially using the `pq_getmsgbyte` function, which is part of PostgreSQL's message processing infrastructure.

## Parameters / Member Variables
- Uses the standard PostgreSQL function calling convention via `PG_FUNCTION_ARGS`
- `buf`: StringInfo buffer containing the binary MAC address data (accessed via `PG_GETARG_POINTER(0)`)

## Dependencies
- Functions called/Symbols referenced:
  - `macaddr8`: The target data structure for storing the parsed address
  - `pq_getmsgbyte`: PostgreSQL function for reading bytes from protocol messages (called 6-8 times)
  - `PG_RETURN_MACADDR8_P`: PostgreSQL macro for returning macaddr8 values
  - `palloc0`: PostgreSQL memory allocation function (zero-initialized)
- Called from:
  - PostgreSQL binary protocol handler (automatically called when receiving binary macaddr8 data)

## Notes and Other Information
- Handles both 6-byte and 8-byte binary input formats automatically
- Uses buffer length detection (`buf->len == 6`) to determine input format
- For 6-byte input: reads 3 bytes, inserts FF-FE, then reads remaining 3 bytes
- For 8-byte input: reads all 8 bytes directly
- Part of PostgreSQL's binary I/O protocol support for efficient network transmission
- The function assumes valid input and doesn't perform extensive error checking
- Returns a Datum containing a pointer to the newly allocated macaddr8 structure
- Used primarily for client-server communication when binary protocol mode is enabled