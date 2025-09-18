# macaddr8_send

## Location
[src/backend/utils/adt/mac8.c:287-309](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/mac8.c#L287-L309)

## Overview
A PostgreSQL binary output function that converts the internal macaddr8 structure to binary format for transmission over the network protocol.

## Definition
```c
Datum macaddr8_send(PG_FUNCTION_ARGS)
```

## Detailed Description
This function handles the conversion of macaddr8 data to PostgreSQL's binary wire format for network transmission. It takes a macaddr8 structure and serializes it into a byte array that can be transmitted efficiently over the network protocol. The function always outputs all 8 bytes of the EUI-64 format address in Most Significant Byte (MSB) first order.

The function uses PostgreSQL's type send infrastructure (`pq_begintypsend`, `pq_sendbyte`, `pq_endtypsend`) to build a properly formatted binary message. Each of the 8 bytes (a through h) is written sequentially to create the binary representation.

Unlike the text output function which formats the address as a colon-separated string, this function produces a compact 8-byte binary representation suitable for network transmission when binary protocol mode is enabled.

## Parameters / Member Variables
- Uses the standard PostgreSQL function calling convention via `PG_FUNCTION_ARGS`
- `addr`: Input macaddr8 structure containing the 8-byte MAC address (accessed via `PG_GETARG_MACADDR8_P(0)`)

## Dependencies
- Functions called/Symbols referenced:
  - `macaddr8`: The input data structure containing the MAC address bytes
  - `PG_GETARG_MACADDR8_P`: PostgreSQL macro for extracting macaddr8 arguments
  - [pq_begintypsend](../p/pq_begintypsend.md): PostgreSQL function to initialize binary output buffer
  - [pq_sendbyte](../p/pq_sendbyte.md): PostgreSQL function to write a byte to the output buffer (called 8 times)
  - [pq_endtypsend](../p/pq_endtypsend.md): PostgreSQL function to finalize binary output buffer
  - `PG_RETURN_BYTEA_P`: PostgreSQL macro for returning binary data (bytea type)
- Called from:
  - PostgreSQL binary protocol handler (automatically called when transmitting binary macaddr8 data)

## Notes and Other Information
- Always outputs exactly 8 bytes in network byte order (big-endian)
- Part of PostgreSQL's binary I/O protocol support for efficient network transmission
- Complementary function to `macaddr8_recv` - what this function sends can be received by `macaddr8_recv`
- More efficient than text-based transmission for applications that can handle binary data
- Used primarily in client-server communication when binary protocol mode is enabled
- Returns a Datum containing bytea (binary array) data
- The output format is independent of the original input format (EUI-48 addresses are always transmitted as 8 bytes)