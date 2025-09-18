# macaddr_recv

## Location
src/backend/utils/adt/mac.c: 140 - 160

## Overview
This function converts an external binary representation of a MAC address (6 bytes in network byte order) to PostgreSQL's internal macaddr data type structure.

## Definition
```c
Datum macaddr_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
The `macaddr_recv` function is the binary input function for PostgreSQL's macaddr data type. It receives MAC address data in external binary format over the network protocol and converts it to the internal macaddr structure. The external representation consists of exactly 6 bytes transmitted in Most Significant Byte (MSB) first order, which corresponds to the natural byte order of MAC addresses.

This function is part of PostgreSQL's type system infrastructure and is used when MAC address values are transmitted in binary format through the PostgreSQL wire protocol, such as when using prepared statements with binary parameter transmission or when retrieving binary-formatted query results.

## Parameters / Member Variables
- `buf`: StringInfo buffer containing the binary data (retrieved via `PG_GETARG_POINTER(0)`)
- `addr`: Pointer to the newly allocated macaddr structure to store the result

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POINTER`: PostgreSQL macro to extract pointer from function arguments
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md): PostgreSQL function to read a single byte from message buffer
  - `PG_RETURN_MACADDR_P`: PostgreSQL macro to return macaddr pointer
- Called from (representative examples):
  - No direct references found in the codebase (likely called via PostgreSQL type system during binary protocol operations)

## Notes and Other Information
- Reads exactly 6 bytes from the input buffer in sequence (a, b, c, d, e, f)
- Uses MSB-first (big-endian) byte order, which is the network standard for MAC addresses
- No validation is performed on the input bytes since all 8-bit values are valid for MAC address octets
- Complementary to `macaddr_send` function which performs the reverse operation
- Memory allocation follows PostgreSQL conventions using palloc
- Part of the binary I/O protocol support for efficient data transmission