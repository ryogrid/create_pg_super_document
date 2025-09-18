# macaddr_send

## Location
src/backend/utils/adt/mac.c: 161 - 181

## Overview
This function converts PostgreSQL's internal macaddr data type to external binary format for transmission over the network protocol.

## Definition
```c
Datum macaddr_send(PG_FUNCTION_ARGS)
```

## Detailed Description
The `macaddr_send` function is the binary output function for PostgreSQL's macaddr data type. It converts an internal macaddr structure to external binary representation for transmission through the PostgreSQL wire protocol. The function serializes the MAC address as a sequence of 6 bytes in Most Significant Byte (MSB) first order, which is the standard network byte order for MAC addresses.

This function is the counterpart to `macaddr_recv` and is used when MAC address values need to be transmitted in binary format, such as in prepared statements with binary result transmission or when clients request binary-formatted query results for better performance.

## Parameters / Member Variables
- `addr`: Pointer to the input macaddr structure (retrieved via `PG_GETARG_MACADDR_P(0)`)
- `buf`: StringInfoData structure for building the binary output message

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_MACADDR_P`: PostgreSQL macro to extract macaddr pointer from function arguments
  - `[pq_begintypsend](../p/pq_begintypsend.md)`: PostgreSQL function to initialize binary output buffer
  - `[pq_sendbyte](../p/pq_sendbyte.md)`: PostgreSQL function to append a single byte to the output buffer
  - `[pq_endtypsend](../p/pq_endtypsend.md)`: PostgreSQL function to finalize binary output buffer
  - `PG_RETURN_BYTEA_P`: PostgreSQL macro to return binary data as bytea
- Called from (representative examples):
  - No direct references found in the codebase (likely called via PostgreSQL type system during binary protocol operations)

## Notes and Other Information
- Outputs exactly 6 bytes in MSB-first (big-endian) order
- Uses PostgreSQL's standard binary output protocol infrastructure
- Complementary to `macaddr_recv` function which performs the reverse operation
- The resulting binary format is more compact and efficient than text representation
- Part of PostgreSQL's type system support for binary I/O protocol
- The output buffer management is handled automatically by the pq_* functions
- Returns the binary data wrapped in PostgreSQL's bytea format