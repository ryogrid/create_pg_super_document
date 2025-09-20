# float4send

## Location
[src/backend/utils/adt/float.c:343-356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L343-L356)

## Overview
PostgreSQL function that converts a float4 value to binary format for external transmission or storage in network byte order.

## Definition

```c
Datum
float4send(PG_FUNCTION_ARGS)
```
## Detailed Description
The float4send function is part of PostgreSQL's binary I/O system for the float4 data type. It converts internal float4 values to a standardized external binary format suitable for network transmission, binary file storage, or binary protocol communication. The function creates a binary representation that can be transmitted across different architectures and later reconstructed using float4recv. It handles byte order conversion to ensure consistent binary format regardless of the host system's endianness.

The function uses PostgreSQL's StringInfoData buffer system along with the pq_begintypsend/pq_sendfloat4/pq_endtypsend sequence to create a properly formatted binary message containing the float4 value.

## Parameters / Member Variables
- Function accepts PostgreSQL function arguments via PG_FUNCTION_ARGS macro
- : The float4 value extracted from the first function argument using PG_GETARG_FLOAT4(0)
- : Local StringInfoData buffer used for building the binary output

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (argument extraction)
  - [pq_begintypsend](../p/pq_begintypsend.md) (initialize binary output buffer)
  - [pq_sendfloat4](../p/pq_sendfloat4.md) (write float4 in binary format)
  - [pq_endtypsend](../p/pq_endtypsend.md) (finalize binary output buffer)
  - PG_RETURN_BYTEA_P (return bytea value macro)
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL's function call mechanism for binary I/O)

## Notes and Other Information
- Part of PostgreSQL's binary I/O infrastructure for efficient data transmission
- Produces binary output compatible with float4recv for round-trip conversion
- Handles endianness conversion automatically for cross-platform compatibility
- Used in binary protocol communications between client and server
- Used in COPY BINARY operations for efficient bulk data transfer
- Registered in PostgreSQL's system catalogs as the binary output function for float4 type