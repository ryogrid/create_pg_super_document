# pg_md5_ctx

## Location
[src/common/md5_int.h:78-85](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/md5_int.h#L78-L85)

## Overview
The  structure is the context data structure for MD5 hash computation in PostgreSQL. It maintains the internal state needed for incremental MD5 hashing operations, allowing data to be processed in chunks.

## Definition

```c
#define md5_n8  md5_count.md5_count8
```
## Detailed Description
The  structure serves as the context for PostgreSQL's fallback MD5 implementation. It stores the intermediate state of an MD5 hash computation, enabling the hash to be calculated incrementally over multiple data chunks rather than requiring all data to be available at once.

The structure contains the MD5 algorithm's four 32-bit state variables (A, B, C, D), a byte counter tracking the total amount of data processed, an index for buffer management, and a 64-byte buffer for holding partial blocks of input data. The union-based design allows the same data to be accessed as either 32-bit words or individual bytes, facilitating both the algorithm's mathematical operations and byte-level data manipulation.

This context is used within PostgreSQL's unified cryptographic hash interface () and provides the foundation for MD5-based operations throughout the system, including authentication, data integrity checks, and other cryptographic functions.

## Parameters / Member Variables
- : Union containing the MD5 algorithm's four state variables
  - : Four 32-bit state variables (A, B, C, D) for MD5 computation
  - : Same data accessible as 16 individual bytes
- : Union for tracking processed data length
  - : 64-bit counter of total bytes processed
  - : Same counter accessible as 8 individual bytes
- : Index into the buffer for managing partial blocks
- : 64-byte buffer for storing incomplete data blocks

## Dependencies
- Functions called/Symbols referenced:
  - MD5_BUFLEN (constant defining buffer size)
  - uint32, uint8, uint64 (standard integer types)
- Called from (representative examples):
  - pg_cryptohash_ctx (used as union member for unified hash interface)
  - pg_md5_init (initializes the context)
  - pg_md5_update (updates context with new data)
  - pg_md5_final (finalizes hash computation)
  - md5_calc (internal MD5 calculation functions)
  - md5_pad (internal MD5 padding functions)
  - md5_result (internal MD5 result functions)

## Notes and Other Information
- This structure is part of PostgreSQL's fallback MD5 implementation, used when system-provided cryptographic libraries are not available or suitable
- The union-based design enables efficient access to state data as both 32-bit words (for algorithm operations) and bytes (for data manipulation)
- The context supports incremental hashing, allowing large data sets to be processed in chunks without loading everything into memory
- Buffer size is defined by MD5_BUFLEN constant (64 bytes), matching the MD5 algorithm's block size
- Macros (md5_sta, md5_stb, etc.) provide convenient access to individual state components
- This is an internal implementation detail; external code should use the higher-level pg_cryptohash_* interface when possible