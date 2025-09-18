# brin_bloom_summary_send

## Location
[src/backend/access/brin/brin_bloom.c:840-843](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_bloom.c#L840-L843)

## Overview
Binary output function for the brin_bloom_summary PostgreSQL data type that serializes the internal bloom filter data to binary format by delegating to the standard bytea send function.

## Definition
```c
Datum brin_bloom_summary_send(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the binary output routine for the brin_bloom_summary data type. Unlike the input functions (brin_bloom_summary_in and brin_bloom_summary_recv) which reject external creation of bloom summary values, this function allows the binary serialization of existing bloom summaries. It leverages the fact that BRIN bloom summaries are internally stored in a binary format compatible with bytea, so it simply delegates the serialization work to PostgreSQL's standard byteasend function.

## Parameters / Member Variables
- No direct parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface)
- Receives a brin_bloom_summary datum to serialize
- Passes the function call info (fcinfo) directly to byteasend

## Dependencies
- Functions called/Symbols referenced:
  - [byteasend](byteasend.md) (PostgreSQL's standard binary output function for bytea type)
- Called from (representative examples):
  - PostgreSQL type system (during binary output operations like network protocol transfers)
  - COPY TO BINARY operations
  - Client-server protocol binary data transmission

## Notes and Other Information
- This function allows binary output while still preventing binary input, creating a asymmetric access pattern
- The bloom summary data is internally stored in a format compatible with bytea serialization
- This enables BRIN bloom summaries to be transmitted over PostgreSQL's binary protocol
- Unlike text output (brin_bloom_summary_out), this preserves the exact binary representation
- The delegation to byteasend ensures compatibility with PostgreSQL's standard binary serialization infrastructure
- This function is essential for operations that need to transfer bloom summaries between processes or over network connections