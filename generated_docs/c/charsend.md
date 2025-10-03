# charsend

## Location
[src/backend/utils/adt/char.c:105-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/char.c#L105-L126)

## Overview
Converts a character value to PostgreSQL's external binary format for transmission, encoding it as a single byte in a bytea structure.

## Definition

```c
Datum
charsend(PG_FUNCTION_ARGS)
```
## Detailed Description
The charsend function is the binary output (send) function for PostgreSQL's "char" data type. It is part of the binary protocol support that allows efficient transmission of data between PostgreSQL servers and clients without text formatting overhead.

The function takes a character value and converts it to PostgreSQL's external binary representation by:
1. Initializing a StringInfo buffer using pq_begintypsend()
2. Writing the single character byte to the buffer using pq_sendbyte()  
3. Finalizing the buffer and returning it as a bytea using pq_endtypsend()

This creates a binary representation that can be efficiently transmitted over the network and later reconstructed using the corresponding charrecv() function.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CHAR (to extract input character)
  - [pq_begintypsend](../p/pq_begintypsend.md) (to initialize binary output buffer)
  - [pq_sendbyte](../p/pq_sendbyte.md) (to write the character byte to the buffer)
  - [pq_endtypsend](../p/pq_endtypsend.md) (to finalize the buffer and prepare for output)
  - PG_RETURN_BYTEA_P (to return the bytea result)
- Called from (representative examples):
  - PostgreSQL binary protocol handling
  - Prepared statements using binary format for "char" type parameters

## Notes and Other Information
- The external binary representation consists of exactly one byte
- No character set conversion is performed - the byte value is transmitted as-is
- This function is the counterpart to charrecv() for round-trip binary serialization
- The resulting bytea contains the raw byte representation suitable for network transmission
- Used when clients request binary format (format code 1) instead of text format
- Part of PostgreSQL's type system infrastructure for efficient binary I/O operations
- The pq_begintypsend/pq_endtypsend functions handle the bytea header and length information