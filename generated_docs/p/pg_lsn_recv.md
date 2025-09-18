# pg_lsn_recv

## Location
src/backend/utils/adt/pg_lsn.c: 92 - 101

## Overview
A PostgreSQL receive function that deserializes a pg_lsn value from binary format during network communication or binary data transfer.

## Definition
```c
Datum pg_lsn_recv(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the binary input function for the pg_lsn data type in PostgreSQL's type system. It is used to deserialize LSN values from their binary representation, typically during network communication between PostgreSQL clients and servers, or when reading binary-format data. The function reads a 64-bit integer from a StringInfo buffer and converts it directly to an XLogRecPtr value.

The function is part of PostgreSQL's binary I/O system, which provides more efficient data transfer compared to text-based serialization. It complements the text-based pg_lsn_in function by handling binary protocol communication.

## Parameters / Member Variables
- Function arguments accessed via PG_FUNCTION_ARGS macro:
  - `buf`: StringInfo buffer containing binary data (accessed via PG_GETARG_POINTER(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (extracts pointer argument from function args)
  - pq_getmsgint64 (reads 64-bit integer from binary message buffer)
  - PG_RETURN_LSN (returns LSN value as Datum)

- Called from (representative examples):
  - No direct references found (typically called by PostgreSQL's binary protocol system)

## Notes and Other Information
- This is the official binary receive function registered in PostgreSQL's type system for pg_lsn
- Used by PostgreSQL's binary protocol for efficient data transfer
- Follows PostgreSQL's function calling convention for type binary I/O functions
- The binary format represents LSN as a 64-bit integer in network byte order
- Essential for client-server communication when using binary protocol
- Paired with pg_lsn_send for complete binary serialization support
- More efficient than text-based parsing for bulk data operations