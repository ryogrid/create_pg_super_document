# pg_lsn_send

## Location
[src/backend/utils/adt/pg_lsn.c:102-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_lsn.c#L102-L117)

## Overview
A PostgreSQL send function that serializes a pg_lsn value into binary format for network communication or binary data transfer.

## Definition
```c
Datum pg_lsn_send(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the binary output function for the pg_lsn data type in PostgreSQL's type system. It serializes LSN values into their binary representation for efficient network communication between PostgreSQL clients and servers, or when writing binary-format data. The function converts an XLogRecPtr value to a 64-bit integer and writes it to a binary message buffer using PostgreSQL's binary protocol functions.

The function follows PostgreSQL's standard binary send protocol: it initializes a StringInfo buffer, writes the 64-bit LSN value to it, and returns the buffer as a bytea datum. This provides efficient data transfer compared to text-based serialization and complements the pg_lsn_recv function for complete binary I/O support.

## Parameters / Member Variables
- Function arguments accessed via PG_FUNCTION_ARGS macro:
  - `lsn`: XLogRecPtr value to serialize (accessed via PG_GETARG_LSN(0))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN (extracts LSN argument from function args)
  - [pq_begintypsend](pq_begintypsend.md) (initializes binary output buffer)
  - [pq_sendint64](pq_sendint64.md) (writes 64-bit integer to binary buffer)
  - [pq_endtypsend](pq_endtypsend.md) (finalizes binary output buffer)
  - PG_RETURN_BYTEA_P (returns binary data as bytea Datum)

- Called from (representative examples):
  - No direct references found (typically called by PostgreSQL's binary protocol system)

## Notes and Other Information
- This is the official binary send function registered in PostgreSQL's type system for pg_lsn
- Used by PostgreSQL's binary protocol for efficient data transfer
- Follows PostgreSQL's function calling convention for type binary I/O functions
- The binary format represents LSN as a 64-bit integer in network byte order
- Essential for client-server communication when using binary protocol
- Paired with pg_lsn_recv for complete binary serialization support
- More efficient than text-based formatting for bulk data operations
- The output bytea can be transmitted over the network or stored in binary format

## Simplified Source

```c
Datum pg_lsn_send(PG_FUNCTION_ARGS) {
    // Extract LSN value from function arguments
    XLogRecPtr lsn = PG_GETARG_LSN(0);
    StringInfoData buf;

    // Initialize binary output buffer
    pq_begintypsend(&buf);

    // Write 64-bit LSN value to buffer
    pq_sendint64(&buf, lsn);

    // Return finalized buffer as bytea
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
```