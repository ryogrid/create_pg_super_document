# boolrecv

## Location
[src/backend/utils/adt/bool.c:174-186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/bool.c#L174-L186)

## Overview
PostgreSQL binary input function for the boolean data type that converts external binary format to internal boolean values using a single-byte representation.

## Definition

```c
Datum
boolrecv(PG_FUNCTION_ARGS)
```
## Detailed Description
The `boolrecv` function serves as the binary input conversion function for PostgreSQL's boolean data type. It is used by the PostgreSQL protocol and binary data exchange mechanisms to convert boolean values from their external binary representation to internal boolean values. The function reads exactly one byte from the input buffer using PostgreSQL's message parsing system. Any non-zero byte value is interpreted as true, while a zero byte represents false. This follows a common binary boolean convention where any non-zero value indicates truth.

## Parameters / Member Variables
- Input parameter accessed via `PG_GETARG_POINTER(0)`: StringInfo buffer containing the binary data to parse

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINTER (PostgreSQL function argument extraction macro for pointers)
  - [pq_getmsgbyte](../p/pq_getmsgbyte.md) (PostgreSQL message parsing function to extract a byte)
  - PG_RETURN_BOOL (PostgreSQL return value macro for boolean)
- Called from:
  - PostgreSQL binary protocol system (no direct references in indexed code)

## Notes and Other Information
- This is a PostgreSQL "receive function" registered in the system catalogs for the boolean data type
- Used primarily by the PostgreSQL binary protocol for efficient data transmission
- Automatically invoked during binary COPY operations, prepared statement execution, and binary result formatting
- The external binary format uses exactly one byte per boolean value
- Any non-zero byte value (1-255) is considered true, zero is false
- This convention provides flexibility in binary data sources while maintaining clear semantics
- Part of PostgreSQL's type system infrastructure for binary serialization/deserialization
- The function signature follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS
- Complements `boolsend` function (not in target list) for complete binary I/O support

## Simplified Source

```c
Datum
boolrecv(PG_FUNCTION_ARGS)
{
    StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
    int ext;

    // Read one byte from buffer
    ext = pq_getmsgbyte(buf);

    // Return true if non-zero, false if zero
    PG_RETURN_BOOL(ext != 0);
}
```