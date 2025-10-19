# pq_endtypsend

## Location
[src/backend/libpq/pqformat.c:346-366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L346-L366)

## Overview
Finalizes the construction of a bytea result by setting the correct length header and returning the completed bytea value.

## Definition

```c
bytea *
pq_endtypsend(StringInfo buf)
```
## Detailed Description
The  function completes the construction of a bytea value that was initiated with . It takes the StringInfo buffer that has been populated with binary data and converts it into a proper bytea structure by setting the PostgreSQL variable-length header (VARHDRSZ) with the correct total length.

This function assumes that the buffer's data is already properly aligned for use as a bytea value (since it was palloc'd) and that the StringInfo structure itself is just a local variable that doesn't need to be explicitly freed. The function essentially transforms the raw buffer data into a valid PostgreSQL bytea type that can be returned to the client.

## Parameters / Member Variables
- `buf`: A StringInfo buffer that has been populated with binary data using pq_begintypsend and subsequent append operations
## Dependencies
- Functions called/Symbols referenced:
  - SET_VARSIZE (macro to set the PostgreSQL variable-length header)
  - Assert (for debugging verification)
- Called from (representative examples):
  - [array_send](../a/array_send.md) (src/backend/utils/adt/arrayfuncs.c:1644)
  - [boolsend](../b/boolsend.md) (src/backend/utils/adt/bool.c:194)
  - [numeric_send](../n/numeric_send.md) (src/backend/utils/adt/numeric.c:1179)
  - [textsend](../t/textsend.md) (src/backend/utils/adt/varlena.c:626)
  - Many other type output functions across the codebase

## Notes and Other Information
- Must be paired with  for proper bytea construction
- The function assumes the buffer length is at least VARHDRSZ bytes (verified by Assert)
- Returns the buffer's data pointer cast to bytea*, meaning the original buffer data becomes the result
- The StringInfo structure is assumed to be stack-allocated and doesn't require explicit cleanup
- Critical for implementing binary output functions for PostgreSQL data types
- The returned bytea follows PostgreSQL's standard variable-length data format with a proper header
- Memory management is handled by the PostgreSQL memory context system since the buffer was palloc'd

## Simplified Source

```c
bytea *
pq_endtypsend(StringInfo buf)
{
    // Cast buffer data to bytea for return
    bytea *result = (bytea *) buf->data;

    // Set the correct length in the bytea header
    Assert(buf->len >= VARHDRSZ);
    SET_VARSIZE(result, buf->len);

    return result;
}
```