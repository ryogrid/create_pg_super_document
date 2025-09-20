# logicalrep_read_origin

## Location
[src/backend/replication/logical/proto.c:401-413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/proto.c#L401-L413)

## Overview
This function reads an ORIGIN message from the logical replication input stream, parsing replication origin information and returning the origin name string.

## Definition

```c
char *
logicalrep_read_origin(StringInfo in, XLogRecPtr *origin_lsn)
```
## Detailed Description
The  function is the counterpart to , responsible for deserializing ORIGIN messages from the logical replication stream. It extracts the origin LSN position and origin name from the input buffer, providing this information to the caller for replication origin tracking. The function reads the 64-bit LSN value first, then retrieves the null-terminated origin name string and returns a newly allocated copy of it.

This function is crucial for maintaining replication origin information in logical replication subscribers, enabling proper tracking of change provenance and preventing replication loops in complex replication topologies.

## Parameters / Member Variables
- : StringInfo buffer containing the serialized ORIGIN message to be parsed
- : Pointer to XLogRecPtr where the parsed origin LSN will be stored

## Return Value
- Returns a newly allocated string (via pstrdup) containing the origin name, which must be freed by the caller

## Dependencies
- Functions called/Symbols referenced:
  - [pq_getmsgint64](../p/pq_getmsgint64.md)
  - [pq_getmsgstring](../p/pq_getmsgstring.md)
  - [pstrdup](../p/pstrdup.md) (implicit through return)
- Called from (representative examples):
  - Currently no direct callers found in the codebase

## Notes and Other Information
- This function allocates memory for the returned string using pstrdup
- The caller is responsible for freeing the returned string
- Part of PostgreSQL's replication origin tracking system
- Counterpart to logicalrep_write_origin for message deserialization
- Located in src/backend/replication/logical/proto.c:401-413