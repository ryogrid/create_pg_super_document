# logicalrep_read_origin

## Location
src/backend/replication/logical/proto.c: 401 - 413

## Overview
This function reads an ORIGIN message from the logical replication input stream, parsing replication origin information and returning the origin name string.

## Definition


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
  - pq_getmsgint64
  - pq_getmsgstring
  - pstrdup (implicit through return)
- Called from (representative examples):
  - Currently no direct callers found in the codebase

## Notes and Other Information
- This function allocates memory for the returned string using pstrdup
- The caller is responsible for freeing the returned string
- Part of PostgreSQL's replication origin tracking system
- Counterpart to logicalrep_write_origin for message deserialization
- Located in src/backend/replication/logical/proto.c:401-413