# be_lo_truncate

## Location
src/backend/libpq/be-fsstubs.c: 574 - 585

## Overview
Backend function that truncates a PostgreSQL large object to a specified 32-bit length, implementing the lo_truncate() SQL function.

## Definition
Datum be_lo_truncate(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the backend implementation for PostgreSQL's lo_truncate() SQL function with 32-bit length support. It provides a way to reduce the size of an existing large object to a specified length, discarding any data beyond the truncation point. The function follows PostgreSQL's standard function calling conventions using the PG_FUNCTION_ARGS interface.

The function acts as a thin wrapper around lo_truncate_internal(), providing:
1. Parameter extraction from the PostgreSQL function call interface
2. Read-only transaction validation (prevents truncation in read-only transactions)
3. Delegation to the internal truncation implementation
4. Standard PostgreSQL function return handling

This is the 32-bit version of the truncate function, suitable for large objects up to 2GB in size.

## Parameters / Member Variables
- : Large object file descriptor (32-bit integer)
- : Target length for truncation (32-bit integer, in bytes)

## Dependencies
- Functions called/Symbols referenced:
  - PreventCommandIfReadOnly
  - lo_truncate_internal
- Called from (representative examples):
  - SQL function lo_truncate() (via function manager)

## Notes and Other Information
- This is a PostgreSQL internal function registered in the system catalog
- Limited to 32-bit length values (maximum ~2GB)
- Requires the large object to be previously opened with write permissions
- Will fail in read-only transactions due to PreventCommandIfReadOnly check
- Returns 0 on success as per PostgreSQL convention
- For large objects exceeding 2GB, use be_lo_truncate64 instead
- The actual truncation logic is handled by lo_truncate_internal
- Truncation cannot extend a large object - only reduce its size