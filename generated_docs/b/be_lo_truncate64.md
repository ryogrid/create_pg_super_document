# be_lo_truncate64

## Location
src/backend/libpq/be-fsstubs.c: 586 - 601

## Overview
Backend function that truncates a PostgreSQL large object to a specified 64-bit length, implementing the lo_truncate64() SQL function for large objects exceeding 2GB.

## Definition
Datum be_lo_truncate64(PG_FUNCTION_ARGS)

## Detailed Description
This function serves as the backend implementation for PostgreSQL's lo_truncate64() SQL function, providing 64-bit length support for truncating large objects. It enables truncation of very large objects that exceed the 2GB limit of the 32-bit version. The function follows PostgreSQL's standard function calling conventions using the PG_FUNCTION_ARGS interface.

Like its 32-bit counterpart, this function acts as a thin wrapper around lo_truncate_internal(), but with extended length support:
1. Parameter extraction with 64-bit length support
2. Read-only transaction validation (prevents truncation in read-only transactions) 
3. Delegation to the internal truncation implementation that handles 64-bit lengths
4. Standard PostgreSQL function return handling

This is the 64-bit version of the truncate function, essential for handling large objects that can exceed 2GB in size.

## Parameters / Member Variables
- : Large object file descriptor (32-bit integer)
- : Target length for truncation (64-bit integer, in bytes)

## Dependencies
- Functions called/Symbols referenced:
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md)
  - [lo_truncate_internal](../l/lo_truncate_internal.md)
  - PG_GETARG_INT64
- Called from (representative examples):
  - SQL function lo_truncate64() (via function manager)

## Notes and Other Information
- This is a PostgreSQL internal function registered in the system catalog
- Supports 64-bit length values for large objects exceeding 2GB
- Requires the large object to be previously opened with write permissions
- Will fail in read-only transactions due to PreventCommandIfReadOnly check
- Returns 0 on success as per PostgreSQL convention
- Uses the same internal truncation logic as the 32-bit version
- Essential for handling very large objects in modern applications
- Truncation cannot extend a large object - only reduce its size
- The 64-bit length parameter allows for objects up to several exabytes in theory