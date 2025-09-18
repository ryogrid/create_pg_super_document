# pq_copymsgbytes

## Location
src/backend/libpq/pqformat.c: 528 - 545

## Overview
Copies raw data from a message buffer into a caller-provided buffer, ensuring data isolation and safety.

## Definition


## Detailed Description
The  function extracts raw binary data from a PostgreSQL message buffer () and copies it into a caller-provided buffer using . Unlike  which returns a pointer directly into the message buffer, this function creates a separate copy of the data, providing data isolation and ensuring the caller owns the copied data. The function validates data availability and advances the message cursor appropriately.

## Parameters / Member Variables
- : A  structure representing the message buffer being read from
- : Caller-provided buffer where the data will be copied to (must be pre-allocated)
- : The number of bytes to copy from the message buffer (must be non-negative)

## Dependencies
- Functions called/Symbols referenced:
  -  (for error reporting)
  -  (error level constant)
  -  (error code function)
  -  (error code constant)
  -  (error message function)
  -  (memory copy function)
- Called from (representative examples):
  -  (COPY command data retrieval)
  -  (integer message extraction)
  -  (64-bit integer message extraction)
  -  (logical replication tuple reading)
  -  (bit type receive function)
  -  (variable bit type receive function)
  -  (bytea type receive function)

## Notes and Other Information
- Provides data isolation by copying data rather than returning direct pointers
- Caller is responsible for allocating sufficient buffer space before calling this function
- Validates data availability before performing the copy operation to prevent buffer overruns
- Advances the message cursor automatically to maintain proper position tracking
- Throws a protocol violation error if insufficient data is available
- More memory-safe than direct pointer access for cases where data needs to persist beyond the message buffer's lifetime
- Commonly used for extracting fixed-size data types from protocol messages