# be_lo_get_fragment

## Location
src/backend/libpq/be-fsstubs.c: 806 - 826

## Overview
A PostgreSQL backend function that reads a specific fragment (range) of data from a large object, returning only the requested portion as bytea data.

## Definition


## Detailed Description
The  function provides selective reading capability for PostgreSQL large objects by allowing retrieval of a specific range of bytes. Unlike  which reads the entire object, this function accepts an offset and length parameter to read only a portion of the large object. It includes parameter validation to ensure the requested length is not negative, and delegates the actual reading operation to the internal  function. This functionality is essential for efficient handling of large objects where only specific portions need to be accessed.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that contains:
  -  (Oid): The object identifier of the large object to read from
  -  (int64): The starting position in bytes from which to begin reading
  -  (int32): The number of bytes to read from the large object

## Dependencies
- Functions called/Symbols referenced:
  - : Internal function that performs the actual fragment reading
  - : Macro to extract OID argument from function call
  - : Macro to extract 64-bit integer argument (offset)
  - : Macro to extract 32-bit integer argument (length)
  - : Macro to return bytea result
  - : Error reporting function for parameter validation
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function interface)

## Notes and Other Information
- Validates that the requested length () is not negative before processing
- Supports reading from any offset within the large object
- Returns data as bytea type for binary content handling
- More efficient than reading entire large objects when only a portion is needed
- Part of PostgreSQL's large object filesystem stub interface
- Located in src/backend/libpq/be-fsstubs.c:806-826