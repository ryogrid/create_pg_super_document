# oidvectorrecv

## Location
src/backend/utils/adt/oid.c: 184 - 225

## Overview
Binary receive function that converts external binary format data into PostgreSQL's internal oidvector format.

## Definition


## Detailed Description
The  function is a type receive function that handles the conversion from PostgreSQL's external binary protocol format to the internal oidvector data structure. This function is part of the binary I/O infrastructure used for network communication and binary data exchange.

The function works by delegating to the general  function with specific parameters for oidvector (OIDOID element type, -1 typmod). However, it cannot use DirectFunctionCall3 because array_recv needs to cache data in the function info structure. After receiving the array, it performs comprehensive sanity checks to ensure the result is a valid oidvector (1-dimensional, 0-based, no nulls, correct element type).

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides:
  - : StringInfo buffer containing the binary data to parse

## Dependencies
- Functions called/Symbols referenced:
  - LOCAL_FCINFO (macro for local function call info)
  - oidvector (data type)
  - InitFunctionCallInfoData (function call setup)
  - array_recv (general array receive function)
  - ARR_NDIM, ARR_HASNULL, ARR_ELEMTYPE, ARR_LBOUND (array metadata macros)
  - ereport (error reporting)
- Called from (representative examples):
  - PostgreSQL binary protocol handling
  - Network communication during data transfer
  - Binary format data import operations

## Notes and Other Information
- Cannot use DirectFunctionCall3 due to array_recv's need for function info caching
- Manually sets up function call info with proper parameters for oidvector
- Performs strict validation: must be 1-dimensional, 0-based, no nulls, OIDOID elements
- Raises ERROR with ERRCODE_INVALID_BINARY_REPRESENTATION for invalid data
- Part of PostgreSQL's binary I/O protocol for efficient data transfer
- Uses Assert to verify the function call succeeded (result is not null)