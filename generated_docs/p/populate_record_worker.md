# populate_record_worker

## Location
src/backend/utils/adt/jsonfuncs.c: 3697 - 3808

## Overview
A common worker function that implements the core logic for JSON/JSONB to record conversion functions, handling both populate and to_record variants with comprehensive type resolution and caching.

## Definition


## Detailed Description
This function serves as the unified implementation for multiple JSON record conversion functions including , , , and . It handles type resolution, caching, input validation, and delegates the actual conversion work to . The function manages different input scenarios, including cases where record types must be inferred from query context.

Key behaviors:
- Initializes and manages per-function-call caching for performance
- Handles type resolution from arguments or query context
- Supports both JSON text and JSONB binary formats
- Manages null inputs and returns appropriate results
- Delegates actual conversion to populate_composite
- Handles RECORD type resolution at runtime

## Parameters / Member Variables
- : Function call information containing arguments and execution context
- : Name of the calling function (for error reporting)
- : Boolean indicating whether input is JSON text (true) or JSONB (false)
- : Boolean indicating whether function has a record argument (populate functions vs to_record functions)
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextAllocZero
  - get_record_type_from_argument
  - get_record_type_from_query
  - PG_GETARG_HEAPTUPLEHEADER
  - HeapTupleHeaderGetTypeId
  - HeapTupleHeaderGetTypMod
  - PG_GETARG_TEXT_PP
  - PG_GETARG_JSONB_P
  - populate_composite
  - SOFT_ERROR_OCCURRED
  - PG_RETURN_DATUM
- Called from (representative examples):
  - jsonb_populate_record
  - jsonb_populate_record_valid
  - jsonb_to_record
  - json_populate_record
  - json_to_record

## Notes and Other Information
- This is a static function serving as the common implementation for multiple public functions
- Implements sophisticated caching to avoid repeated type lookups within a query
- Handles the distinction between JSON text and JSONB binary formats
- Supports soft error handling through the escontext parameter
- Manages runtime type resolution for RECORD types
- Returns unchanged record for null JSON inputs
- Part of PostgreSQL's comprehensive JSON/JSONB to record conversion infrastructure
- The function signature uses boolean flags to distinguish between different calling patterns