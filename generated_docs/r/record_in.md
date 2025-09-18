# record_in

## Location
src/backend/utils/adt/rowtypes.c: 74 - 328

## Overview
Converts a string representation of a composite type (record) into its internal binary format for PostgreSQL storage.

## Definition


## Detailed Description
The  function serves as the input conversion function for any composite type in PostgreSQL. It parses string representations of records in the format  and converts them into the internal  format used by PostgreSQL for storage and manipulation. The function handles complex parsing requirements including quote handling, escape sequences, null values, and nested composite types through recursive calls.

The function performs comprehensive validation of the input string format, ensures proper column count matching, and converts each field value using the appropriate type-specific input function. It maintains performance by caching I/O information for repeated calls with the same record type.

## Parameters / Member Variables
- : Input string representation of the record in format 
- : OID identifying the composite type being parsed
- : Type modifier for the composite type (-1 for standard composite types)
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - check_stack_depth: Stack overflow protection for recursive calls
  - lookup_rowtype_tupdesc: Retrieves tuple descriptor for the record type
  - MemoryContextAlloc: Memory allocation in function context
  - getTypeInputInfo: Gets input function info for column types
  - InputFunctionCallSafe: Safely calls type-specific input functions
  - heap_form_tuple: Creates heap tuple from values array
  - ReleaseTupleDesc: Releases tuple descriptor reference

- Called from (representative examples):
  - Type system as registered input function for composite types
  - SQL parsing when processing record literals

## Notes and Other Information
- Supports both quoted and unquoted field values with proper escape sequence handling
- Handles anonymous record types (RECORD) only when valid typmod is provided
- Uses function-local caching (fn_extra) to optimize repeated calls with same type
- Implements comprehensive error reporting for malformed input strings
- Supports soft error handling through error context parameter
- Memory management ensures result can be safely freed by caller