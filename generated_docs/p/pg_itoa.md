# pg_itoa

## Location
src/backend/utils/adt/numutils.c: 1044 - 1056

## Overview
Converts a signed 16-bit integer to its string representation and returns the length of the resulting string.

## Definition


## Detailed Description
The  function is a PostgreSQL utility function that converts a signed 16-bit integer () to its decimal string representation. The function is implemented as a simple wrapper around , casting the 16-bit integer to a 32-bit integer before conversion. This approach leverages the existing 32-bit conversion logic rather than implementing separate conversion code for 16-bit integers.

The function writes the string representation directly to the provided buffer and returns the length of the string (equivalent to ), which can be useful for applications that need to know the string length without making an additional  call.

## Parameters / Member Variables
- : The signed 16-bit integer to convert to string representation
- : Pointer to the output buffer where the string representation will be written. Must point to sufficient memory to hold the result (at least 7 bytes to accommodate the worst case: sign, 5 digits for -32768, and null terminator)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ltoa](pg_ltoa.md) (casts input to int32 and delegates conversion)
- Called from (representative examples):
  - [int2out](../i/int2out.md) (converts int16 to string for output)
  - [int2vectorout](../i/int2vectorout.md) (converts int16 vector to string representation)
  - LogicalTapeSetCreate (for tape numbering in sort operations)
  - LogicalTapeImport (for tape identification in sort operations)

## Notes and Other Information
- The caller must ensure adequate buffer space (minimum 7 bytes) to prevent buffer overflow
- The function does not perform bounds checking on the output buffer
- Implementation leverages existing 32-bit conversion logic for code reuse and maintainability
- Used primarily for PostgreSQL's int2 (smallint) data type output functions
- Also utilized in internal sort/tape management operations for numeric identifiers