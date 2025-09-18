# CopyFromErrorCallback

## Location
src/backend/commands/copyfrom.c: 112 - 190

## Overview
CopyFromErrorCallback is an error context callback function for COPY FROM operations that provides detailed error context information including line numbers, column names, and data values when errors occur during data copying.

## Definition


## Detailed Description
This function serves as the error context callback for COPY FROM operations in PostgreSQL. It takes a CopyFromState argument and generates contextual error messages based on the current state of the copy operation. The function handles different scenarios:

1. **Relation-only context**: When only relation name information is needed
2. **Binary format errors**: For binary COPY operations where data cannot be meaningfully displayed
3. **Text format errors**: For text COPY operations with detailed column and value information

The function intelligently formats error messages to include:
- Relation name
- Current line number
- Column name (if relevant)
- Actual data value (if available and not binary)
- Full line content (when line buffer is valid)

For text format operations, it uses CopyLimitPrintoutLength to ensure error messages don't become excessively long by truncating displayed values when necessary.

## Parameters / Member Variables
- : A void pointer that must be cast to CopyFromState, containing the current state of the COPY FROM operation including relation name, line number, column information, and data buffers

## Dependencies
- Functions called/Symbols referenced:
  - CopyFromState (struct type)
  - errcontext (error reporting function)
  - CopyLimitPrintoutLength (utility function for limiting output length)
  - pfree (memory deallocation function)
- Called from (representative examples):
  - CopyFrom (main COPY FROM function at src/backend/commands/copyfrom.c:950)

## Notes and Other Information
- The function is designed to be used with PostgreSQL's error context callback mechanism
- It provides progressively more detailed error information based on what context is available
- Binary format operations have limited error detail display since binary data cannot be meaningfully shown to users
- Memory management is handled properly with pfree() calls for allocated strings
- The function handles NULL values gracefully and provides appropriate messaging
- Line buffer validity is checked before attempting to display line content to avoid displaying stale data