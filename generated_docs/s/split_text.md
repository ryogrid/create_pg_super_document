# split_text

## Location
src/backend/utils/adt/varlena.c: 4591 - 4726

## Overview
Core text splitting engine that provides common functionality for text_to_array, text_to_table and their null-handling variants.

## Definition
```c
static bool split_text(FunctionCallInfo fcinfo, SplitTextOutputData *tstate)
```

## Detailed Description
The split_text function is the central text processing engine that handles string splitting operations for PostgreSQL's text-to-array and text-to-table functions. It supports two main splitting modes: delimiter-based splitting (when fldsep is provided) and character-by-character splitting (when fldsep is NULL). The function handles null inputs, empty strings, and provides null string replacement functionality. It uses PostgreSQL's text positioning machinery for efficient delimiter searching and supports multi-byte character handling for proper Unicode support.

## Parameters / Member Variables
- `fcinfo`: Function call information containing input arguments
- `tstate`: Output state structure that determines whether results go to arrays or tuple stores
- Input arguments handled:
  - Argument 0: Input text string to split
  - Argument 1: Field separator (delimiter) - can be NULL
  - Argument 2: Null replacement string (optional) - can be NULL

## Dependencies
- Functions called/Symbols referenced:
  - SplitTextOutputData (output state structure)
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (PostgreSQL function call metadata)
  - PG_GET_COLLATION (collation information retrieval)
  - PG_NARGS (argument count checking)
  - TextPositionState (text search state management)
  - [split_text_accum_result](split_text_accum_result.md) (result accumulation)
  - text_position_setup/next/cleanup/get_match_ptr (text positioning engine)
  - VARSIZE_ANY (variable-length type size calculation)
  - cstring_to_text_with_len (text datum creation)
  - [pg_mblen](../p/pg_mblen.md) (multi-byte character length calculation)
- Called from (representative examples):
  - [text_to_array](../t/text_to_array.md)
  - [text_to_table](../t/text_to_table.md)
  - DatumGetVarStringPP

## Notes and Other Information
- Located in src/backend/utils/adt/varlena.c:4591-4726
- Static function - internal implementation detail not exposed outside this file
- Handles two distinct splitting modes: delimiter-based and character-by-character
- Uses CHECK_FOR_INTERRUPTS() to allow query cancellation during long operations
- Supports null string replacement functionality for converting specific values to SQL NULL
- Returns false if the overall result should be NULL, true otherwise
- Caller must handle empty result sets (when no elements are produced)
- Part of PostgreSQL's variable-length data type utilities core engine