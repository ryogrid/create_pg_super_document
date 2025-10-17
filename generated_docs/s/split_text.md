# split_text

## Location
[src/backend/utils/adt/varlena.c:4591-4726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4591-L4726)

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
  - [TextPositionState](../T/TextPositionState.md) (text search state management)
  - [split_text_accum_result](split_text_accum_result.md) (result accumulation)
  - [text_position_setup](../t/text_position_setup.md)/next/cleanup/get_match_ptr (text positioning engine)
  - VARSIZE_ANY (variable-length type size calculation)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) (text datum creation)
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

## Simplified Source

```c
static bool split_text(FunctionCallInfo fcinfo, SplitTextOutputData *tstate) {
    text *inputstring;
    text *fldsep;
    text *null_string;
    Oid collation = PG_GET_COLLATION();

    // Return false (NULL result) if input string is NULL
    if (PG_ARGISNULL(0))
        return false;

    inputstring = PG_GETARG_TEXT_PP(0);

    // Handle optional field separator
    fldsep = PG_ARGISNULL(1) ? NULL : PG_GETARG_TEXT_PP(1);

    // Handle optional null replacement string
    null_string = (PG_NARGS() > 2 && !PG_ARGISNULL(2)) ?
                  PG_GETARG_TEXT_PP(2) : NULL;

    if (fldsep != NULL) {
        // Normal delimiter-based splitting
        int inputstring_len = VARSIZE_ANY_EXHDR(inputstring);
        int fldsep_len = VARSIZE_ANY_EXHDR(fldsep);

        // Handle empty input
        if (inputstring_len < 1)
            return true;

        // Handle empty separator - return input as single element
        if (fldsep_len < 1) {
            split_text_accum_result(tstate, inputstring, null_string, collation);
            return true;
        }

        // Setup text search for delimiter
        TextPositionState state;
        text_position_setup(inputstring, fldsep, collation, &state);

        char *start_ptr = VARDATA_ANY(inputstring);

        // Process each field separated by delimiter
        for (;;) {
            CHECK_FOR_INTERRUPTS();

            bool found = text_position_next(&state);
            char *end_ptr;
            int chunk_len;

            if (!found) {
                // Last field - from current position to end
                chunk_len = ((char *) inputstring + VARSIZE_ANY(inputstring)) - start_ptr;
            } else {
                // Regular field - from current position to delimiter
                end_ptr = text_position_get_match_ptr(&state);
                chunk_len = end_ptr - start_ptr;
            }

            // Create text datum for this field and accumulate it
            text *result_text = cstring_to_text_with_len(start_ptr, chunk_len);
            split_text_accum_result(tstate, result_text, null_string, collation);
            pfree(result_text);

            if (!found)
                break;

            // Move past the delimiter for next iteration
            start_ptr = end_ptr + fldsep_len;
        }

        text_position_cleanup(&state);
    } else {
        // Character-by-character splitting (when fldsep is NULL)
        int inputstring_len = VARSIZE_ANY_EXHDR(inputstring);
        char *start_ptr = VARDATA_ANY(inputstring);

        while (inputstring_len > 0) {
            CHECK_FOR_INTERRUPTS();

            // Get length of next character (handles multi-byte)
            int chunk_len = pg_mblen(start_ptr);

            // Create text datum for this character and accumulate it
            text *result_text = cstring_to_text_with_len(start_ptr, chunk_len);
            split_text_accum_result(tstate, result_text, null_string, collation);
            pfree(result_text);

            // Move to next character
            start_ptr += chunk_len;
            inputstring_len -= chunk_len;
        }
    }

    return true;
}
```