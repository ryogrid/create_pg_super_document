# split_part

## Location
[src/backend/utils/adt/varlena.c:4368-4499](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4368-L4499)

## Overview
A PostgreSQL built-in function that splits an input string by a field separator and returns the N-th field, supporting both positive (1-based) and negative (count from end) field indexing.

## Definition

```c
struct_empty_array(TEXTOID));
```
## Detailed Description
This function implements the SQL split_part() function which parses an input string based on a provided field separator and returns the specified field. Key features include:

- **Flexible indexing**: Supports positive field numbers (1-based from start) and negative field numbers (counting from end)
- **Edge case handling**: Gracefully handles empty strings, missing separators, and non-existent field numbers
- **Efficient processing**: Uses TextPositionState for optimized string searching with proper collation support
- **Memory safety**: Proper cleanup of internal state structures

The function handles several special cases:
- Empty input string returns empty string
- Empty field separator: returns input string for field 1 or -1, empty string otherwise  
- Field separator not found: returns input string for field 1 or -1, empty string otherwise
- Non-existent field numbers: returns empty string

## Parameters / Member Variables
- **Argument 0**:  (text) - The input string to be split
- **Argument 1**:  (text) - The field separator string
- **Argument 2**:  (int32) - Field number to return (1-based, negative counts from end)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (get text argument with potential detoasting)
  - PG_GETARG_INT32 (get integer argument)
  - PG_GET_COLLATION (get collation for string operations)
  - [text_position_setup](../t/text_position_setup.md) (initialize text search state)
  - [text_position_next](../t/text_position_next.md) (find next occurrence of separator)
  - [text_position_get_match_ptr](../t/text_position_get_match_ptr.md) (get pointer to match location)
  - [text_position_cleanup](../t/text_position_cleanup.md) (clean up search state)
  - [text_position_reset](../t/text_position_reset.md) (reset search to beginning)
  - [cstring_to_text](../c/cstring_to_text.md) (convert C string to PostgreSQL text)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) (convert C string with length to PostgreSQL text)
  - PG_RETURN_TEXT_P (return text value from function)
  - [TextPositionState](../T/TextPositionState.md) (structure for text search state)
- Called from (representative examples):
  - SQL queries using split_part() function

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL as split_part(string, delimiter, field_number)
- Field numbering is 1-based, with field 0 being invalid and causing an error
- Negative field numbers count from the end (-1 is last field, -2 is second-to-last, etc.)
- The function is collation-aware and uses the current collation context for string comparisons
- Located in src/backend/utils/adt/varlena.c:4368-4499
- Efficiently handles the common case of retrieving the last field without requiring multiple passes

## Simplified Source

```c
Datum split_part(PG_FUNCTION_ARGS) {
    text *inputstring = PG_GETARG_TEXT_PP(0);
    text *fldsep = PG_GETARG_TEXT_PP(1);
    int fldnum = PG_GETARG_INT32(2);

    // Field number must not be zero
    if (fldnum == 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("field position must not be zero")));

    int inputstring_len = VARSIZE_ANY_EXHDR(inputstring);
    int fldsep_len = VARSIZE_ANY_EXHDR(fldsep);

    // Handle empty input string
    if (inputstring_len < 1)
        PG_RETURN_TEXT_P(cstring_to_text(""));

    // Handle empty field separator
    if (fldsep_len < 1) {
        if (fldnum == 1 || fldnum == -1)
            PG_RETURN_TEXT_P(inputstring);
        else
            PG_RETURN_TEXT_P(cstring_to_text(""));
    }

    // Setup text position search for separator
    TextPositionState state;
    text_position_setup(inputstring, fldsep, PG_GET_COLLATION(), &state);
    bool found = text_position_next(&state);

    // Handle case where separator not found
    if (!found) {
        text_position_cleanup(&state);
        if (fldnum == 1 || fldnum == -1)
            PG_RETURN_TEXT_P(inputstring);
        else
            PG_RETURN_TEXT_P(cstring_to_text(""));
    }

    // Convert negative field numbers to positive by counting fields
    if (fldnum < 0) {
        int numfields = 2;  // Found separator, so at least 2 fields
        while (text_position_next(&state))
            numfields++;

        // Optimize last field case
        if (fldnum == -1) {
            char *start_ptr = text_position_get_match_ptr(&state) + fldsep_len;
            char *end_ptr = VARDATA_ANY(inputstring) + inputstring_len;
            text_position_cleanup(&state);
            PG_RETURN_TEXT_P(cstring_to_text_with_len(start_ptr, end_ptr - start_ptr));
        }

        fldnum += numfields + 1;
        if (fldnum <= 0) {
            text_position_cleanup(&state);
            PG_RETURN_TEXT_P(cstring_to_text(""));
        }

        // Reset search to start with positive field number
        text_position_reset(&state);
        found = text_position_next(&state);
    }

    // Find the requested field by iterating through separators
    char *start_ptr = VARDATA_ANY(inputstring);
    char *end_ptr = text_position_get_match_ptr(&state);

    while (found && --fldnum > 0) {
        start_ptr = end_ptr + fldsep_len;
        found = text_position_next(&state);
        if (found)
            end_ptr = text_position_get_match_ptr(&state);
    }

    text_position_cleanup(&state);

    // Return the appropriate field or empty string
    text *result_text;
    if (fldnum > 0) {
        // Requested field beyond available fields
        if (fldnum == 1) {
            // Return last available field
            int last_len = start_ptr - VARDATA_ANY(inputstring);
            result_text = cstring_to_text_with_len(start_ptr, inputstring_len - last_len);
        } else {
            result_text = cstring_to_text("");
        }
    } else {
        // Return the found field
        result_text = cstring_to_text_with_len(start_ptr, end_ptr - start_ptr);
    }

    PG_RETURN_TEXT_P(result_text);
}
```