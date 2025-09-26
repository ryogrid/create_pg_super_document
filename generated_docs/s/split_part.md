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
  - text_position_setup (initialize text search state)
  - text_position_next (find next occurrence of separator)
  - text_position_get_match_ptr (get pointer to match location)
  - text_position_cleanup (clean up search state)
  - text_position_reset (reset search to beginning)
  - cstring_to_text (convert C string to PostgreSQL text)
  - cstring_to_text_with_len (convert C string with length to PostgreSQL text)
  - PG_RETURN_TEXT_P (return text value from function)
  - TextPositionState (structure for text search state)
- Called from (representative examples):
  - SQL queries using split_part() function

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL as split_part(string, delimiter, field_number)
- Field numbering is 1-based, with field 0 being invalid and causing an error
- Negative field numbers count from the end (-1 is last field, -2 is second-to-last, etc.)
- The function is collation-aware and uses the current collation context for string comparisons
- Located in src/backend/utils/adt/varlena.c:4368-4499
- Efficiently handles the common case of retrieving the last field without requiring multiple passes