# NUM_eat_non_data_chars

## Location
[src/backend/utils/adt/formatting.c:5810-5822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L5810-L5822)

## Overview
A static utility function that skips over non-numeric data characters in input string processing for PostgreSQL's number formatting operations.

## Definition


## Detailed Description
This function is designed to advance the input pointer past a specified number of characters, but only if those characters are not numeric data. It's used during number parsing to skip formatting characters that should be ignored when extracting numeric values. The function respects multibyte character boundaries and includes safety checks to prevent buffer overruns.

The function identifies numeric data characters as digits (0-9), decimal points (.), commas (,), and sign characters (+/-). Any other characters are considered non-data and can be skipped.

## Parameters / Member Variables
- : Pointer to NUMProc structure containing input/output state and pointers
- : Maximum number of characters to potentially skip
- : Length of the input string (used for boundary checking via OVERLOAD_TEST)

## Dependencies
- Functions called/Symbols referenced:
  - [NUMProc](NUMProc.md) (structure type)
  - OVERLOAD_TEST (macro for boundary checking)
  - [pg_mblen](../p/pg_mblen.md) (function for multibyte character length)
- Called from (representative examples):
  - [NUM_processor](NUM_processor.md) (multiple call sites at lines 6123, 6167, 6185, 6206, 6228, 6245)

## Notes and Other Information
- This is a static function, only available within the formatting.c compilation unit
- The function properly handles multibyte characters using pg_mblen()
- Uses OVERLOAD_TEST macro to prevent reading beyond input buffer boundaries
- The character set "0123456789.,+-" defines what constitutes numeric data
- Stops early if it encounters numeric data, ensuring actual number content is preserved
- Part of PostgreSQL's comprehensive number formatting system in src/backend/utils/adt/formatting.c