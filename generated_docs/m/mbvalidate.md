# mbvalidate

## Location
[src/fe_utils/mbprint.c:392-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/mbprint.c#L392-L405)

## Overview
Validates and sanitizes multibyte character strings by removing invalid byte sequences according to the specified encoding, ensuring safe text processing in PostgreSQL frontend utilities.

## Definition

```c
unsigned char *
mbvalidate(unsigned char *pwcs, int encoding)
```
## Detailed Description
mbvalidate provides encoding validation for multibyte character strings, removing any characters that are not valid according to the specified encoding rules. Currently, the function specifically handles UTF-8 encoding validation through the mb_utf_validate function. For other encodings, the function serves as a framework where additional validation routines can be added as needed.

The function operates in-place, modifying the input string to remove invalid sequences. This ensures that subsequent text processing operations will not encounter malformed multibyte sequences that could cause errors or incorrect display behavior.

## Parameters / Member Variables
- `*pwcs`: Input/output multibyte character string to validate and modify in-place
- `encoding`: Character encoding identifier that determines which validation routine to apply
## Dependencies
- Functions called/Symbols referenced:
  - PG_UTF8: Encoding constant for UTF-8 identification
  - [mb_utf_validate](mb_utf_validate.md): UTF-8 specific validation function that removes invalid sequences
- Called from (representative examples):
  - [printTableAddHeader](../p/printTableAddHeader.md): For validating table header text before formatting
  - [printTableAddCell](../p/printTableAddCell.md): For validating table cell content before display
  - [lineptr](../l/lineptr.md): Through header inclusion for line text validation

## Notes and Other Information
- The function currently only implements UTF-8 validation, with a framework for adding other encodings
- The comment suggests this functionality may be redundant with existing validation elsewhere in PostgreSQL
- Validation is performed in-place, modifying the original string
- The function always returns the input pointer, making it suitable for use in expression contexts
- Additional encoding validation routines should be added to the else branch as needed
- This is part of PostgreSQL's frontend utilities for ensuring safe text display across different character encodings

## Simplified Source

```c
unsigned char *
mbvalidate(unsigned char *pwcs, int encoding)
{
    // Currently only UTF-8 validation is implemented
    if (encoding == PG_UTF8) {
        mb_utf_validate(pwcs);
    }
    // Other encodings can be added here as needed

    return pwcs;  // Always return the (possibly modified) input string
}
```