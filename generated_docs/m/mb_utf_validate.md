# mb_utf_validate

## Location
src/fe_utils/mbprint.c: 136 - 176

## Overview
A static function that validates and sanitizes a UTF-8 string by removing invalid characters while preserving valid ones in-place.

## Definition
```c
static void mb_utf_validate(unsigned char *pwcs)
```

## Detailed Description
This function performs in-place validation and sanitization of a UTF-8 string. It scans through the input string character by character, using `utf_charcheck` to validate each UTF-8 sequence. Valid characters are preserved and copied to the output position, while invalid characters are silently skipped. This creates a sanitized version of the original string containing only valid UTF-8 sequences.

The function operates efficiently by:
1. Using two pointers - one for reading (`pwcs`) and one for writing (`p`)
2. Only copying bytes when invalid characters have been encountered (when `p != pwcs`)
3. Advancing both pointers together when no invalid characters have been found yet
4. Null-terminating the result string if any modifications were made

## Parameters / Member Variables
- `pwcs`: Pointer to a null-terminated UTF-8 string to validate and sanitize in-place

## Dependencies
- Functions called/Symbols referenced:
  - utf_charcheck
- Called from (representative examples):
  - mbvalidate

## Notes and Other Information
- Performs in-place modification of the input string
- The output string will always be equal to or shorter than the input string
- Invalid UTF-8 sequences are completely removed rather than replaced with placeholder characters
- Used as part of PostgreSQL's multibyte character validation pipeline
- Efficient implementation that minimizes memory copying when the string is already valid
- The function assumes the input buffer is writable and properly null-terminated
- Essential for ensuring data integrity when processing potentially malformed UTF-8 input