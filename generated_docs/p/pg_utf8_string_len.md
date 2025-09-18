# pg_utf8_string_len

## Location
src/common/saslprep.c: 1007 - 1048

## Overview
A static utility function that calculates the length in Unicode characters of a null-terminated UTF-8 encoded string, with validation to ensure the input is valid UTF-8.

## Definition
```c
static int pg_utf8_string_len(const char *source)
```

## Detailed Description
This function iterates through a UTF-8 encoded string to count the number of Unicode characters (not bytes). It performs validation at each step to ensure the UTF-8 encoding is legal and well-formed. The function handles multi-byte UTF-8 sequences correctly by using pg_utf_mblen() to determine the byte length of each character and pg_utf8_islegal() to validate the encoding.

The function serves as a prerequisite validation step in SASL string preparation, ensuring that input strings are valid UTF-8 before further processing. This is critical because SASL string preparation operations require valid Unicode input to function correctly.

Returns the character count on success, or -1 if any invalid UTF-8 sequences are encountered, making it both a length calculator and a UTF-8 validator.

## Parameters / Member Variables
- `source`: Pointer to a null-terminated UTF-8 encoded string to measure

## Dependencies
- Functions called/Symbols referenced:
  - pg_utf_mblen (determines byte length of UTF-8 character)
  - [pg_utf8_islegal](pg_utf8_islegal.md) (validates UTF-8 sequence legality)
  - pg_saslprep_rc (return code enumeration)
- Called from (representative examples):
  - [pg_saslprep](pg_saslprep.md) (main SASL preparation function)

## Notes and Other Information
- This is a static function local to src/common/saslprep.c
- Returns character count, not byte count - important distinction for Unicode
- Performs comprehensive UTF-8 validation during length calculation
- Essential for SASL string preparation input validation
- Handles all valid UTF-8 sequences including multi-byte characters
- Early validation helps prevent downstream Unicode processing errors