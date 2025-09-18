# downcase_truncate_identifier

## Location
src/backend/parser/scansup.c: 37 - 45

## Overview
A utility function that performs both downcasing and truncation of unquoted SQL identifiers, ensuring they conform to PostgreSQL's identifier naming rules and length limits.

## Definition


## Detailed Description
This function is a convenience wrapper around  that performs both case conversion and truncation of SQL identifiers in a single call. It converts uppercase letters to lowercase following SQL standards and truncates identifiers that exceed PostgreSQL's maximum identifier length (NAMEDATALEN). The function is specifically designed for processing unquoted identifiers, as quoted identifiers preserve their original case and are handled differently.

The function allocates memory for the result using  and returns a newly allocated string containing the processed identifier. The downcasing follows a hybrid approach: ASCII characters are converted using simple arithmetic, while high-bit characters use locale-aware  for single-byte encodings.

## Parameters / Member Variables
- : Pointer to the input identifier string (may not be null-terminated)
- : Length of the input identifier string in characters
- : Boolean flag indicating whether to emit a warning if truncation occurs

## Dependencies
- Functions called/Symbols referenced:
  - downcase_identifier (the main workhorse function)
- Called from (representative examples):
  - extract_date (in date/time processing)
  - timestamp_trunc (timestamp truncation operations)
  - SplitIdentifierString (identifier parsing utilities)
  - Various date/time functions for field name processing

## Notes and Other Information
- Returns a 'd string that must be freed by the caller
- The API is designed to potentially support downcasing transformations that increase string length, though this is not currently implemented
- Part of PostgreSQL's identifier processing infrastructure in the parser subsystem
- Used extensively in date/time functions for processing field names like 'YEAR', 'MONTH', etc.
- The function handles the case where the input string may not be null-terminated by using the explicit length parameter