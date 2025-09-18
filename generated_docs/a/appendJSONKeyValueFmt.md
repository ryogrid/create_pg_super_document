# appendJSONKeyValueFmt

## Location
[src/backend/utils/error/jsonlog.c:31-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/error/jsonlog.c#L31-L70)

## Overview
A static helper function that formats a value using printf-style formatting and appends it as a JSON key-value pair to a StringInfo buffer.

## Definition


## Detailed Description
This function serves as a wrapper around  that adds printf-style formatting capability. It takes a format string and variable arguments, formats them into a string using , and then calls  to append the result as a JSON key-value pair to the buffer. The function implements dynamic buffer allocation, starting with an initial size assumption of 128 bytes and expanding as needed if the formatted string doesn't fit.

The function preserves the original errno value throughout the formatting process to avoid side effects. It uses a loop-and-retry approach for buffer allocation: if the initial buffer is too small, it frees the buffer, uses the required size returned by , and tries again.

## Parameters / Member Variables
- : StringInfo buffer to append the JSON key-value pair to
- : The JSON property key (will be escaped when appended)
- : Boolean flag controlling whether the formatted value should be escaped as JSON
- : printf-style format string for the value
- : Variable arguments corresponding to the format string placeholders

## Dependencies
- Functions called/Symbols referenced:
  -  (memory allocation)
  -  (formatted string printing)
  -  (memory deallocation) 
  -  (core JSON key-value appending)
  -  (compiler attribute for format checking)
- Called from (representative examples):
  -  (multiple times at lines 160, 171, 175, 199, 203, 236, 248, 260, 282, 287)

## Notes and Other Information
- This is a static function, only accessible within the jsonlog.c file
- The function handles variable-length formatted strings efficiently by starting with a reasonable buffer size and expanding only when necessary
- The  attribute enables compile-time format string checking
- Memory management is handled carefully with proper cleanup of the temporary value buffer
- The errno preservation ensures that formatting operations don't interfere with error reporting contexts