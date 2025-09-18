# pg_database_encoding_max_length

## Location
src/backend/utils/mb/mbutils.c: 1546 - 1555

## Overview
Returns the maximum length in bytes that any single character can occupy in the current database's character encoding.

## Definition
```c
int pg_database_encoding_max_length(void)
```

## Detailed Description
This function provides a simple but critical piece of information: the maximum number of bytes that any character in the current database encoding can occupy. It retrieves this information from the pg_wchar_table which contains encoding-specific metadata.

The function is essential for:
- Buffer allocation when processing multi-byte characters
- String processing algorithms that need to account for variable-width encodings
- Memory management in text operations where worst-case character width must be considered
- Performance optimizations in string functions

For example:
- UTF-8 encoding returns 4 (as UTF-8 characters can be 1-4 bytes)
- Single-byte encodings like ASCII return 1
- Multi-byte Asian encodings typically return 2 or 3

This information allows PostgreSQL's text processing functions to allocate appropriate buffer sizes and handle character boundaries correctly.

## Parameters / Member Variables
None - this function takes no parameters and operates on the current database encoding

## Dependencies
- Functions called/Symbols referenced:
  - GetDatabaseEncoding (retrieves current database encoding ID)
  - pg_wchar_table (global table containing encoding metadata)
- Called from (representative examples):
  - downcase_identifier
  - lowerstr_with_len
  - str_tolower
  - str_toupper
  - str_initcap
  - GenericMatchText
  - like_escape
  - text_length
  - text_substring
  - pg_mbstrlen
  - pg_mbcharcliplen

## Notes and Other Information
- Returns an integer representing maximum bytes per character for current database encoding
- Critical for memory allocation and buffer sizing in text processing operations
- Used extensively throughout PostgreSQL's string handling functions
- Enables safe buffer allocation without per-character encoding checks
- Essential for performance in multi-byte character processing scenarios