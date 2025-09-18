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
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (retrieves current database encoding ID)
  - pg_wchar_table (global table containing encoding metadata)
- Called from (representative examples):
  - [downcase_identifier](../d/downcase_identifier.md)
  - [lowerstr_with_len](../l/lowerstr_with_len.md)
  - [str_tolower](../s/str_tolower.md)
  - [str_toupper](../s/str_toupper.md)
  - [str_initcap](../s/str_initcap.md)
  - [GenericMatchText](../G/GenericMatchText.md)
  - [like_escape](../l/like_escape.md)
  - [text_length](../t/text_length.md)
  - [text_substring](../t/text_substring.md)
  - [pg_mbstrlen](pg_mbstrlen.md)
  - [pg_mbcharcliplen](pg_mbcharcliplen.md)

## Notes and Other Information
- Returns an integer representing maximum bytes per character for current database encoding
- Critical for memory allocation and buffer sizing in text processing operations
- Used extensively throughout PostgreSQL's string handling functions
- Enables safe buffer allocation without per-character encoding checks
- Essential for performance in multi-byte character processing scenarios