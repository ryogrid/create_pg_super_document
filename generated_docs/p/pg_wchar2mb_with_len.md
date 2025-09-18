# pg_wchar2mb_with_len

## Location
src/backend/utils/mb/mbutils.c: 1008 - 1014

## Overview
Converts a wide character string to a multibyte character string with a specified length limit, using the current database encoding.

## Definition
```c
int pg_wchar2mb_with_len(const pg_wchar *from, char *to, int len)
```

## Detailed Description
This function converts a wide character string to a multibyte character string with a length restriction, providing safer bounded conversion operations. It acts as a wrapper that delegates the actual conversion to the appropriate encoding-specific wchar2mb_with_len function stored in the pg_wchar_table array based on the current DatabaseEncoding. The length parameter helps prevent buffer overflows by limiting the number of wide characters processed during conversion.

## Parameters / Member Variables
- `from`: Pointer to the source wide character string to be converted
- `to`: Pointer to the destination buffer where the multibyte character string will be stored
- `len`: Maximum number of wide characters to process during conversion

## Dependencies
- Functions called/Symbols referenced:
  - pg_wchar_table (global encoding table)
  - DatabaseEncoding (current database encoding setting)
  - wchar2mb_with_len (encoding-specific conversion function pointer)
- Called from (representative examples):
  - [build_regexp_match_result](../b/build_regexp_match_result.md) (in src/backend/utils/adt/regexp.c:1670)
  - [build_regexp_split_result](../b/build_regexp_split_result.md) (in src/backend/utils/adt/regexp.c:1838)
  - [regexp_fixed_prefix](../r/regexp_fixed_prefix.md) (in src/backend/utils/adt/regexp.c:2029)
  - [build_test_match_result](../b/build_test_match_result.md) (in src/test/modules/test_regex/test_regex.c:726)

## Notes and Other Information
- Primarily used in regular expression processing and pattern matching operations
- The length parameter provides bounds checking to prevent processing more characters than intended
- Returns the number of bytes written to the destination buffer
- More commonly used than pg_wchar2mb due to its safer length-limited operation
- Essential for regex result building and string manipulation functions where buffer size control is critical