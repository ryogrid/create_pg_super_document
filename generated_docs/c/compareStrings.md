# compareStrings

## Location
src/backend/utils/adt/jsonpath_exec.c: 3274 - 3340

## Overview
Compares two strings using Unicode codepoint collation in the current server encoding, with optimizations for UTF-8 and ASCII.

## Definition
static int compareStrings(const char *mbstr1, int mblen1, const char *mbstr2, int mblen2)

## Detailed Description
The compareStrings function performs string comparison using Unicode codepoint ordering while handling various database encodings. For UTF-8 and ASCII encodings, it takes advantage of the property that byte-order comparison matches codepoint comparison, providing optimal performance. For other encodings, it converts strings to UTF-8 first, performs the comparison, and includes a fallback binary comparison for cases where Unicode codepoints are equal but the original representations differ. This approach balances standard conformance with performance, particularly for equality operations.

## Parameters / Member Variables
- `mbstr1`: First string in the current server encoding
- `mblen1`: Length of the first string in bytes
- `mbstr2`: Second string in the current server encoding
- `mblen2`: Length of the second string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - GetDatabaseEncoding
  - binaryCompareStrings
  - pg_server_to_any
  - pfree
  - PG_SQL_ASCII
  - PG_UTF8
- Called from (representative examples):
  - compareItems

## Notes and Other Information
The function includes special handling for encoding edge cases where the same Unicode characters may have different byte representations. The fallback to binary comparison when Unicode codepoints are equal ensures that the equality operator (`==`) can use simple binary comparison for performance. Future enhancements could include input string normalization for strict standard conformance. Memory management is carefully handled for dynamically allocated UTF-8 conversions.