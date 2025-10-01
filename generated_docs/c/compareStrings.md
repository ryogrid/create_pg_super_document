# compareStrings

## Location
[src/backend/utils/adt/jsonpath_exec.c:3274-3340](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3274-L3340)

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
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [binaryCompareStrings](../b/binaryCompareStrings.md)
  - [pg_server_to_any](../p/pg_server_to_any.md)
  - [pfree](../p/pfree.md)
  - PG_SQL_ASCII
  - PG_UTF8
- Called from (representative examples):
  - [compareItems](compareItems.md)

## Notes and Other Information
The function includes special handling for encoding edge cases where the same Unicode characters may have different byte representations. The fallback to binary comparison when Unicode codepoints are equal ensures that the equality operator (`==`) can use simple binary comparison for performance. Future enhancements could include input string normalization for strict standard conformance. Memory management is carefully handled for dynamically allocated UTF-8 conversions.

## Simplified Source

```c
static int
compareStrings(const char *mbstr1, int mblen1,
               const char *mbstr2, int mblen2)
{
    // Fast path for UTF-8 and ASCII - byte comparison equals codepoint comparison
    if (GetDatabaseEncoding() == PG_SQL_ASCII || GetDatabaseEncoding() == PG_UTF8) {
        return binaryCompareStrings(mbstr1, mblen1, mbstr2, mblen2);
    }

    // For other encodings, convert to UTF-8 first
    char *utf8str1 = pg_server_to_any(mbstr1, mblen1, PG_UTF8);
    char *utf8str2 = pg_server_to_any(mbstr2, mblen2, PG_UTF8);

    // Determine lengths (conversion may or may not have occurred)
    int utf8len1 = (mbstr1 == utf8str1) ? mblen1 : strlen(utf8str1);
    int utf8len2 = (mbstr2 == utf8str2) ? mblen2 : strlen(utf8str2);

    // Compare the UTF-8 strings
    int cmp = binaryCompareStrings(utf8str1, utf8len1, utf8str2, utf8len2);

    // If no conversion happened, we're done
    if (mbstr1 == utf8str1 && mbstr2 == utf8str2)
        return cmp;

    // Clean up allocated memory
    if (mbstr1 != utf8str1)
        pfree(utf8str1);
    if (mbstr2 != utf8str2)
        pfree(utf8str2);

    // If Unicode codepoints are equal, fall back to binary comparison
    // of original strings (handles encoding edge cases)
    if (cmp == 0)
        return binaryCompareStrings(mbstr1, mblen1, mbstr2, mblen2);

    return cmp;
}
```