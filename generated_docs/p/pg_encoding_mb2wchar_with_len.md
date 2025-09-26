# pg_encoding_mb2wchar_with_len

## Location
[src/backend/utils/mb/mbutils.c:993-1000](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L993-L1000)

## Overview
Converts a multibyte string to a wide character array with a specified length limit, using any specified encoding rather than the current database encoding.

## Definition
```c
int pg_encoding_mb2wchar_with_len(int encoding, const char *from, pg_wchar *to, int len)
```

## Detailed Description
This function provides the same multibyte to wide character conversion functionality as pg_mb2wchar_with_len, but allows the caller to specify any encoding rather than being limited to the current database encoding. It directly accesses the pg_wchar_table array using the provided encoding parameter to call the appropriate encoding-specific conversion function. This flexibility makes it useful for processing text data that may be in a different encoding than the current database.

## Parameters / Member Variables
- `encoding`: The specific character encoding to use for the conversion (encoding ID)
- `from`: Pointer to the source multibyte character string to be converted
- `to`: Pointer to the destination array where wide characters will be stored
- `len`: Maximum length limit for the conversion operation

## Dependencies
- Functions called/Symbols referenced:
  - pg_wchar_table (global encoding table)
  - mb2wchar_with_len (encoding-specific conversion function pointer)
- Called from (representative examples):
  - [sqlchar_to_unicode](../s/sqlchar_to_unicode.md) (in src/backend/utils/adt/xml.c:2344)

## Notes and Other Information
- This function provides encoding flexibility compared to pg_mb2wchar_with_len which uses the current database encoding
- Primarily used in XML processing where different character encodings may be encountered
- The encoding parameter must be a valid encoding ID that corresponds to an entry in pg_wchar_table
- Returns the number of wide characters produced by the conversion
- Useful for cross-encoding operations and data processing tasks