# pg_mb2wchar_with_len

## Location
[src/backend/utils/mb/mbutils.c:986-992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mb/mbutils.c#L986-L992)

## Overview
Converts a multibyte string to a wide character array with a specified length limit, using the current database encoding.

## Definition

```c
int
pg_mb2wchar_with_len(const char *from, pg_wchar *to, int len)
```
## Detailed Description
This function converts a multibyte character string to an array of wide characters (pg_wchar) with a length restriction. It acts as a wrapper that delegates the actual conversion to the appropriate encoding-specific conversion function stored in the pg_wchar_table array based on the current DatabaseEncoding. The function respects the specified length limit during conversion, making it safer for bounded operations.

## Parameters / Member Variables
- : Pointer to the source multibyte character string to be converted
- : Pointer to the destination array where wide characters will be stored
- : Maximum length limit for the conversion operation

## Dependencies
- Functions called/Symbols referenced:
  - pg_wchar_table (global encoding table)
  - DatabaseEncoding (current database encoding setting)
  - mb2wchar_with_len (encoding-specific conversion function pointer)
- Called from (representative examples):
  - [regcomp_auth_token](../r/regcomp_auth_token.md) (in src/backend/libpq/hba.c:315)
  - [regexec_auth_token](../r/regexec_auth_token.md) (in src/backend/libpq/hba.c:356)
  - [NIAddAffix](../N/NIAddAffix.md) (in src/backend/tsearch/spell.c:732)
  - [TParserInit](../T/TParserInit.md) (in src/backend/tsearch/wparser_def.c:312)
  - [RE_compile_and_cache](../R/RE_compile_and_cache.md) (in src/backend/utils/adt/regexp.c:193)
  - [replace_text_regexp](../r/replace_text_regexp.md) (in src/backend/utils/adt/varlena.c:4228)

## Notes and Other Information
- This function is primarily used in text search, regular expression processing, and authentication token handling
- The length parameter provides bounds checking to prevent buffer overflows
- The actual conversion logic is encoding-specific and handled by function pointers in pg_wchar_table
- Returns the number of wide characters produced by the conversion

## Simplified Source

```c
// Simplified version of pg_mb2wchar_with_len
int pg_mb2wchar_with_len(const char *from, pg_wchar *to, int len) {
    // Get the encoding-specific conversion function from the table
    // and call it with the current database encoding
    return pg_wchar_table[DatabaseEncoding->encoding].mb2wchar_with_len(
        (const unsigned char *) from, to, len);
}
```

Key simplifications made:
- Added clear comment explaining the function pointer lookup and delegation
- Broke the function call into multiple lines for better readability
- Preserved the cast to unsigned char for the source parameter
- Maintained the length-limited conversion functionality
- This function is already quite simple as it's primarily a wrapper