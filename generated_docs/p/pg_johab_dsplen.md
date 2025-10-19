# pg_johab_dsplen

## Location
[src/common/wchar.c:429-440](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L429-L440)

## Overview
A display length function for JOHAB encoding that determines the number of display columns required for a character sequence.

## Definition
```c
static int pg_johab_dsplen(const unsigned char *s)
```

## Detailed Description
This function calculates the display length for JOHAB (Korean) encoded characters. JOHAB is a Korean character encoding that uses 1-2 bytes per character. The function is implemented as a simple wrapper around `pg_euc_dsplen`, indicating that JOHAB encoding follows the same display length rules as EUC (Extended Unix Code) encodings.

The function returns the number of display columns that would be occupied by the character at the given byte position, which is essential for proper text formatting and display alignment in terminal applications and text processing.

## Parameters / Member Variables
- `s`: Pointer to the unsigned character byte sequence to analyze for display length

## Dependencies
- Functions called/Symbols referenced:
  - [pg_euc_dsplen](pg_euc_dsplen.md)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md)

## Notes and Other Information
- This is a static function, meaning it has internal linkage and is only accessible within the same translation unit (wchar.c)
- JOHAB encoding shares display length characteristics with EUC encodings, hence the delegation to pg_euc_dsplen
- The function is part of PostgreSQL's character encoding support infrastructure
- JOHAB is primarily used for Korean text representation and is one of the legacy Korean encodings supported by PostgreSQL

## Simplified Source

```c
static int pg_johab_dsplen(const unsigned char *s) {
    // JOHAB shares display length characteristics with EUC encoding
    return pg_euc_dsplen(s);
}
```