# pg_euc_dsplen

## Location
[src/common/wchar.c:160-178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L160-L178)

## Overview
Calculates the display length (number of screen columns) for EUC (Extended Unix Code) encoded characters.

## Definition

```c
static inline int
pg_euc_dsplen(const unsigned char *s)
```
## Detailed Description
This function determines how many screen columns an EUC-encoded character will occupy when displayed. EUC encoding is a variable-width character encoding used primarily for Asian languages. The function handles different types of EUC characters:

- Single Shift 2 (SS2) characters: 2 display columns
- Single Shift 3 (SS3) characters: 2 display columns  
- High-bit set characters: 2 display columns
- ASCII characters: Uses pg_ascii_dsplen for proper handling

The function is designed to work with EUC character encoding schemes where multi-byte characters typically occupy 2 display columns (full-width), while ASCII characters may occupy 1 column.

## Parameters / Member Variables
- `*s`: Pointer to the first byte of the EUC-encoded character to measure
## Dependencies
- Functions called/Symbols referenced:
  - SS2 (Single Shift 2 constant)
  - SS3 (Single Shift 3 constant)
  - IS_HIGHBIT_SET (macro to check if high bit is set)
  - [pg_ascii_dsplen](pg_ascii_dsplen.md) (function for ASCII display length calculation)
- Called from (representative examples):
  - [pg_euckr_dsplen](pg_euckr_dsplen.md) (EUC-KR display length calculation)
  - [pg_johab_dsplen](pg_johab_dsplen.md) (Johab encoding display length calculation)

## Notes and Other Information
- This is a static inline function for performance optimization
- Returns 2 columns for most EUC multi-byte characters, which is typical for full-width Asian characters
- Falls back to ASCII display length calculation for standard ASCII characters
- Part of PostgreSQL's character encoding support infrastructure

## Simplified Source

```c
static inline int pg_euc_dsplen(const unsigned char *s) {
    // Determine EUC character display width
    if (*s == SS2)                    // Single Shift 2: 2 columns
        return 2;
    else if (*s == SS3)               // Single Shift 3: 2 columns
        return 2;
    else if (IS_HIGHBIT_SET(*s))      // High-bit set: 2 columns
        return 2;
    else                              // ASCII: use ASCII rules
        return pg_ascii_dsplen(s);
}
```