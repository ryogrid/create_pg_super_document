# pg_eucjp2wchar_with_len

## Location
[src/common/wchar.c:179-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L179-L184)

## Overview
Converts EUC-JP (Extended Unix Code for Japanese) encoded byte sequences to PostgreSQL's internal wide character format.

## Definition
```c
static int pg_eucjp2wchar_with_len(const unsigned char *from, pg_wchar *to, int len)
```

## Detailed Description
This function serves as a wrapper for EUC-JP character conversion, delegating the actual conversion work to the generic EUC conversion function `pg_euc2wchar_with_len`. EUC-JP is a variable-width character encoding used for Japanese text that can represent ASCII characters (1 byte), half-width katakana (2 bytes), and full-width characters including hiragana, katakana, and kanji (2-3 bytes).

The function is part of PostgreSQL's character encoding conversion infrastructure, specifically handling the conversion from EUC-JP encoded bytes to PostgreSQL's internal wide character representation (pg_wchar).

## Parameters / Member Variables
- `from`: Pointer to the source EUC-JP encoded byte sequence to convert
- `to`: Pointer to the destination buffer where converted wide characters will be stored
- `len`: Maximum number of bytes to process from the source sequence

## Dependencies
- Functions called/Symbols referenced:
  - [pg_euc2wchar_with_len](pg_euc2wchar_with_len.md) (generic EUC to wide character conversion function)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (character encoding setup function)

## Notes and Other Information
- This is a static function, indicating it's only used within the wchar.c compilation unit
- Acts as a thin wrapper around the generic EUC conversion function, allowing for EUC-JP specific handling if needed in the future
- Part of PostgreSQL's comprehensive character encoding support system
- Returns the result from the underlying pg_euc2wchar_with_len function, typically indicating the number of bytes consumed or an error code

## Simplified Source

```c
static int pg_eucjp2wchar_with_len(const unsigned char *from, pg_wchar *to, int len) {
    // EUC-JP uses the same conversion as generic EUC
    return pg_euc2wchar_with_len(from, to, len);
}
```