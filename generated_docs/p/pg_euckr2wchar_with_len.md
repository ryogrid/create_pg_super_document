# pg_euckr2wchar_with_len

## Location
src/common/wchar.c: 210 - 215

## Overview
Converts EUC-KR (Extended Unix Code for Korean) encoded byte sequences to PostgreSQL's internal wide character format.

## Definition
```c
static int pg_euckr2wchar_with_len(const unsigned char *from, pg_wchar *to, int len)
```

## Detailed Description
This function serves as a wrapper for EUC-KR character conversion, delegating the actual conversion work to the generic EUC conversion function `pg_euc2wchar_with_len`. EUC-KR is a variable-width character encoding used for Korean text that can represent ASCII characters (1 byte) and Korean Hangul syllables, Hanja characters, and other symbols (2 bytes).

The function is part of PostgreSQL's character encoding conversion infrastructure, specifically handling the conversion from EUC-KR encoded bytes to PostgreSQL's internal wide character representation (pg_wchar). This enables proper storage, processing, and retrieval of Korean text data in PostgreSQL databases.

## Parameters / Member Variables
- `from`: Pointer to the source EUC-KR encoded byte sequence to convert
- `to`: Pointer to the destination buffer where converted wide characters will be stored  
- `len`: Maximum number of bytes to process from the source sequence

## Dependencies
- Functions called/Symbols referenced:
  - pg_euc2wchar_with_len (generic EUC to wide character conversion function)
- Called from (representative examples):
  - pg_encoding_set_invalid (character encoding setup function)

## Notes and Other Information
- This is a static function, indicating it's only used within the wchar.c compilation unit
- Acts as a thin wrapper around the generic EUC conversion function, allowing for EUC-KR specific handling if needed in the future
- Part of PostgreSQL's comprehensive character encoding support system for Asian languages
- Returns the result from the underlying pg_euc2wchar_with_len function, typically indicating the number of bytes consumed or an error code
- Enables PostgreSQL to properly handle Korean text data in databases and applications