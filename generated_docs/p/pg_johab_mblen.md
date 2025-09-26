# pg_johab_mblen

## Location
src/common/wchar.c: 423 - 428

## Overview
Determines the byte length of a single multibyte character in JOHAB (Korean standard) encoding.

## Definition
```c
static int pg_johab_mblen(const unsigned char *s)
```

## Detailed Description
This function determines the number of bytes that comprise a single character in JOHAB encoding, which is a Korean character encoding standard. JOHAB is structurally similar to EUC encoding in terms of multibyte character length determination, so this function simply delegates to the existing EUC multibyte length function (`pg_euc_mblen`).

JOHAB encoding uses:
- **Single-byte characters**: ASCII characters (1 byte)
- **Double-byte characters**: Korean Hangul and Hanja characters (2 bytes)

The function serves as a specialized interface for JOHAB encoding while leveraging the common EUC character length logic.

## Parameters / Member Variables
- `s`: Pointer to the first byte of a JOHAB encoded character sequence

## Dependencies
- Functions called/Symbols referenced:
  - pg_euc_mblen (function to determine EUC character byte length)
- Called from (representative examples):
  - pg_johab_verifychar (for character validation in JOHAB encoding)
  - pg_encoding_set_invalid (indirectly through encoding function tables)

## Notes and Other Information
- Returns an integer representing the number of bytes in the character (1 or 2 for JOHAB)
- Leverages existing EUC length determination logic due to structural similarity
- Used for character boundary detection in JOHAB Korean text processing
- JOHAB is one of the Korean encoding standards supported by PostgreSQL
- Essential for proper parsing and processing of Korean text in JOHAB encoding
- Part of PostgreSQL's comprehensive Asian character encoding support system