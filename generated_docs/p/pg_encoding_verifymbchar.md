# pg_encoding_verifymbchar

## Location
src/common/wchar.c: 2189 - 2201

## Overview
Verifies the validity of the first multibyte character in a given string and returns its byte length if valid, or -1 if invalid.

## Definition
```c
int pg_encoding_verifymbchar(int encoding, const char *mbstr, int len)
```

## Detailed Description
This function validates the first multibyte character of a string according to the specified encoding rules. It examines the character starting at the given position and determines whether it forms a valid character sequence in the target encoding. The function is part of PostgreSQL's character encoding validation infrastructure and is crucial for ensuring data integrity when processing multibyte text.

The function delegates to encoding-specific verification functions through the `pg_wchar_table` array. Each encoding has its own verification logic that understands the specific byte patterns and constraints for that encoding. If an invalid encoding is provided, it falls back to ASCII verification.

The verification follows these rules:
- Returns the byte length of the character if it's validly encoded
- Returns -1 if the character is invalid or malformed
- Can assume len > 0 and *mbstr != '\0'
- Must test for and reject embedded zero bytes in multibyte characters
- Only validates the first character, not the entire string

## Parameters / Member Variables
- `encoding`: The character encoding identifier to use for validation
- `mbstr`: Pointer to the start of the multibyte character to verify
- `len`: The remaining length of the string (ensures bounds checking)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_VALID_ENCODING`: Macro to validate encoding identifier
  - `PG_SQL_ASCII`: Fallback encoding constant used for invalid encodings
  - `pg_wchar_table[].mbverifychar`: Encoding-specific character verification function

- Called from (representative examples):
  - `[LocalToUtf](../L/LocalToUtf.md)`: Character encoding conversion functions (conv.c)
  - Multiple Japanese encoding converters: `euc_jis_20042shift_jis_2004`, `shift_jis_20042euc_jis_2004`, etc.
  - Korean encoding converters: `euc_kr2mic`, `mic2euc_kr`
  - Chinese encoding converters: `euc_tw2big5`, `big52euc_tw`, etc.
  - String utilities: `fmtIdEnc`, `appendStringLiteral`, `PQescapeStringInternal`

## Notes and Other Information
- This function is defined in src/common/wchar.c:2189-2201
- Part of the multibyte sequence validator family alongside `pg_encoding_verifymbstr`
- Essential for preventing malformed character sequences from corrupting data
- Used extensively in character encoding conversion routines to ensure source data validity
- The verification is encoding-specific and handles various complex multibyte encoding rules
- For single-byte encodings, verification typically just returns 1 (always valid)
- Critical for security as it prevents malformed input from causing buffer overruns or data corruption