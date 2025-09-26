# pg_mule_mblen

## Location
[src/common/wchar.c:793-810](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L793-L810)

## Overview
Determines the byte length of a MULE internal encoding character by examining its leading byte to identify the character type.

## Definition
int pg_mule_mblen(const unsigned char *s)

## Detailed Description
The  function calculates the number of bytes that comprise a single character in MULE internal encoding. It examines the first byte of a character sequence to determine which MULE character type it represents and returns the corresponding byte length.

The function recognizes all standard MULE character types:
- **LC1 characters**: Single-byte character sets encoded in 2 bytes
- **LCPRV1 characters**: Private use single-byte character sets in 3 bytes
- **LC2 characters**: Double-byte character sets encoded in 3 bytes
- **LCPRV2 characters**: Private use double-byte character sets in 4 bytes
- **ASCII characters**: Standard single-byte characters (default case)

This function is essential for properly parsing MULE-encoded text, allowing other functions to advance through character sequences correctly without splitting multi-byte characters.

## Parameters
- : Pointer to the first byte of a MULE-encoded character sequence

## Dependencies
- Functions called/Symbols referenced:
  - IS_LC1 (macro to test for LC1 character type)
  - IS_LCPRV1 (macro to test for LCPRV1 character type)
  - IS_LC2 (macro to test for LC2 character type)
  - IS_LCPRV2 (macro to test for LCPRV2 character type)
- Called from:
  - mic2latin (MULE to Latin conversion)
  - mic2latin_with_table (MULE to Latin conversion with table lookup)
  - pg_mule_verifychar (MULE character validation)
  - pg_encoding_set_invalid (as part of encoding function table setup)

## Notes and Other Information
- This is a public function (exported for direct use by conv.c)
- Returns 1, 2, 3, or 4 depending on the MULE character type
- Assumes ASCII encoding for unrecognized byte values
- Critical for text processing functions that need to iterate through MULE-encoded strings
- Part of PostgreSQL's multi-byte character support infrastructure
- Used extensively in character encoding conversion routines