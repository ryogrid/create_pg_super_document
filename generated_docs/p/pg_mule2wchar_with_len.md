# pg_mule2wchar_with_len

## Location
[src/common/wchar.c:674-726](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L674-L726)

## Overview
Converts MULE internal encoding to PostgreSQL's wide character representation (pg_wchar), handling multi-byte character sequences of varying lengths.

## Definition
static int pg_mule2wchar_with_len(const unsigned char *from, pg_wchar *to, int len)

## Detailed Description
The  function converts characters from MULE internal encoding to PostgreSQL's internal wide character format. MULE (MULti-lingual Enhancement) is an encoding system that can represent characters from multiple character sets simultaneously.

The function processes different MULE character types:
- **LC1 characters**: 2-byte sequences for single-byte character sets
- **LCPRV1 characters**: 3-byte sequences (private use single-byte sets)
- **LC2 characters**: 3-byte sequences for double-byte character sets  
- **LCPRV2 characters**: 4-byte sequences (private use double-byte sets)
- **ASCII characters**: Single-byte characters (assumed for unrecognized bytes)

Each character type is packed differently into the pg_wchar format using bit shifting and OR operations to preserve the original character information.

## Parameters
- : Pointer to source MULE-encoded byte sequence (not necessarily null-terminated)
- : Pointer to destination pg_wchar array (caller must allocate sufficient space)
- : Length of the source byte sequence to process

## Dependencies
- Functions called/Symbols referenced:
  - IS_LC1 (macro to test for LC1 character type)
  - IS_LCPRV1 (macro to test for LCPRV1 character type)
  - IS_LC2 (macro to test for LC2 character type)
  - IS_LCPRV2 (macro to test for LCPRV2 character type)
- Called from:
  - pg_encoding_set_invalid (as part of encoding function table setup)

## Notes and Other Information
- Returns the number of wide characters produced in the conversion
- Null-terminates the output array
- Handles variable-length input sequences safely by checking remaining length
- Part of PostgreSQL's internal character encoding conversion system
- The caller is responsible for allocating sufficient space in the destination array
- Used internally when PostgreSQL needs to work with MULE-encoded text data
- Stops processing when input length is exhausted or null character is encountered