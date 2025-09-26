# pg_wchar2mule_with_len

## Location
[src/common/wchar.c:727-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L727-L792)

## Overview
Converts PostgreSQL's wide character representation (pg_wchar) back to MULE internal encoding, performing the reverse operation of pg_mule2wchar_with_len.

## Definition
static int pg_wchar2mule_with_len(const pg_wchar *from, unsigned char *to, int len)

## Detailed Description
The  function converts characters from PostgreSQL's internal wide character format back to MULE internal encoding. It extracts the leading byte from each pg_wchar value to determine the character type and reconstructs the appropriate MULE byte sequence.

The function handles the reverse conversion for all MULE character types:
- **LC1 characters**: Extracts 2-byte sequences for single-byte character sets
- **LC2 characters**: Extracts 3-byte sequences for double-byte character sets
- **LCPRV1_A/B characters**: Reconstructs 3-byte private use single-byte sequences
- **LCPRV2_A/B characters**: Reconstructs 4-byte private use double-byte sequences
- **ASCII characters**: Single-byte output for standard characters

The conversion uses bit shifting and masking operations to extract the original byte values that were packed into the pg_wchar format.

## Parameters
- : Pointer to source pg_wchar array (not necessarily null-terminated)
- : Pointer to destination MULE-encoded byte array (caller must allocate sufficient space)
- : Number of pg_wchar characters to process

## Dependencies
- Functions called/Symbols referenced:
  - IS_LC1 (macro to test for LC1 character type)
  - IS_LC2 (macro to test for LC2 character type)
  - IS_LCPRV1_A_RANGE (macro to test for LCPRV1_A range)
  - IS_LCPRV1_B_RANGE (macro to test for LCPRV1_B range)
  - IS_LCPRV2_A_RANGE (macro to test for LCPRV2_A range)
  - IS_LCPRV2_B_RANGE (macro to test for LCPRV2_B range)
  - LCPRV1_A, LCPRV1_B, LCPRV2_A, LCPRV2_B (constants for private use prefixes)
- Called from:
  - pg_encoding_set_invalid (as part of encoding function table setup)

## Notes and Other Information
- Returns the number of bytes produced in the output MULE sequence
- Null-terminates the output byte array
- Complements pg_mule2wchar_with_len for bidirectional MULE conversion
- Part of PostgreSQL's internal character encoding conversion system
- The caller is responsible for allocating sufficient space in the destination array
- Used internally when PostgreSQL needs to output text in MULE encoding
- Processes input until the specified length is reached or a null character is encountered