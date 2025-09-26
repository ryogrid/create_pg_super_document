# pg_euctw_verifychar

## Location
src/common/wchar.c: 1228 - 1277

## Overview
Validates a single character in EUC-TW (Extended Unix Code for Traditional Chinese) encoding and returns the number of bytes consumed if valid.

## Definition
```c
static int pg_euctw_verifychar(const unsigned char *s, int len)
```

## Detailed Description
This function validates individual characters in EUC-TW encoding, which is used for Traditional Chinese text. EUC-TW is a variable-width encoding that supports multiple character planes:

- ASCII characters (0x00-0x7F): single byte
- CNS 11643 Plane 1: two bytes (high bit set, excluding SS2/SS3)
- CNS 11643 Planes 1-7: four bytes (starting with SS2 0x8E)
- SS3 (0x8F): currently unused and treated as invalid

The function uses a switch statement to handle different cases based on the first byte. For SS2 sequences, it validates that the plane number is within the valid range (0xA1-0xA7) and that subsequent bytes conform to EUC range requirements.

## Parameters / Member Variables
- `s`: Pointer to the unsigned char array containing the character to validate
- `len`: Maximum number of bytes available in the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - SS2 (0x8E - single shift 2 constant)
  - SS3 (0x8F - single shift 3 constant)
  - IS_EUC_RANGE_VALID (macro to validate EUC byte range 0xA1-0xFE)
  - IS_HIGHBIT_SET (macro to check if high bit is set)
- Called from (representative examples):
  - pg_euctw_verifystr
  - pg_encoding_set_invalid

## Notes and Other Information
- Returns the number of bytes consumed (1, 2, or 4) for valid characters, -1 for invalid sequences
- SS2 sequences represent CNS 11643 planes 1-7 and require exactly 4 bytes
- SS3 sequences are currently unused in EUC-TW and always return -1
- CNS 11643 Plane 1 characters use 2-byte sequences without SS2/SS3 prefixes  
- The function performs range validation on plane numbers for SS2 sequences (must be 0xA1-0xA7)
- Part of PostgreSQL's character encoding validation system ensuring data integrity for Traditional Chinese text