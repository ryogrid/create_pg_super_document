# pg_mule_verifychar

## Location
src/common/wchar.c: 1360 - 1380

## Overview
Verifies the validity of a single multi-byte character in MULE (Multi-lingual Emacs) encoding by checking that all continuation bytes have the high bit set.

## Definition


## Detailed Description
This function validates a single MULE-encoded character by performing two key checks:
1. Determines the expected length of the multi-byte character using 
2. Verifies that all continuation bytes (bytes after the first) have the high bit set using 

The function ensures that multi-byte MULE characters follow the proper encoding rules where continuation bytes must have their most significant bit set to 1. This is a fundamental requirement of the MULE encoding scheme to distinguish continuation bytes from single-byte ASCII characters.

## Parameters / Member Variables
- : Pointer to the beginning of the character sequence to verify
- : Maximum number of bytes available in the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - pg_mule_mblen
  - IS_HIGHBIT_SET
- Called from (representative examples):
  - pg_mule_verifystr
  - pg_encoding_set_invalid

## Notes and Other Information
- Returns the length of the valid multi-byte character if successful, or -1 if invalid
- Part of PostgreSQL's character encoding validation infrastructure for MULE encoding
- The function is static, indicating it's only used within the wchar.c compilation unit
- MULE encoding was historically used by Emacs for multi-lingual text support