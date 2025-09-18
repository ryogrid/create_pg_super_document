# euc_jis_20042shift_jis_2004

## Location
src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c: 75 - 221

## Overview
Core conversion function that performs the actual character-by-character conversion from EUC-JIS-2004 encoding to Shift-JIS-2004 encoding.

## Definition
```c
static int euc_jis_20042shift_jis_2004(const unsigned char *euc, unsigned char *p, int len, bool noError)
```

## Detailed Description
This is the core implementation function that handles the complex logic of converting EUC-JIS-2004 encoded text to Shift-JIS-2004 encoding. It processes the input byte by byte, handling different character planes and ranges according to JIS X 0213 specifications. The function handles ASCII characters, JIS X 0201 kana characters (plane 1), JIS X 0213 plane 1, and JIS X 0213 plane 2 characters. It performs mathematical transformations on ku (row) and ten (column) values to map between the two encoding schemes.

## Parameters / Member Variables
- `euc`: Pointer to the source string in EUC-JIS-2004 encoding
- `p`: Pointer to the destination buffer for converted Shift-JIS-2004 string
- `len`: Length of the source string in bytes
- `noError`: If true, stops conversion on error instead of throwing exception

## Dependencies
- Functions called/Symbols referenced:
  - IS_HIGHBIT_SET
  - report_invalid_encoding
  - pg_encoding_verifymbchar
  - Constants: PG_EUC_JIS_2004, SS2, SS3
- Called from:
  - euc_jis_2004_to_shift_jis_2004 (wrapper function)

## Notes and Other Information
- Located in src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c:75-221
- Static function - only accessible within the same compilation unit
- Returns the number of bytes processed from the source string
- Handles multiple character encoding planes:
  - ASCII characters (0x00-0x7F) - passed through unchanged
  - SS2 sequences (JIS X 0201 kana) - single byte output
  - SS3 sequences (JIS X 0213 plane 2) - two byte output with complex mapping
  - Regular sequences (JIS X 0213 plane 1) - two byte output
- Uses mathematical formulas to convert ku (row) and ten (column) positions between encodings
- Implements comprehensive error checking and validation
- Null-terminates the output string