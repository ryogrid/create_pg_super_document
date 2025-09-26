# pg_unicode_normprops

## Location
src/include/common/unicode_normprops_table.h: 13 - 23

## Overview
A structure type that represents Unicode normalization quick check properties for individual Unicode code points, designed with bit fields for memory efficiency.

## Definition


## Detailed Description
This structure is a compact representation of Unicode normalization quick check information for specific code points. It uses bit fields to minimize memory usage while storing essential normalization data. The structure is part of PostgreSQL's comprehensive Unicode normalization system, which implements the Unicode Standard's normalization forms (NFC, NFD, NFKC, NFKD).

The quick check mechanism allows for efficient determination of whether a string needs normalization. Instead of performing full normalization on every string, the system can quickly check if characters have special normalization properties that might require processing.

This structure is generated automatically from Unicode data files and is used in static lookup tables throughout PostgreSQL's Unicode processing infrastructure.

## Parameters / Member Variables
- : A 21-bit field storing the Unicode code point value (sufficient for all Unicode code points up to U+10FFFF)
- : A 4-bit signed field storing the normalization quick check result, corresponding to UnicodeNormalizationQC enum values (YES, NO, MAYBE)

## Dependencies
- Functions called/Symbols referenced:
  - UnicodeNormalizationQC (enum type referenced in quickcheck field)
  - UNICODE_NORM_QC_MAYBE (constant used in static initializations)

- Called from (representative examples):
  - unicode_normalize function (src/common/unicode_norm.c:542)
  - qc_is_allowed function (src/common/unicode_norm.c:576)
  - pg_unicode_norminfo structures
  - NFC_QC_hash_func lookup operations

## Notes and Other Information
- Generated automatically by src/common/unicode/generate-unicode_normprops_table.pl
- Uses bit fields to pack data efficiently: 21 bits for codepoint + 4 bits for quickcheck = 25 bits total
- Part of static lookup tables like UnicodeNormProps_NFC_QC[]
- The quickcheck field can represent: UNICODE_NORM_QC_YES, UNICODE_NORM_QC_NO, UNICODE_NORM_QC_MAYBE
- Essential for implementing Unicode Standard normalization algorithms efficiently
- Used across multiple normalization forms (NFC, NFKC, etc.) with different hash functions and lookup tables
- Optimized for space efficiency due to potentially large Unicode property tables