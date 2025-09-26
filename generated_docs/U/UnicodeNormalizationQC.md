# UnicodeNormalizationQC

## Location
[src/include/common/unicode_norm.h:33-39](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/unicode_norm.h#L33-L39)

## Overview
UnicodeNormalizationQC is an enumeration type that represents the result of Unicode normalization quick check operations, following the Unicode Standard Annex #15 (UAX #15) specification for Unicode text normalization.

## Definition
```c
typedef enum
{
    UNICODE_NORM_QC_NO = 0,
    UNICODE_NORM_QC_YES = 1,  
    UNICODE_NORM_QC_MAYBE = -1,
} UnicodeNormalizationQC;
```

## Detailed Description
This enumeration encodes the three possible results of a Unicode normalization quick check as defined in UAX #15. The quick check is an optimization technique that allows efficient determination of whether a Unicode string is already in a particular normalized form without performing the full normalization process.

The enum follows the Unicode specification where:
- QC_YES means the string is definitely in the target normalization form
- QC_NO means the string is definitely not in the target normalization form  
- QC_MAYBE means the quick check is inconclusive and full normalization may be needed

The implementation is specifically optimized for NFC and NFKC forms, while NFD and NFKD forms always return QC_MAYBE to avoid including large lookup tables since the decomposed forms are less commonly checked and the full normalization is relatively faster.

## Parameters / Member Variables
- `UNICODE_NORM_QC_NO` (0): Indicates the input string is definitively not in the target normalization form
- `UNICODE_NORM_QC_YES` (1): Indicates the input string is definitively in the target normalization form  
- `UNICODE_NORM_QC_MAYBE` (-1): Indicates the quick check is inconclusive; full normalization is required to determine the result

## Dependencies
- Functions using this type:
  - [unicode_is_normalized_quickcheck](../u/unicode_is_normalized_quickcheck.md) (returns this type)
  - [qc_is_allowed](../q/qc_is_allowed.md) (returns this type)
  - [qc_hash_lookup](../q/qc_hash_lookup.md) (accesses quickcheck field of this type)
- Called from:
  - [unicode_is_normalized](../u/unicode_is_normalized.md) (in varlena.c)
  - Various internal normalization functions in unicode_norm.c

## Notes and Other Information
- Based on Unicode Standard Annex #15 (UAX #15) for Unicode Normalization Forms
- The enum values are specifically chosen to match the Unicode specification requirements
- Used primarily as an optimization to avoid expensive full normalization when possible
- The MAYBE result requires falling back to full normalization to get a definitive answer
- Part of PostgreSQL's Unicode text processing infrastructure, used by both frontend and backend code
- The quick check is only implemented for NFC and NFKC forms due to lookup table size considerations