# qc_is_allowed

## Location
[src/common/unicode_norm.c:574-597](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_norm.c#L574-L597)

## Overview
Determines the Quick Check property of a Unicode codepoint for specific normalization forms (NFC/NFKC) to optimize normalization validation.

## Definition
```c
static UnicodeNormalizationQC qc_is_allowed(UnicodeNormalizationForm form, pg_wchar ch)
```

## Detailed Description
This function implements Unicode Quick Check property lookup for normalization optimization. The Quick Check mechanism allows efficient determination of whether a string requires normalization without performing the full normalization process.

The function supports two composed normalization forms:
- **NFC**: Uses NFC-specific Quick Check properties
- **NFKC**: Uses NFKC-specific Quick Check properties

For codepoints not found in the Quick Check tables, the function defaults to `UNICODE_NORM_QC_YES`, meaning the character does not prevent the string from being in the specified normalization form.

The Quick Check properties can return:
- YES: Character allows the normalization form
- NO: Character prevents the normalization form  
- MAYBE: Character requires further checking

## Parameters / Member Variables
- `form`: The normalization form to check (UNICODE_NFC or UNICODE_NFKC only)
- `ch`: The Unicode codepoint to check Quick Check properties for

## Dependencies
- Functions called/Symbols referenced:
  - UnicodeNormalizationForm (normalization form enumeration)
  - [UnicodeNormalizationQC](../U/UnicodeNormalizationQC.md) (Quick Check result enumeration)
  - [pg_unicode_normprops](../p/pg_unicode_normprops.md) (normalization properties structure)
  - UNICODE_NFC, UNICODE_NFKC (normalization form constants)
  - [qc_hash_lookup](qc_hash_lookup.md) (hash table lookup function)
  - UnicodeNormInfo_NFC_QC, UnicodeNormInfo_NFKC_QC (Quick Check tables)
  - UNICODE_NORM_QC_YES (default Quick Check result)
- Called from (representative examples):
  - [unicode_is_normalized_quickcheck](../u/unicode_is_normalized_quickcheck.md)

## Notes and Other Information
- Returns `UnicodeNormalizationQC` enumeration value
- Only supports composed forms (NFC/NFKC) as decomposed forms (NFD/NFKD) have different Quick Check requirements
- Defaults to YES for codepoints not in Quick Check tables
- Critical for performance optimization in normalization validation
- Part of Unicode Standard Annex #15 Quick Check algorithm
- Enables early termination of normalization checks when possible