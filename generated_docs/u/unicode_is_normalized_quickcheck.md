# unicode_is_normalized_quickcheck

## Location
[src/common/unicode_norm.c:598-634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_norm.c#L598-L634)

## Overview
Performs a quick normalization check on a Unicode string to determine whether it is already in the specified normalization form without performing the full normalization process.

## Definition
```c
UnicodeNormalizationQC unicode_is_normalized_quickcheck(UnicodeNormalizationForm form, const pg_wchar *input)
```

## Detailed Description
This function implements a "quick check" algorithm as defined in Unicode Standard Annex #15 (UAX #15) to efficiently determine if a Unicode string is already in a specific normalization form. The quick check can return one of three results:

- **UNICODE_NORM_QC_YES**: The string is definitely in the specified normalization form
- **UNICODE_NORM_QC_NO**: The string is definitely not in the specified normalization form  
- **UNICODE_NORM_QC_MAYBE**: The normalization status cannot be determined quickly and requires full normalization

The function optimizes performance by:
1. **Skipping decomposed forms**: For NFD and NFKD forms, it immediately returns UNICODE_NORM_QC_MAYBE since the lookup tables for these forms are large and the full normalization is relatively fast (no recomposition needed).
2. **Canonical class ordering check**: It verifies that characters appear in proper canonical combining class order, which is required for normalized text.
3. **Per-character quick check**: It uses precomputed lookup tables to check if individual characters have normalization properties that would require processing.

The algorithm processes each character in the input string and maintains the canonical combining class of the previous character to ensure proper ordering. If any character fails the quick check or violates canonical ordering, the function can immediately return a definitive result.

## Parameters / Member Variables
- `form`: The target Unicode normalization form to check against (NFC, NFD, NFKC, or NFKD)
- `input`: Null-terminated array of Unicode code points (pg_wchar) to be checked

## Dependencies
- Functions called/Symbols referenced:
  - `[get_canonical_class](../g/get_canonical_class.md)`: Retrieves the canonical combining class for a Unicode code point
  - `[qc_is_allowed](../q/qc_is_allowed.md)`: Performs character-level quick check using precomputed tables
  - `UNICODE_NFD`, `UNICODE_NFKD`: Normalization form constants for decomposed forms
  - `UNICODE_NORM_QC_YES`, `UNICODE_NORM_QC_NO`, `UNICODE_NORM_QC_MAYBE`: Return value constants
- Called from (representative examples):
  - `[unicode_is_normalized](unicode_is_normalized.md)`: Main normalization checking function in backend

## Notes and Other Information
- The function is part of PostgreSQL's Unicode normalization support, used primarily for text processing and collation
- For NFD and NFKD forms, the quick check is bypassed because the lookup tables would be prohibitively large, and these forms are less commonly used
- The canonical class ordering check (lastCanonicalClass > canonicalClass) implements the Unicode requirement that combining marks must appear in canonical order
- Performance is optimized for the common case where strings are already normalized (returns UNICODE_NORM_QC_YES)
- This is a read-only operation that does not modify the input string
- The function is available in both frontend and backend code contexts

## Simplified Source

```c
UnicodeNormalizationQC unicode_is_normalized_quickcheck(UnicodeNormalizationForm form, const pg_wchar *input) {
    uint8 lastCanonicalClass = 0;
    UnicodeNormalizationQC result = UNICODE_NORM_QC_YES;

    // Skip quickcheck for decomposed forms (NFD/NFKD) - use full normalization instead
    // (avoids large lookup tables and decomposition is fast without recomposition)
    if (form == UNICODE_NFD || form == UNICODE_NFKD)
        return UNICODE_NORM_QC_MAYBE;

    // Check each character in the string
    for (const pg_wchar *p = input; *p; p++) {
        pg_wchar ch = *p;
        uint8 canonicalClass = get_canonical_class(ch);

        // Check canonical combining class ordering (required for normalized text)
        if (lastCanonicalClass > canonicalClass && canonicalClass != 0)
            return UNICODE_NORM_QC_NO;

        // Check if character needs normalization processing
        UnicodeNormalizationQC check = qc_is_allowed(form, ch);
        if (check == UNICODE_NORM_QC_NO)
            return UNICODE_NORM_QC_NO;
        else if (check == UNICODE_NORM_QC_MAYBE)
            result = UNICODE_NORM_QC_MAYBE;

        lastCanonicalClass = canonicalClass;
    }

    return result;
}
```