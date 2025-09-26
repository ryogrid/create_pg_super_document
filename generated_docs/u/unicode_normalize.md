# unicode_normalize

## Location
[src/common/unicode_norm.c:402-542](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_norm.c#L402-L542)

## Overview
Normalizes a Unicode string to the specified normalization form (NFC, NFD, NFKC, or NFKD) according to Unicode Standard Annex #15.

## Definition
```c
pg_wchar *unicode_normalize(UnicodeNormalizationForm form, const pg_wchar *input)
```

## Detailed Description
This function implements the complete Unicode normalization process supporting all four standard normalization forms:
- **NFC**: Normalization Form Composed (canonical decomposition + recomposition)
- **NFD**: Normalization Form Decomposed (canonical decomposition only)  
- **NFKC**: Normalization Form Compatibility Composed (compatibility decomposition + recomposition)
- **NFKD**: Normalization Form Compatibility Decomposed (compatibility decomposition only)

The normalization process consists of up to three phases:
1. **Decomposition**: Breaks composite characters into their constituent parts using either canonical or compatibility decomposition
2. **Canonical Ordering**: Sorts combining characters by their canonical combining class to ensure consistent order
3. **Recomposition** (NFC/NFKC only): Recombines characters where possible to form the most composed representation

Memory allocation differs between frontend (malloc) and backend (palloc) environments, with appropriate error handling for out-of-memory conditions.

## Parameters / Member Variables
- `form`: The target normalization form (UNICODE_NFC, UNICODE_NFD, UNICODE_NFKC, or UNICODE_NFKD)
- `input`: Null-terminated array of Unicode codepoints to be normalized

## Dependencies
- Functions called/Symbols referenced:
  - UnicodeNormalizationForm (normalization form enumeration)
  - UNICODE_NFC, UNICODE_NFD, UNICODE_NFKC, UNICODE_NFKD (normalization form constants)
  - get_decomposed_size (calculates decomposition size)
  - decompose_code (performs character decomposition)
  - get_canonical_class (retrieves canonical combining class)
  - recompose_code (attempts character recomposition)
  - ALLOC, FREE (memory management macros)
- Called from (representative examples):
  - unicode_normalize_func (backend SQL function)
  - unicode_is_normalized (normalization checking)
  - pg_saslprep (SASL string preparation)
  - main (test program)

## Notes and Other Information
- Returns null-terminated array allocated with malloc (frontend) or palloc (backend)
- Returns NULL on memory allocation failure (frontend) or reports error with ereport (backend)
- Implements canonical ordering using bubble sort with backtracking for efficiency
- Recomposition phase only applies to NFC and NFKC forms
- Follows Unicode Standard Annex #15 specification precisely
- Critical for text processing, collation, and string comparison operations
- Ensures Unicode string equivalence across different representations