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
  - [get_decomposed_size](../g/get_decomposed_size.md) (calculates decomposition size)
  - [decompose_code](../d/decompose_code.md) (performs character decomposition)
  - [get_canonical_class](../g/get_canonical_class.md) (retrieves canonical combining class)
  - [recompose_code](../r/recompose_code.md) (attempts character recomposition)
  - ALLOC, FREE (memory management macros)
- Called from (representative examples):
  - [unicode_normalize_func](unicode_normalize_func.md) (backend SQL function)
  - [unicode_is_normalized](unicode_is_normalized.md) (normalization checking)
  - [pg_saslprep](../p/pg_saslprep.md) (SASL string preparation)
  - [main](../m/main.md) (test program)

## Notes and Other Information
- Returns null-terminated array allocated with malloc (frontend) or palloc (backend)
- Returns NULL on memory allocation failure (frontend) or reports error with ereport (backend)
- Implements canonical ordering using bubble sort with backtracking for efficiency
- Recomposition phase only applies to NFC and NFKC forms
- Follows Unicode Standard Annex #15 specification precisely
- Critical for text processing, collation, and string comparison operations
- Ensures Unicode string equivalence across different representations

## Simplified Source

```c
pg_wchar *
unicode_normalize(UnicodeNormalizationForm form, const pg_wchar *input)
{
    bool compat = (form == UNICODE_NFKC || form == UNICODE_NFKD);
    bool recompose = (form == UNICODE_NFC || form == UNICODE_NFKC);
    pg_wchar *decomp_chars, *recomp_chars;
    int decomp_size, current_size;
    const pg_wchar *p;

    // PHASE 1: Character Decomposition

    // Calculate decomposed length
    decomp_size = 0;
    for (p = input; *p; p++)
        decomp_size += get_decomposed_size(*p, compat);

    // Allocate and fill decomposed string
    decomp_chars = (pg_wchar *) ALLOC((decomp_size + 1) * sizeof(pg_wchar));
    if (decomp_chars == NULL)
        return NULL;

    current_size = 0;
    for (p = input; *p; p++)
        decompose_code(*p, compat, &decomp_chars, &current_size);
    decomp_chars[decomp_size] = '\0';

    // Early exit if empty
    if (decomp_size == 0)
        return decomp_chars;

    // PHASE 2: Canonical Ordering (bubble sort with backtracking)

    for (int count = 1; count < decomp_size; count++) {
        pg_wchar prev = decomp_chars[count - 1];
        pg_wchar next = decomp_chars[count];
        const uint8 prevClass = get_canonical_class(prev);
        const uint8 nextClass = get_canonical_class(next);

        // Skip if either is a starter (class 0) or order is correct
        if (prevClass == 0 || nextClass == 0 || prevClass <= nextClass)
            continue;

        // Swap characters and backtrack
        decomp_chars[count - 1] = next;
        decomp_chars[count] = prev;
        if (count > 1)
            count -= 2;  // Backtrack to recheck
    }

    // Return decomposed form for NFD/NFKD
    if (!recompose)
        return decomp_chars;

    // PHASE 3: Recomposition (for NFC/NFKC)

    recomp_chars = (pg_wchar *) ALLOC((decomp_size + 1) * sizeof(pg_wchar));
    if (!recomp_chars) {
        FREE(decomp_chars);
        return NULL;
    }

    // Initialize recomposition state
    int last_class = -1;
    int starter_pos = 0;
    int target_pos = 1;
    pg_wchar starter_ch = recomp_chars[0] = decomp_chars[0];

    // Process each character for recomposition
    for (int count = 1; count < decomp_size; count++) {
        pg_wchar ch = decomp_chars[count];
        int ch_class = get_canonical_class(ch);
        pg_wchar composite;

        if (last_class < ch_class && recompose_code(starter_ch, ch, &composite)) {
            // Successful composition - replace starter
            recomp_chars[starter_pos] = composite;
            starter_ch = composite;
        } else if (ch_class == 0) {
            // New starter character
            starter_pos = target_pos;
            starter_ch = ch;
            last_class = -1;
            recomp_chars[target_pos++] = ch;
        } else {
            // Non-composable combining character
            last_class = ch_class;
            recomp_chars[target_pos++] = ch;
        }
    }
    recomp_chars[target_pos] = '\0';

    FREE(decomp_chars);
    return recomp_chars;
}
```