# r_SUFFIX_AN_OK

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:154-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c#L154-L158)

## Overview
A static boolean function in the Indonesian stemmer that validates whether the '-an' suffix removal is acceptable based on morphological rules.

## Definition
```c
static int r_SUFFIX_AN_OK(struct SN_env * z)
```

## Detailed Description
This function serves as a validation rule in the Indonesian stemming process. It checks the morphological context stored in z->I[0] to determine whether removing an '-an' suffix is linguistically valid. The function implements a single condition:

- The value in z->I[0] must not equal 1

If this condition is true, the function returns 1 (OK for suffix removal); if z->I[0] equals 1, it returns 0 (not OK).

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment, specifically accessing the counter/flag array z->I[0]

## Dependencies
- Functions called/Symbols referenced: None (simple validation function)
- Called from: No direct references found in the indexed symbols (may be used indirectly or in conditional expressions)

## Notes and Other Information
- This function is part of PostgreSQL's Indonesian language support for full-text search
- The z->I[0] value likely represents different morphological classes or word types in Indonesian
- Value 1 probably corresponds to a specific word category where '-an' suffix removal would be morphologically incorrect
- The '-an' suffix in Indonesian can form nouns from verbs or indicate various grammatical functions
- This validation ensures that stemming maintains linguistic accuracy by preventing inappropriate suffix removal

## Simplified Source

```c
static int r_SUFFIX_AN_OK(struct SN_env * z) {
    // Check if '-an' suffix removal is morphologically valid
    // Reject if word type is 1 (specific morphological class)
    if (z->I[0] == 1) return 0;

    return 1; // OK to remove -an suffix
}
```