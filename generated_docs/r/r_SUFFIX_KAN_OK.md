# r_SUFFIX_KAN_OK

## Location
src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c: 147 - 153

## Overview
A static boolean function in the Indonesian stemmer that validates whether the '-kan' suffix removal is acceptable based on morphological rules.

## Definition
```c
static int r_SUFFIX_KAN_OK(struct SN_env * z)
```

## Detailed Description
This function serves as a validation rule in the Indonesian stemming process. It checks the morphological context stored in z->I[0] to determine whether removing a '-kan' suffix is linguistically valid. The function implements two conditions:

1. The value in z->I[0] must not equal 3
2. The value in z->I[0] must not equal 2

Both conditions must be true for the suffix removal to be considered acceptable. If either condition fails, the function returns 0 (not OK); otherwise, it returns 1 (OK).

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment, specifically accessing the counter/flag array z->I[0]

## Dependencies
- Functions called/Symbols referenced: None (simple validation function)
- Called from: No direct references found in the indexed symbols (may be used indirectly or in conditional expressions)

## Notes and Other Information
- This function is part of PostgreSQL's Indonesian language support for full-text search
- The z->I[0] value likely represents different morphological classes or word types in Indonesian
- Values 2 and 3 probably correspond to specific word categories where '-kan' suffix removal would be inappropriate
- The '-kan' suffix in Indonesian is a verbal suffix that can change meaning significantly, making validation crucial
- This function ensures morphological correctness during the stemming process