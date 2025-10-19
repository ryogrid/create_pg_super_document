# r_SUFFIX_I_OK

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c:159-170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_indonesian.c#L159-L170)

## Overview
A static boolean function in the Indonesian stemmer that validates whether the '-i' suffix removal is acceptable based on morphological rules and preceding character constraints.

## Definition
```c
static int r_SUFFIX_I_OK(struct SN_env * z)
```

## Detailed Description
This function serves as a validation rule in the Indonesian stemming process with two key checks:

1. **Morphological validation**: The value in z->I[0] must be less than or equal to 2
2. **Character constraint**: The character immediately before the current position must NOT be 's'

The function first checks the morphological context (z->I[0] <= 2). Then it temporarily moves the cursor back one position to check if the preceding character is 's' (ASCII 115). If it finds 's', the function returns 0 (not OK). Otherwise, it returns 1 (OK for suffix removal).

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing the stemming environment with word buffer, cursor positions, and morphological flags

## Dependencies
- Functions called/Symbols referenced: None (direct buffer manipulation)
- Called from: No direct references found in the indexed symbols (may be used indirectly or in conditional expressions)

## Notes and Other Information
- This function is part of PostgreSQL's Indonesian language support for full-text search
- The z->I[0] constraint (≤ 2) likely represents valid morphological classes for '-i' suffix removal
- The 's' character check prevents inappropriate stemming of words where '-i' follows 's'
- Uses temporary cursor manipulation (m1 = z->l - z->c) to safely check the preceding character
- The '-i' suffix in Indonesian can be a verbal suffix, and its removal must follow specific phonological rules
- The function uses goto/label for efficient backtracking when the character check fails

## Simplified Source

```c
static int r_SUFFIX_I_OK(struct SN_env * z) {
    // Check morphological validity: z->I[0] must be <= 2
    if (z->I[0] > 2) return 0;

    // Save current position
    int saved_pos = z->l - z->c;

    // Check if preceding character is 's' - if so, reject
    if (z->c > z->lb && z->p[z->c - 1] == 's') {
        z->c = z->l - saved_pos; // restore position
        return 0;
    }

    // Restore position and allow -i suffix removal
    z->c = z->l - saved_pos;
    return 1;
}
```