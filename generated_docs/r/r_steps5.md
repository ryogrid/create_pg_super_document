# r_steps5

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2647-2675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L2647-L2675)

## Overview
The r_steps5 function is part of the Greek stemming algorithm in PostgreSQL's snowball stemmer library, implementing step 5 of the stemming process with pattern matching and conditional suffix replacement.

## Definition
```c
static int r_steps5(struct SN_env * z)
```

## Detailed Description
This function implements step 5 of the Greek stemming algorithm through a two-phase pattern matching and transformation process:

1. **First Phase**: Sets ket position and searches for patterns from array a_11 (11 patterns). If a match is found, the matched slice is deleted and counter I[0] is reset to 0.

2. **Second Phase**: Performs conditional replacement based on pattern matching:
   - Sets both ket and bra positions to current cursor
   - Searches for patterns from array a_10 (40 patterns)
   - Uses a switch statement based on among_var to determine replacement action:
     - Case 1: Replaces with string s_43 (2 characters)
     - Case 2: Replaces with string s_44 (6 characters)
   - Includes bounds checking to ensure safe operations

The function follows the standard snowball stemmer pattern of backward matching and conditional transformations.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing:
  - `ket`: End position marker for pattern matching
  - `bra`: Start position marker for pattern matching
  - `c`: Current cursor position
  - `lb`: Lower bound for matching
  - `I[0]`: Integer counter array (element 0 reset to 0)
- `among_var`: Local variable storing the result of pattern matching to determine replacement type

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward pattern matching)
  - [slice_del](../s/slice_del.md) (slice deletion)
  - [slice_from_s](../s/slice_from_s.md) (slice replacement with predefined strings)
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md) (main stemming function)

## Notes and Other Information
- This is a static function, only accessible within the Greek stemmer module
- Uses predefined pattern arrays (a_10, a_11) and replacement strings (s_43, s_44)
- Returns 1 on success, 0 on no match, or negative values on error
- The among_var variable determines which of two possible replacements to apply
- Part of the sequential stemming process where each step handles specific morphological patterns
- Bounds checking (z->c > z->lb) prevents buffer underruns during pattern matching

## Simplified Source

```c
static int r_steps5(struct SN_env * z) {
    // Phase 1: Find and delete suffix patterns from array a_11
    z->ket = z->c;
    if (!(find_among_b(z, a_11, 11))) return 0;
    z->bra = z->c;
    slice_del(z);  // Delete matched suffix
    z->I[0] = 0;   // Reset counter

    // Phase 2: Apply conditional replacement from array a_10
    z->ket = z->c;
    z->bra = z->c;
    int pattern_type = find_among_b(z, a_10, 40);
    if (!pattern_type) return 0;
    if (z->c > z->lb) return 0;  // Bounds check

    // Apply appropriate replacement based on pattern type
    switch (pattern_type) {
        case 1: slice_from_s(z, 2, s_43); break;  // Replace with s_43
        case 2: slice_from_s(z, 6, s_44); break;  // Replace with s_44
    }
    return 1;  // Success
}
```