# r_steps4

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2628-2646](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L2628-L2646)

## Overview
The r_steps4 function is part of the Greek stemming algorithm in PostgreSQL's snowball stemmer library, implementing step 4 of the stemming process with specific pattern matching and suffix transformation.

## Definition
```c
static int r_steps4(struct SN_env * z)
```

## Detailed Description
This function implements step 4 of the Greek stemming algorithm. It performs a two-phase pattern matching and transformation process:

1. **First Phase**: Sets ket position and searches for patterns from array a_9 (7 patterns). If found, deletes the matched slice and resets counter I[0] to 0.

2. **Second Phase**: Performs a more complex pattern matching operation:
   - Sets both ket and bra positions to current cursor
   - Applies a sophisticated character filter using bit manipulation to check if the character at position c-1 meets specific criteria
   - The bit mask operation (-2145255424 >> (z->p[z->c - 1] & 0x1f)) & 1) filters characters based on their Unicode properties
   - Searches for patterns from array a_8 (19 patterns)
   - If successful, replaces the matched portion with a predefined string (s_42)

The function uses backward matching throughout and includes bounds checking to ensure safe string operations.

## Parameters / Member Variables
- `z`: Pointer to SN_env structure containing:
  - `ket`: End position marker for pattern matching
  - `bra`: Start position marker for pattern matching
  - `c`: Current cursor position
  - `l`: Length/limit of the string
  - `lb`: Lower bound for matching
  - `p`: Pointer to the string buffer
  - `I[0]`: Integer counter array (element 0 reset to 0)

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md) (backward pattern matching)
  - [slice_del](../s/slice_del.md) (slice deletion)
  - [slice_from_s](../s/slice_from_s.md) (slice replacement with predefined string)
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md) (main stemming function)

## Notes and Other Information
- This is a static function, only accessible within the Greek stemmer module
- Uses predefined pattern arrays (a_8, a_9) and replacement string (s_42)
- Returns 1 on success, 0 on no match, or negative values on error
- The bit manipulation operation is used for efficient Unicode character classification
- Includes bounds checking (z->c - 3 <= z->lb) to prevent buffer underruns
- Part of the sequential stemming process where steps are applied in order

## Simplified Source

```c
static int r_steps4(struct SN_env * z) {
    // Phase 1: Find and delete suffix patterns from array a_9
    z->ket = z->c;
    if (!(find_among_b(z, a_9, 7))) return 0;
    z->bra = z->c;
    slice_del(z);  // Delete matched suffix
    z->I[0] = 0;   // Reset counter

    // Phase 2: Apply replacement patterns from array a_8
    z->ket = z->c;
    z->bra = z->c;

    // Check character properties with bounds checking
    if (z->c - 3 <= z->lb || !char_matches_filter(z->p[z->c - 1]))
        return 0;

    // Find pattern and replace with predefined string
    if (!(find_among_b(z, a_8, 19))) return 0;
    if (z->c > z->lb) return 0;
    slice_from_s(z, 2, s_42);  // Replace with s_42 string

    return 1;  // Success
}
```