# r_step5a

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3005-3043](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L3005-L3043)

## Overview
A complex step function in the Greek Snowball stemmer that performs multi-phase suffix transformations with backtracking, handling specific Greek word endings and morphological patterns.

## Definition
```c
static int r_step5a(struct SN_env * z)
```

## Detailed Description
The `r_step5a` function implements a sophisticated four-phase transformation process using backtracking mechanisms:

1. **Phase 1 - Optional Specific Suffix Check**: Uses backtracking (`m1` position marker) to:
   - Check for exact match of "αγαμε" (s_72, 10 characters) at word boundary
   - If found and at word beginning, replace with "αγαμ" (s_73, 8 characters)
   - Restore position regardless of outcome

2. **Phase 2 - Conditional Pattern Matching**: Uses another backtracking marker (`m2`) to:
   - Check for specific character (181/0xB5) at position `c-1` with minimum 9 characters
   - Search for patterns in `a_35` array (5 patterns)
   - Delete matched pattern and reset state (`z->I[0] = 0`)
   - Restore position if no match

3. **Phase 3 - Mandatory Suffix Processing**: 
   - Check for exact match of "αμε" (s_74, 6 characters)
   - Delete matched suffix and reset state (`z->I[0] = 0`)
   - Return 0 if no match found (mandatory step)

4. **Phase 4 - Final Pattern Replacement**:
   - Search for patterns in `a_36` array (12 patterns) at word beginning
   - Replace with "αμ" (s_75, 4 characters)

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure (`struct SN_env`) containing:
  - `z->c`: Current position in the string being processed
  - `z->ket`: End position of the substring being matched
  - `z->bra`: Start position of the substring being matched  
  - `z->lb`: Left boundary of the string
  - `z->l`: Length of the string
  - `z->p`: Pointer to the string data
  - `z->I[0]`: Integer state variable that gets reset to 0 in phases 2 and 3

## Dependencies
- Functions called/Symbols referenced:
  - [eq_s_b](../e/eq_s_b.md): Checks for exact string match backwards
  - [find_among_b](../f/find_among_b.md): Searches backwards for patterns in the given array
  - [slice_del](../s/slice_del.md): Deletes the substring between bra and ket
  - [slice_from_s](../s/slice_from_s.md): Replaces the substring with specified string
  - `s_72`: Greek string "αγαμε" for phase 1 matching
  - `s_73`: Greek string "αγαμ" for phase 1 replacement
  - `s_74`: Greek string "αμε" for phase 3 matching
  - `s_75`: Greek string "αμ" for phase 4 replacement
  - `a_35`: Array of 5 patterns for phase 2 matching
  - `a_36`: Array of 12 patterns for phase 4 matching
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md): Main Greek stemming function at line 3577

## Notes and Other Information
- This is step 5a in the Greek stemming algorithm, featuring the most sophisticated backtracking logic
- Uses multiple position markers (`m1`, `m2`) for independent backtracking in different phases
- Phases 1 and 2 are optional (use goto to skip on failure), while phases 3 and 4 are mandatory
- State variable `z->I[0]` is reset in multiple phases, indicating preparation for subsequent processing
- The character validation (181/0xB5) in phase 2 suggests specific Unicode handling for Greek text
- All replacement strings are related to the Greek root "αγαμ"/"αμ", indicating morphological standardization
- Returns 1 on successful completion of all applicable phases, 0 if mandatory phases fail, or negative on error

## Simplified Source

```c
static int r_step5a(struct SN_env * z) {
    // Phase 1: Optional check for "αγαμε" at word boundary
    int saved_pos1 = z->l - z->c;
    if (eq_s_b(z, 10, s_72) && z->c <= z->lb) {  // Found "αγαμε" at word start
        slice_from_s(z, 8, s_73);  // Replace with "αγαμ"
    }
    z->c = z->l - saved_pos1;  // Restore position regardless

    // Phase 2: Optional pattern matching with character validation
    int saved_pos2 = z->l - z->c;
    z->ket = z->c;
    if (z->c - 9 > z->lb && z->p[z->c - 1] == 181) {  // Check specific character
        if (find_among_b(z, a_35, 5)) {  // Try to find pattern from a_35 (5 patterns)
            z->bra = z->c;
            slice_del(z);  // Remove matched pattern
            z->I[0] = 0;   // Reset state
        }
    }
    z->c = z->l - saved_pos2;  // Restore position

    // Phase 3: Mandatory suffix processing for "αμε"
    z->ket = z->c;
    if (!eq_s_b(z, 6, s_74)) return 0;  // Must find "αμε"
    z->bra = z->c;
    slice_del(z);  // Remove "αμε"
    z->I[0] = 0;   // Reset state

    // Phase 4: Final pattern replacement at word boundary
    z->ket = z->c;
    z->bra = z->c;
    if (!find_among_b(z, a_36, 12)) return 0;  // Find from a_36 (12 patterns)
    if (z->c > z->lb) return 0;  // Must be at word beginning

    // Replace with "αμ"
    slice_from_s(z, 4, s_75);

    return 1;  // Success
}
```