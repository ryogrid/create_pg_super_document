# r_step2c

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2922-2938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L2922-L2938)

## Overview
A step function in the Greek Snowball stemmer that performs specific suffix transformations during the second phase of stemming, focusing on particular suffix patterns ending with specific Greek characters.

## Definition

```c
}

static int r_step2c(struct SN_env * z)
```
## Detailed Description
The  function is part of the Greek language stemmer in PostgreSQL's Snowball stemming implementation. It performs a two-phase suffix transformation:

1. **Phase 1**: Checks for specific suffix patterns using the  array (2 patterns) and removes matching suffixes if found
2. **Phase 2**: After the deletion, it searches for patterns from the  array (15 patterns) and replaces them with the Greek suffix "ουδ" (s_67)

The function operates by moving backwards from the current position () and requires at least 9 characters before the left boundary (). It also performs character validation by checking if the character at position  is either 131 (0x83) or 189 (0xBD) in UTF-8 encoding.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure () containing:
  - : Current position in the string being processed
  - : End position of the substring being matched
  - : Start position of the substring being matched  
  - : Left boundary of the string
  - : Pointer to the string data

## Dependencies
- Functions called/Symbols referenced:
  - : Searches backwards for patterns in the given array
  - : Deletes the substring between bra and ket
  - : Replaces the substring with specified string
  - : Array of 2 suffix patterns for initial matching
  - : Array of 15 suffix patterns for replacement phase
  - : Greek string "ουδ" used as replacement suffix
- Called from (representative examples):
  - : Main Greek stemming function at line 3553

## Notes and Other Information
- This is step 2c in the Greek stemming algorithm, part of a sequential series of transformation steps
- The function uses UTF-8 encoded Greek characters and is specifically designed for Greek text processing
- Returns 1 on successful transformation, 0 if no match found, or negative values on error
- The two-phase approach first removes certain suffixes, then applies standardized replacements
- Character validation ensures the function operates on appropriate Greek character sequences

## Simplified Source

```c
static int r_step2c(struct SN_env * z) {
    // Initial validation and suffix removal
    z->ket = z->c;

    // Check bounds (need at least 9 characters) and specific characters
    if (z->c - 9 <= z->lb) return 0;
    char last_char = z->p[z->c - 1];
    if (last_char != 131 && last_char != 189) return 0;  // Check for ƒ or ½

    // Find and delete suffix patterns from array a_28
    if (!(find_among_b(z, a_28, 2))) return 0;
    z->bra = z->c;
    slice_del(z);  // Delete matched suffix

    // Apply replacement with Greek suffix "ουδ"
    z->ket = z->c;
    z->bra = z->c;
    if (!(find_among_b(z, a_29, 15))) return 0;
    slice_from_s(z, 6, s_67);  // Replace with Greek suffix "ουδ"

    return 1;  // Success
}
```