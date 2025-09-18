# r_step5k

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3372-3390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_greek.c#L3372-L3390)

## Overview
A static function within the Greek language stemmer that implements step 5k of the Greek stemming algorithm, performing length-restricted and character-specific suffix processing with dual-phase pattern matching.

## Definition
```c
static int r_step5k(struct SN_env * z)
```

## Detailed Description
The `r_step5k` function is part of the Snowball stemming algorithm implementation for Greek text processing. It employs the most restrictive validation among the step5 functions:

1. **Preliminary Validation**: Before any pattern matching, performs two critical checks:
   - Ensures sufficient string length by verifying `z->c - 7 <= z->lb` (requires at least 7 characters from current position to left boundary)
   - Validates that the character at position `c-1` is specifically character code 181 (a specific Greek character in UTF-8)

2. **Primary Suffix Processing**: Searches for suffixes in array `a_59` (1 entry) and removes them if found, then sets the stemmer state flag `z->I[0]` to 0.

3. **Secondary Pattern Matching and Replacement**: After suffix removal, performs pattern replacement:
   - Searches for patterns in array `a_60` (10 entries)
   - Ensures cursor position does not exceed left boundary (`z->c > z->lb`)
   - Replaces matched pattern with 6-character string `s_103`

This function has the strictest constraints of all step5 functions, requiring specific character validation and minimum string length.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure (`struct SN_env`) containing:
  - `c`: Current cursor position
  - `l`: Length/end position of the string
  - `lb`: Left boundary position
  - `bra`, `ket`: Substring boundaries for operations
  - `I[0]`: Integer state variable used by the stemming algorithm
  - `p`: Pointer to the string being processed

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward search for suffix patterns in arrays
  - [slice_del](../s/slice_del.md): Delete the substring between bra and ket
  - [slice_from_s](../s/slice_from_s.md): Replace substring with a specific string
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md) at src/backend/snowball/libstemmer/stem_UTF_8_greek.c:3637

## Notes and Other Information
- This function is part of the automatically generated Snowball stemmer code for Greek language
- Returns 1 on successful completion, 0 if required patterns/conditions are not met, or negative values on errors
- The function modifies the input string in-place by removing/replacing suffixes
- Uses two suffix lookup arrays (a_59, a_60) with 1 and 10 entries respectively
- Character code 181 represents a specific Greek character marker in the UTF-8 encoding scheme
- Most restrictive step5 function due to both character validation and minimum length requirements
- The length check (`z->c - 7 <= z->lb`) ensures there are at least 7 characters available for processing
- Array a_59 contains only 1 entry, making this a very specific pattern match
- The replacement string s_103 is 6 characters long, longer than most other step5 replacement strings
- This step appears to handle very specific Greek linguistic patterns that require both character and length validation