# r_tolower

## Location
src/backend/snowball/libstemmer/stem_UTF_8_greek.c: 2330 - 2473

## Overview
A string transformation function that converts Greek uppercase characters to their lowercase equivalents using pattern matching and replacement, as part of the Greek stemming preprocessing pipeline.

## Definition
```c
static int r_tolower(struct SN_env * z)
```

## Detailed Description
This function implements a comprehensive Greek character case conversion system by iteratively processing the string from right to left (backward processing). It uses the Snowball pattern matching system with a predefined array (`a_0` with 46 patterns) to identify Greek uppercase character sequences and replace them with corresponding lowercase equivalents through a switch statement with 25 different cases.

The function operates in a loop, continuously searching for uppercase patterns until no more matches are found. For most cases (1-24), it performs direct character replacement using `slice_from_s()` with predefined replacement strings (`s_0` through `s_23`). Case 25 handles a special scenario where it skips backward by one UTF-8 character using `skip_b_utf8()`.

The backward processing approach (using `find_among_b()`) is characteristic of stemming algorithms, allowing efficient suffix-to-root processing while maintaining proper string boundaries.

## Parameters / Member Variables
- `z`: Pointer to the Snowball environment structure (`struct SN_env *`) containing the string being processed, cursor positions, and working buffers

## Dependencies
- Functions called/Symbols referenced:
  - [skip_b_utf8](../s/skip_b_utf8.md): UTF-8 aware backward character skipping function
  - [find_among_b](../f/find_among_b.md): Backward pattern matching against predefined arrays
  - [slice_from_s](../s/slice_from_s.md): String replacement function using predefined string constants
- Called from (representative examples):
  - [greek_UTF_8_stem](../g/greek_UTF_8_stem.md): Main Greek stemming function where case normalization occurs before suffix removal

## Notes and Other Information
- This function is specifically designed for Greek text processing and contains language-specific character mapping rules
- The 25 different transformation cases handle the full range of Greek uppercase to lowercase conversions
- Case conversion is performed as a preprocessing step before the actual stemming operations begin
- The function uses the Snowball library's efficient pattern matching system for performance
- Returns 1 on successful completion, maintaining consistency with other Snowball functions
- Located in src/backend/snowball/libstemmer/stem_UTF_8_greek.c:2330-2473