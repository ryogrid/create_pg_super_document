# r_step2

## Location
src/backend/snowball/libstemmer/stem_UTF_8_lithuanian.c: 653 - 675

## Overview
The r_step2 function performs the second step of Lithuanian word stemming by iteratively removing specific suffixes that match predefined patterns.

## Definition
```c
static int r_step2(struct SN_env * z)
```

## Detailed Description
This function implements an iterative suffix removal process for Lithuanian stemming. It operates within the R0 region boundary (z->I[0]) and continuously searches for matching suffixes from a predefined pattern array (a_1 with 62 Lithuanian suffix patterns). When a match is found, the suffix is completely deleted from the word. The process continues until no more matching suffixes are found. The function uses backward pattern matching and maintains proper cursor and boundary management during the iterative deletion process.

## Parameters / Member Variables
- `z`: Pointer to the stemming environment structure (SN_env) containing the Lithuanian word being processed, cursor positions, region boundaries, and transformation state

## Dependencies
- Functions called/Symbols referenced:
  - [find_among_b](../f/find_among_b.md): Backward pattern matching function to identify suffix patterns
  - [slice_del](../s/slice_del.md): Function to delete the matched suffix portion
  - a_1: Array of 62 Lithuanian suffix patterns for matching
- Called from (representative examples):
  - [lithuanian_UTF_8_stem](../l/lithuanian_UTF_8_stem.md): Main Lithuanian stemming function

## Notes and Other Information
- Returns 1 on completion (successful or no matches found), negative values on error
- Uses iterative while loop to handle multiple consecutive suffix removals
- Operates only within the R0 region boundary for morphological correctness
- Temporarily adjusts left boundary (lb) during pattern matching to respect region constraints
- Uses ket/bra cursor positions to mark deletion boundaries
- Located in src/backend/snowball/libstemmer/stem_UTF_8_lithuanian.c:653-675
- Static function scope indicates internal use within the Lithuanian stemmer module
- The iterative nature allows for removal of multiple layered suffixes in a single step