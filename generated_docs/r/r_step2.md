# r_step2

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_lithuanian.c:653-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_lithuanian.c#L653-L675)

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

## Simplified Source

```c
static int r_step2(struct SN_env * z) {
    while(1) {
        // Save current position for backtracking
        int saved_pos = z->l - z->c;

        // Check if we're within the R0 region boundary
        if (z->c < z->I[0]) break;

        // Set region boundary and find suffix match
        int saved_lb = z->lb;
        z->lb = z->I[0];
        z->ket = z->c;

        // Look for Lithuanian suffix patterns (62 patterns in a_1 array)
        if (!find_among_b(z, a_1, 62)) {
            z->lb = saved_lb;
            z->c = z->l - saved_pos;
            break;
        }

        // Mark deletion boundaries and restore boundary
        z->bra = z->c;
        z->lb = saved_lb;

        // Delete the matched suffix
        if (slice_del(z) < 0) return -1;

        // Continue to find more suffixes
    }
    return 1;
}
```