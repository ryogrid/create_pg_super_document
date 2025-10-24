# find_among

## Location
[src/backend/snowball/libstemmer/utilities.c:233-297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L233-L297)

## Overview
A sophisticated pattern matching function that performs binary search through a sorted array of string patterns, with support for substring matching and callback functions.

## Definition

```c
}

extern int find_among(struct SN_env * z, const struct among * v, int v_size)
```
## Detailed Description
The  function is a core utility in the Snowball stemming framework that performs efficient pattern matching against a sorted array of candidate strings. It uses a binary search algorithm to locate matching patterns in the text starting from the current cursor position.

The function implements a sophisticated matching strategy that handles substring relationships between patterns. When a match is found, it can optionally execute a callback function associated with that pattern. The algorithm optimizes performance by tracking common prefixes during the binary search process, avoiding redundant character comparisons.

Key features include:
- Binary search through sorted pattern arrays for O(log n) performance
- Support for substring patterns through the  field
- Optional callback function execution for complex matching rules  
- Forward cursor advancement on successful matches
- Efficient handling of overlapping or nested pattern relationships

The function returns the  field of the matched pattern, or 0 if no match is found.

## Parameters / Member Variables  
- `*z`: Pointer to the Snowball environment structure containing the text buffer and cursor positions
- `*v`: Pointer to a sorted array of  structures containing the patterns to match
- `v_size`: The number of elements in the pattern array
## Dependencies
- Functions called/Symbols referenced:
  -  (structure type for pattern definitions)
  -  (Snowball character type)
- Called from (representative examples):
  - Various language-specific stemming functions across multiple stemmers (Arabic, Catalan, Dutch, English, French, German, Hungarian, Indonesian, Irish, Italian, Portuguese, Romanian, Serbian, Spanish, Tamil, Yiddish)
  - Pattern matching operations in prelude, postlude, and morphological analysis functions
  -  macro in header.h

## Notes and Other Information  
- Critical performance component used extensively throughout all Snowball language stemmers
- The  array must be pre-sorted for the binary search to work correctly
- Supports complex pattern hierarchies through the  mechanism
- Callback functions enable context-sensitive matching rules
- Advances cursor position only on successful matches with proper pattern length
- Part of the forward-matching family of functions in Snowball utilities
- The algorithm handles edge cases like single-element arrays and boundary conditions efficiently

## Simplified Source

```c
extern int find_among(struct SN_env * z, const struct among * v, int v_size) {
    int i = 0, j = v_size;
    int c = z->c, l = z->l;
    const symbol * q = z->p + c;

    int common_i = 0, common_j = 0;
    int first_key_inspected = 0;

    // Binary search for matching pattern
    while (1) {
        int k = i + ((j - i) >> 1);
        int diff = 0;
        int common = common_i < common_j ? common_i : common_j;
        const struct among * w = v + k;

        // Compare characters with current pattern
        for (int i2 = common; i2 < w->s_size; i2++) {
            if (c + common == l) { diff = -1; break; }
            diff = q[common] - w->s[i2];
            if (diff != 0) break;
            common++;
        }

        // Adjust search bounds based on comparison
        if (diff < 0) {
            j = k;
            common_j = common;
        } else {
            i = k;
            common_i = common;
        }

        // Check if search should continue
        if (j - i <= 1) {
            if (i > 0) break;
            if (j == i) break;
            if (first_key_inspected) break;
            first_key_inspected = 1;
        }
    }

    // Process matched pattern and handle substrings
    while (1) {
        const struct among * w = v + i;
        if (common_i >= w->s_size) {
            z->c = c + w->s_size;

            // Execute callback function if present
            if (w->function == 0) return w->result;

            int res = w->function(z);
            z->c = c + w->s_size;
            if (res) return w->result;
        }

        // Handle substring relationships
        i = w->substring_i;
        if (i < 0) return 0;
    }
}
```