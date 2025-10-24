# find_among_b

## Location
[src/backend/snowball/libstemmer/utilities.c:298-354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/utilities.c#L298-L354)

## Overview
The backward-processing counterpart to  that performs binary search through a sorted array of string patterns, matching from the current cursor position backward.

## Definition

```c
}

/* find_among_b is for backwards processing. Same comments apply */

extern int find_among_b(struct SN_env * z, const struct among * v, int v_size)
```
## Detailed Description
The  function is the backward-processing version of  in the Snowball stemming framework. It performs efficient pattern matching against a sorted array of candidate strings, but searches backward from the current cursor position instead of forward.

Like its forward counterpart, the function implements a binary search algorithm for O(log n) performance, but adapts the matching logic to work in reverse. The algorithm compares characters starting from the end of each pattern and works backward, making it ideal for suffix matching operations.

Key features include:
- Binary search through sorted pattern arrays for efficient backward matching
- Reverse character-by-character comparison from pattern end to beginning  
- Support for substring patterns through the  field
- Optional callback function execution for complex matching rules
- Backward cursor movement on successful matches (cursor moves toward left boundary)
- Efficient handling of overlapping or nested suffix relationships

The function moves the cursor backward by the length of the matched pattern and returns the  field of the matched pattern, or 0 if no match is found.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the text buffer and cursor positions
- `*v`: Pointer to a sorted array of  structures containing the patterns to match
- `v_size`: The number of elements in the pattern array
## Dependencies
- Functions called/Symbols referenced:
  -  (structure type for pattern definitions)
  -  (Snowball character type)
- Called from (representative examples):
  - Currently not directly referenced in the indexed codebase, but likely used through macro expansions or dynamic function calls in stemming algorithms

## Notes and Other Information
- This function is specifically designed for suffix removal and backward pattern matching operations
- The  array must be pre-sorted for the binary search to work correctly  
- Supports complex suffix hierarchies through the  mechanism
- Callback functions enable context-sensitive suffix matching rules
- Moves cursor position backward only on successful matches
- Part of the backward-matching family of functions in Snowball utilities
- Shares the same algorithmic complexity and optimization strategies as  but adapted for reverse processing
- Essential for languages with complex suffix morphology where multiple suffix patterns may overlap

## Simplified Source

```c
extern int find_among_b(struct SN_env * z, const struct among * v, int v_size) {
    int i = 0, j = v_size;
    int c = z->c, lb = z->lb;
    const symbol * q = z->p + c - 1;

    int common_i = 0, common_j = 0;
    int first_key_inspected = 0;

    // Binary search for matching pattern (backward)
    while (1) {
        int k = i + ((j - i) >> 1);
        int diff = 0;
        int common = common_i < common_j ? common_i : common_j;
        const struct among * w = v + k;

        // Compare characters backward from pattern end
        for (int i2 = w->s_size - 1 - common; i2 >= 0; i2--) {
            if (c - common == lb) { diff = -1; break; }
            diff = q[-common] - w->s[i2];
            if (diff != 0) break;
            common++;
        }

        // Adjust search bounds
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
            z->c = c - w->s_size;

            // Execute callback function if present
            if (w->function == 0) return w->result;

            int res = w->function(z);
            z->c = c - w->s_size;
            if (res) return w->result;
        }

        // Handle substring relationships
        i = w->substring_i;
        if (i < 0) return 0;
    }
}
```