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
- : Pointer to the Snowball environment structure containing the text buffer and cursor positions
- : Pointer to a sorted array of  structures containing the patterns to match
- : The number of elements in the pattern array

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