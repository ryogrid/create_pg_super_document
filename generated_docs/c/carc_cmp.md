# carc_cmp

## Location
src/backend/regex/regc_nfa.c: 3612 - 3632

## Overview
A comparison function used by  to order compact NFA arcs first by color, then by destination state number.

## Definition


## Detailed Description
This function implements the comparison logic needed to sort compact arcs ( structures) in a deterministic order. The comparison follows a two-level hierarchy:

1. **Primary sort**: By color value ( field) - arcs with smaller color values come first
2. **Secondary sort**: By destination state number ( field) - for arcs with the same color, those leading to lower-numbered states come first

The function returns the standard comparison values expected by :
- Negative value if the first arc should come before the second
- Positive value if the first arc should come after the second  
- Zero if the arcs are equivalent (which should not occur in a properly constructed NFA)

The comment indicates that returning 0 should be unreachable since duplicate arcs should not exist in the NFA at this point.

## Parameters / Member Variables
- : Pointer to the first compact arc to compare (cast from void*)
- : Pointer to the second compact arc to compare (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  -  (compact arc structure)
- Called from (representative examples):
  -  (at src/backend/regex/regc_nfa.c:3608)

## Notes and Other Information
- Standard qsort comparison function signature with void* parameters
- Provides deterministic ordering crucial for efficient arc lookup during pattern matching
- Two-level sort ensures consistent ordering even when multiple arcs have the same color
- The duplicate arc comment suggests this function should never return 0 in normal operation
- Essential for enabling binary search and other efficient lookup algorithms on sorted arc arrays
- Part of PostgreSQL's regex engine performance optimization infrastructure