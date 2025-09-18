# rfree

## Location
[src/backend/regex/regcomp.c:2447-2482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L2447-L2482)

## Overview
Completely deallocates a compiled regular expression structure, freeing all associated memory including the parse tree, lookaround constraints, search NFA, and color map.

## Definition


## Detailed Description
The rfree function serves as the comprehensive cleanup routine for compiled regular expressions in PostgreSQL's regex engine. It is the internal implementation behind the public regfree() function and ensures complete deallocation of all memory structures associated with a compiled regex.

The function performs systematic cleanup in a carefully ordered sequence:
1. Validates that the regex_t structure is valid using the REMAGIC marker
2. Invalidates the regex structure by clearing the magic number
3. Extracts the internal guts structure containing all compiled data
4. Clears external references in the regex_t structure
5. Systematically frees all components within the guts structure:
   - Color map (cmap) used for character classification optimization
   - Parse tree structure containing the regex's internal representation
   - Lookaround constraints (lacons) for assertions like (?=...) and (?<!...)
   - Search NFA used for optimized pattern matching
6. Finally frees the guts structure itself

The function includes safety checks to handle NULL pointers and invalid structures gracefully, making it safe to call multiple times or on partially initialized regex structures.

## Parameters / Member Variables
- : pointer to the regex_t structure to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - REMAGIC - Magic number constant for validating regex structures
  - [freecm](../f/freecm.md) - Frees color map structures
  - freesubre - Recursively frees sub-regular expression tree structures
  - [freelacons](../f/freelacons.md) - Frees lookaround constraint arrays
  - NULLCNFA - Macro to check if compiled NFA is null
  - [freecnfa](../f/freecnfa.md) - Frees compiled NFA structures  
  - FREE - Basic memory deallocation macro
- Called from (representative examples):
  - [freev](../f/freev.md) - [Variables](../V/Variables.md) structure cleanup function
  - COLORED - Color processing context

## Notes and Other Information
- Safe to call with NULL pointers or invalid regex structures
- Invalidates the regex_t structure by clearing magic numbers to prevent reuse
- Part of the comprehensive memory management system preventing leaks
- Handles partially compiled regex structures gracefully
- Order of cleanup is important to avoid accessing freed memory
- The guts structure contains all the heavyweight compiled regex data
- Essential for proper resource management in long-running database processes