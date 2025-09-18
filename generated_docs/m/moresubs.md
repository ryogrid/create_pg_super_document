# moresubs

## Location
src/backend/regex/regcomp.c: 555 - 591

## Overview
Enlarges the subRE vector (sub-regular expression array) by reallocating memory to accommodate additional sub-regular expressions during regex compilation.

## Definition


## Detailed Description
The  function dynamically expands the subRE (sub-regular expression) vector when more storage space is needed during regular expression compilation. It implements a growth strategy that allocates 1.5x the requested size plus one to minimize future reallocations. The function handles two scenarios: initial allocation from a small static array () and subsequent reallocations of an already-allocated dynamic array.

The function first checks if the current  array is pointing to the static  array. If so, it allocates new memory and copies the existing entries. Otherwise, it uses  to expand the existing allocation. After successful allocation, it initializes the new entries to NULL and updates the  counter.

## Parameters / Member Variables
- : Pointer to the vars structure containing regex compilation state, including the current subRE array and count
- : The minimum number of subRE entries needed (must be greater than current )

## Dependencies
- Functions called/Symbols referenced:
  -  - Memory allocation macro
  -  - Memory reallocation macro  
  -  - Memory copying function via VS macro
  -  - Error reporting macro
  -  - Out of memory error constant
  -  - Sub-regular expression structure type
  -  - Void pointer casting macro
- Called from (representative examples):
  -  (src/backend/regex/regcomp.c:1017)

## Notes and Other Information
- Uses a growth factor of 1.5x plus one to balance memory usage and reallocation frequency
- Properly handles transition from static to dynamic allocation
- Initializes new entries to NULL for safety
- Sets REG_ESPACE error and returns on allocation failure
- Includes assertions to verify the wanted parameter and final state consistency