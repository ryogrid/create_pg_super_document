# relptr_store_eval

## Location
[src/include/utils/relptr.h:64-75](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/relptr.h#L64-L75)

## Overview
A helper function that safely calculates the relative offset for storing a pointer value in a relative pointer structure, avoiding double evaluation of the value parameter.

## Definition


## Detailed Description
 is an inline function designed to support PostgreSQL's relative pointer system. It calculates the offset needed to store a pointer relative to a base address. The function is specifically created to avoid the double evaluation problem that could occur if the calculation logic were directly embedded in the  macro.

The function takes two pointer arguments: a base address and a value address, and returns either 0 (for NULL pointers) or the relative offset plus 1. The +1 offset is used because 0 is reserved to represent NULL pointers in the relative pointer system.

The function includes an assertion to ensure that the value pointer is not before the base pointer in memory, which would be invalid for a relative pointer system.

## Parameters / Member Variables
- : Base address from which the relative offset is calculated (typically the start of a memory segment)
- : Pointer value to be stored as a relative offset (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - Assert (macro for debugging assertions)
- Called from (representative examples):
  - relptr_store (macro in src/include/utils/relptr.h:79)

## Notes and Other Information
- This is an inline function to ensure optimal performance when used by the relptr_store macro
- The function is part of PostgreSQL's relative pointer infrastructure, which allows storing pointers that can work across different memory mappings
- Returns 0 for NULL pointers, otherwise returns (val - base + 1) 
- The +1 offset design allows 0 to represent NULL while still enabling storage of pointers at the base address
- Used internally by the relptr_store macro to avoid double evaluation of the 'val' parameter
- The function assumes that val >= base when val is not NULL (enforced by Assert)