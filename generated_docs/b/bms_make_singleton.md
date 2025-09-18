# bms_make_singleton

## Location
src/backend/nodes/bitmapset.c: 216 - 238

## Overview
Creates a Bitmapset containing exactly one member at the specified bit position.

## Definition


## Detailed Description
This function constructs a new Bitmapset with a single bit set at position . It allocates memory for the bitmapset structure, initializes it with the appropriate type and size, and sets only the specified bit. The function uses efficient bit manipulation by calculating the appropriate word number and bit position within that word, then sets the corresponding bit using a left-shift operation.

## Parameters / Member Variables
- : The bit position to set in the new bitmapset (must be non-negative)

## Dependencies
- Functions called/Symbols referenced:
  - WORDNUM (macro to calculate word number from bit position)
  - BITNUM (macro to calculate bit position within word)
  - BITMAPSET_SIZE (macro to calculate memory size needed)
  - palloc0 (PostgreSQL memory allocation function)
  - elog (error logging function)
  - bitmapword (type for individual bitmap words)

- Called from (representative examples):
  - bms_add_member
  - build_base_rel_tlists
  - transform_MERGE_to_join
  - get_matching_hash_bounds
  - examine_simple_variable

## Notes and Other Information
- The function validates input by rejecting negative bit positions with an ERROR
- Memory is allocated using palloc0, which zeros the allocated memory
- The resulting Bitmapset has its type field set to T_Bitmapset for node type identification
- The nwords field is set to accommodate the highest word needed for the specified bit position
- This is a foundational function used throughout PostgreSQL's query optimizer and planner