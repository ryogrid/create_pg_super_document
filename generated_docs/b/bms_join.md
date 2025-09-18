# bms_join

## Location
src/backend/nodes/bitmapset.c: 1230 - 1305

## Overview
Performs union of two bitmap sets with the flexibility to recycle either input bitmap set, choosing the larger one as the base for maximum efficiency.

## Definition


## Detailed Description
The bms_join function computes the union of two bitmap sets, similar to bms_union, but with a key optimization: it can recycle either input bitmap set rather than always creating a new one. The function intelligently chooses the larger bitmap set as the result container and unions the smaller set into it, minimizing memory operations and reallocation needs.

This approach is more flexible than functions like bms_add_members or bms_int_members that can only recycle the left operand. By choosing the larger set as the base, bms_join reduces the amount of copying needed and provides better memory efficiency for asymmetric unions.

## Parameters / Member Variables
- : The first bitmap set to union (can be NULL, may be recycled)
- : The second bitmap set to union (can be NULL, may be recycled)

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_valid_set (validation of both inputs)
  - bms_copy_and_free (conditional memory management)
  - pfree (memory deallocation of unused input)
  
- Called from (representative examples):
  - add_paths_to_joinrel (join path creation)
  - process_equivalence (equivalence class processing)
  - finalize_primnode (primitive node finalization)
  - build_joinrel_tlist (join relation target list construction)
  - pull_varnos_walker (variable number extraction)

## Notes and Other Information
- Returns the non-NULL input if the other input is NULL
- Returns NULL if both inputs are NULL
- Automatically selects the larger bitmap set as the result container for efficiency
- Frees the smaller input bitmap set after union operation
- Uses bitwise OR operation to perform the union
- More memory-efficient than bms_union when one set is significantly larger
- Supports conditional reallocation based on REALLOCATE_BITMAPSETS compile flag
- Extensively used in PostgreSQL's query optimization, particularly in join processing
- The 'pure paranoia' check ensures the smaller set is only freed if it's different from result