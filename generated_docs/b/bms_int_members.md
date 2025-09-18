# bms_int_members

## Location
[src/backend/nodes/bitmapset.c:1109-1160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/bitmapset.c#L1109-L1160)

## Overview
Performs intersection of two bitmap sets with optimization for recycling the left input bitmap set when possible, returning the intersection result.

## Definition


## Detailed Description
The bms_int_members function computes the intersection of two bitmap sets, similar to bms_intersect, but with an important optimization: it modifies and reuses the left input bitmap set (a) rather than creating a new bitmap set. This reduces memory allocation overhead when the caller doesn't need to preserve the original left operand.

The function performs the intersection by ANDing corresponding words from both bitmap sets, keeping only bits that are set in both sets. It automatically handles cases where the sets have different sizes by working with the shorter length. The function also optimizes memory usage by removing trailing zero words from the result.

## Parameters / Member Variables
- : The left bitmap set to intersect and potentially recycle (can be NULL)
- : The right bitmap set to intersect with (const, not modified, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_valid_set](bms_is_valid_set.md) (validation of both inputs)
  - [bms_copy_and_free](bms_copy_and_free.md) (conditional memory management)
  - [pfree](../p/pfree.md) (memory deallocation)
  - Min (minimum calculation macro)
  
- Called from (representative examples):
  - [get_common_eclass_indexes](../g/get_common_eclass_indexes.md) (equivalence class processing)
  - [make_outerjoininfo](../m/make_outerjoininfo.md) (outer join information creation)
  - [find_nonnullable_rels_walker](../f/find_nonnullable_rels_walker.md) (nullable relation analysis)
  - [perform_pruning_combine_step](../p/perform_pruning_combine_step.md) (partition pruning logic)
  - get_param_path_clause_serials (parameter path processing)

## Notes and Other Information
- Returns NULL if either input is NULL or if the intersection is empty
- Modifies and potentially frees the left input bitmap set (a)
- More memory-efficient than bms_intersect when left operand can be recycled
- Automatically trims trailing zero words to minimize memory usage
- The right operand (b) is never modified (marked const)
- Uses bitwise AND operation for efficient intersection computation
- Supports conditional reallocation based on REALLOCATE_BITMAPSETS compile flag