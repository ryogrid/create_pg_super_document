# commute_restrictinfo

## Location
src/backend/optimizer/util/restrictinfo.c: 359 - 415

## Overview
Creates a new RestrictInfo representing the commuted version of a binary operator clause, swapping left and right operands while preserving optimization metadata and cached information.

## Definition


## Detailed Description
This function produces a commuted version of a RestrictInfo containing a binary operator clause by creating new OpExpr and RestrictInfo structures with swapped operands. It performs efficient flat-copy operations of the original structures and then selectively updates only the fields that need to change for commutation. The function preserves valuable cached optimization data like selectivity estimates and cost information, while properly swapping left/right relation sets and equivalence class information. It's designed specifically for use with derived index qualifications where the commuted form may provide better optimization opportunities.

## Parameters / Member Variables
- : The source RestrictInfo containing a binary operator clause to be commuted
- : The OID of the commutator operator (must be provided by the caller after lookup)

## Dependencies
- Functions called/Symbols referenced:
  - OpExpr (type casting and structure creation)
  - lsecond
  - list_make2
- Called from (representative examples):
  - match_opclause_to_indexcol
  - make_simple_restrictinfo

## Notes and Other Information
- Efficient implementation: Uses flat-copy (memcpy) operations to duplicate structures, then selectively updates only the fields that need modification for commutation
- Shared sub-structure warning: The result shares sub-structure with the original RestrictInfo, which is acceptable for derived index quals but could be problematic if the source is subject to change
- Preserved optimization data: Maintains cached selectivity estimates, cost information, and parent equivalence class information since these should be identical for the commuted clause
- Operator class assumption: Assumes without verification that the commutator operator belongs to the same btree and hash operator classes as the original operator
- Hash join handling: Updates the hashjoinoperator field only if it matched the original operator, otherwise sets it to InvalidOid
- Statistical data swapping: Properly swaps left/right bucket sizes and most common value frequencies to maintain accurate optimization statistics
- Cache invalidation: Clears the scansel_cache as it's not worth updating, and resets hash equality operators to InvalidOid for recalculation
- Serial number preservation: Maintains the same rinfo_serial number to preserve debugging and tracking consistency