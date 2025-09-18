# brin_minmax_multi_distance_tid

## Location
[src/backend/access/brin/brin_minmax_multi.c:1990-2020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_minmax_multi.c#L1990-L2020)

## Overview
Computes the distance between two tid (tuple identifier) values used as range boundaries in BRIN minmax-multi indexes by mapping them to a linear address space.

## Definition
```c
Datum brin_minmax_multi_distance_tid(PG_FUNCTION_ARGS)
```

## Detailed Description
This function calculates the distance between two tuple identifiers (TIDs) for BRIN (Block Range Index) minmax-multi operator class. TIDs consist of a block number and an offset number within that block. To compute distance, the function maps each TID to a linear address space by converting it to a float8 value using the formula:

`address = block_number * MaxHeapTuplesPerPage + offset_number`

The distance is then computed as the simple difference between these linear addresses. This approach allows BRIN indexes to effectively handle ranges of TID values by providing a meaningful distance metric.

The function uses "NoCheck" variants of ItemPointer accessor functions because user-supplied TID values may have ip_posid == 0, which would cause the regular accessor functions to fail.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function arguments containing:
  - Argument 0: First ItemPointer (TID range minimum)
  - Argument 1: Second ItemPointer (TID range maximum)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_DATUM`: Extracts datum arguments from function call
  - [ItemPointerCompare](../I/ItemPointerCompare.md): Compares two ItemPointers for ordering
  - [ItemPointerGetBlockNumberNoCheck](../I/ItemPointerGetBlockNumberNoCheck.md): Gets block number from ItemPointer without validation
  - [ItemPointerGetOffsetNumberNoCheck](../I/ItemPointerGetOffsetNumberNoCheck.md): Gets offset number from ItemPointer without validation
  - `MaxHeapTuplesPerPage`: Constant defining maximum tuples per heap page
  - `PG_RETURN_FLOAT8`: Returns float8 result from PostgreSQL function
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- The function assumes that pa1 <= pa2 (enforced by Assert using ItemPointerCompare)
- Uses NoCheck variants to handle edge cases where ip_posid == 0 in user-supplied values
- Maps 2D TID space (block, offset) to 1D linear space for distance calculation
- Returns distance as float8 for compatibility with BRIN distance function interface
- Used internally by BRIN minmax-multi operator class for tid data types
- Part of the extensible operator class framework for BRIN indexes
- The distance calculation is essential for determining when TID ranges should be merged or split in multi-range BRIN summaries
- Linear mapping may not reflect actual physical distance on storage media but provides consistent ordering