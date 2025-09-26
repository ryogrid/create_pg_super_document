# ExecStoreAllNullTuple

## Location
[src/backend/executor/execTuples.c:1663-1692](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L1663-L1692)

## Overview
Creates a virtual tuple containing NULL values in every column, resulting in a full (non-empty) slot where all attributes are explicitly set to NULL.

## Definition
```c
TupleTableSlot *ExecStoreAllNullTuple(TupleTableSlot *slot)
```

## Detailed Description
ExecStoreAllNullTuple fills a TupleTableSlot with a tuple where every column contains a NULL value. Unlike ExecClearTuple which makes a slot empty, this function creates a valid tuple with explicit NULL values in all positions. The function follows the virtual tuple protocol:

1. Clears any existing slot contents using ExecClearTuple
2. Zeroes out the tts_values array (setting all Datum values to 0)
3. Sets all entries in the tts_isnull array to true (marking all columns as NULL)
4. Calls ExecStoreVirtualTuple to mark the slot as containing valid data

This is particularly useful for outer joins, bitmap heap scans with no matching tuples, and other scenarios where a "conceptual" tuple with all NULL values needs to be represented.

## Parameters / Member Variables
- `slot`: The TupleTableSlot to fill with all NULL values

## Dependencies
- Functions called/Symbols referenced:
  - [ExecClearTuple](ExecClearTuple.md) (clears existing slot contents)
  - MemSet (macro for setting memory to zero)
  - [ExecStoreVirtualTuple](ExecStoreVirtualTuple.md) (marks slot as containing valid virtual tuple)
- Called from (representative examples):
  - [heapam_scan_bitmap_next_tuple](../h/heapam_scan_bitmap_next_tuple.md)
  - [ATRewriteTable](../A/ATRewriteTable.md)
  - [ExecInitNullTupleSlot](ExecInitNullTupleSlot.md)
  - [prepare_projection_slot](../p/prepare_projection_slot.md)
  - [ExecDelete](ExecDelete.md)

## Notes and Other Information
- The resulting slot is considered "full" and valid, not empty, despite containing only NULL values
- Commonly used in outer join operations where no matching tuple exists on one side
- Used in bitmap heap scans when no tuples match the bitmap condition
- Essential for maintaining proper tuple semantics in scenarios requiring placeholder tuples
- The function explicitly zeros the Datum array and sets all isnull flags to true for consistency
- Follows the standard virtual tuple creation protocol, making it compatible with all slot operations
- Different from an empty slot - this represents an actual tuple with NULL values rather than the absence of a tuple
- Useful for table rewriting operations where default NULL values need to be represented explicitly