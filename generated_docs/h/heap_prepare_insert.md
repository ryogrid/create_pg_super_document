# heap_prepare_insert

## Location
[src/backend/access/heap/heapam.c:2229-2276](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L2229-L2276)

## Overview
heap_prepare_insert prepares a tuple for insertion by setting header fields and performing toasting if necessary, returning the prepared tuple ready for storage.

## Definition
```c
static HeapTuple heap_prepare_insert(Relation relation, HeapTuple tup, TransactionId xid, CommandId cid, int options);
```

## Detailed Description
heap_prepare_insert is a subroutine of heap_insert that handles tuple preparation before actual insertion. It sets the tuple header fields with appropriate transaction and command IDs, clears transaction state masks, and determines whether the tuple needs to be toasted due to size constraints or external references. The function ensures the tuple is properly prepared for storage while handling various edge cases such as parallel worker restrictions and different relation types.

The function performs three main operations: transaction header setup (setting xmin, cmin, xmax, and various info masks), parallel worker validation (preventing unsafe parallel inserts), and conditional toasting (for tuples that exceed size thresholds or contain external references). For non-regular relations like system tables, toasting is bypassed to prevent infinite recursion.

## Parameters / Member Variables
- `relation`: The relation where the tuple will be inserted
- `tup`: The HeapTuple to prepare for insertion (modified in-place for header fields)
- `xid`: Transaction ID to stamp on the tuple
- `cid`: Command ID to stamp on the tuple  
- `options`: Preparation option flags (e.g., HEAP_INSERT_FROZEN)

## Dependencies
- Functions called/Symbols referenced:
  - IsParallelWorker
  - HeapTupleHeaderSetXmin
  - HeapTupleHeaderSetXminFrozen
  - HeapTupleHeaderSetCmin
  - HeapTupleHeaderSetXmax
  - RelationGetRelid
  - HeapTupleHasExternal
  - [heap_toast_insert_or_update](heap_toast_insert_or_update.md)
  - HEAP_XACT_MASK, HEAP2_XACT_MASK, HEAP_XMAX_INVALID (constants)
  - RELKIND_RELATION, RELKIND_MATVIEW (constants)
  - TOAST_TUPLE_THRESHOLD (constant)
- Called from (representative examples):
  - [heap_insert](heap_insert.md)
  - [heap_multi_insert](heap_multi_insert.md)

## Notes and Other Information
- Returns either the original tuple (if no toasting needed) or a new toasted tuple
- Header fields are always set in the original tuple regardless of toasting
- Prevents parallel worker insertions that could generate new CommandIds
- Handles HEAP_INSERT_FROZEN option for frozen tuple insertion
- Skips toasting for non-regular relations to avoid recursion in toast tables
- Sets t_tableOid to establish the tuple's relation membership
- Critical for maintaining transaction visibility and MVCC semantics

## Simplified Source

```c
static HeapTuple
heap_prepare_insert(Relation relation, HeapTuple tup, TransactionId xid,
                    CommandId cid, int options)
{
    // Block parallel worker inserts (they can't generate new CommandIds safely)
    if (IsParallelWorker())
        ereport(ERROR, "cannot insert tuples in a parallel worker");

    // Set up transaction header fields
    tup->t_data->t_infomask &= ~(HEAP_XACT_MASK);
    tup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
    tup->t_data->t_infomask |= HEAP_XMAX_INVALID;

    HeapTupleHeaderSetXmin(tup->t_data, xid);
    if (options & HEAP_INSERT_FROZEN)
        HeapTupleHeaderSetXminFrozen(tup->t_data);

    HeapTupleHeaderSetCmin(tup->t_data, cid);
    HeapTupleHeaderSetXmax(tup->t_data, 0);
    tup->t_tableOid = RelationGetRelid(relation);

    // Handle toasting for oversized tuples or external references
    if (relation->rd_rel->relkind != RELKIND_RELATION &&
        relation->rd_rel->relkind != RELKIND_MATVIEW)
    {
        // Toast tables and system tables: no recursive toasting
        Assert(!HeapTupleHasExternal(tup));
        return tup;
    }
    else if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD)
    {
        // Tuple needs toasting due to size or external references
        return heap_toast_insert_or_update(relation, tup, NULL, options);
    }
    else
    {
        // Tuple is ready as-is
        return tup;
    }
}
```