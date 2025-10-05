# _bt_truncate

## Location
[src/backend/access/nbtree/nbtutils.c:4657-4801](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtutils.c#L4657-L4801)

## Overview
The _bt_truncate function creates a truncated pivot index tuple by removing unneeded suffix attributes, optimizing B-tree internal node storage efficiency while maintaining correct ordering semantics.

## Definition
```c
IndexTuple _bt_truncate(Relation rel, IndexTuple lastleft, IndexTuple firstright, BTScanInsert itup_key)
```

## Detailed Description
This function performs suffix truncation on B-tree index tuples to create efficient pivot tuples for internal pages. It removes unnecessary trailing attributes from the firstright tuple while ensuring the result still properly separates values on the left page from values on the right page during page splits.

The function implements several key optimizations:
1. **Suffix truncation**: Removes trailing key attributes that are not needed for proper ordering
2. **Non-key attribute removal**: For INCLUDE indexes, always removes non-key attributes from pivot tuples
3. **Heap TID handling**: When key attributes alone cannot distinguish the split point, includes a heap TID as a tiebreaker

The algorithm determines the minimum number of attributes needed by calling _bt_keep_natts, then creates a truncated tuple. If no key attributes can be truncated, it creates a special pivot tuple that includes a heap TID for disambiguation.

## Parameters / Member Variables
- `rel`: Relation object for the B-tree index being modified
- `lastleft`: Last tuple that will remain on the left page after split
- `firstright`: First tuple that will go to the right page after split
- `itup_key`: Insertion scan key used for tuple comparison

## Dependencies
- Functions called/Symbols referenced:
  - [_bt_keep_natts](_bt_keep_natts.md)
  - [index_truncate_tuple](../i/index_truncate_tuple.md)
  - [BTreeTupleIsPivot](../B/BTreeTupleIsPivot.md)
  - [BTreeTupleIsPosting](../B/BTreeTupleIsPosting.md)
  - [BTreeTupleSetNAtts](../B/BTreeTupleSetNAtts.md)
  - [BTreeTupleGetHeapTID](../B/BTreeTupleGetHeapTID.md)
  - [BTreeTupleGetMaxHeapTID](../B/BTreeTupleGetMaxHeapTID.md)
  - [ItemPointerCopy](../I/ItemPointerCopy.md)
  - [ItemPointerCompare](../I/ItemPointerCompare.md)
  - IndexRelationGetNumberOfKeyAttributes
  - IndexRelationGetNumberOfAttributes
- Called from (representative examples):
  - [_bt_split](_bt_split.md)
  - [_bt_buildadd](_bt_buildadd.md)

## Notes and Other Information
This function is critical for B-tree space efficiency and performance. Truncated pivot tuples reduce internal page size, allowing for higher fanout and better cache utilization. The function must carefully maintain Lehman & Yao invariants for concurrent B-tree operations, ensuring that pivot values serve as proper separators between left and right pages. The heap TID tiebreaker mechanism handles cases where key attributes alone cannot provide sufficient discrimination between split points.

## Simplified Source

```c
IndexTuple
_bt_truncate(Relation rel, IndexTuple lastleft, IndexTuple firstright,
             BTScanInsert itup_key)
{
    TupleDesc itupdesc = RelationGetDescr(rel);
    int16 nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
    int keepnatts;
    IndexTuple pivot;

    // Should only truncate non-pivot tuples from leaf pages
    Assert(!BTreeTupleIsPivot(lastleft) && !BTreeTupleIsPivot(firstright));

    // Determine minimum attributes needed for proper separation
    keepnatts = _bt_keep_natts(rel, lastleft, firstright, itup_key);

    // Create basic truncated tuple
    pivot = index_truncate_tuple(itupdesc, firstright,
                                 Min(keepnatts, nkeyatts));

    // Handle posting list truncation if needed
    if (BTreeTupleIsPosting(pivot)) {
        pivot->t_info &= ~INDEX_SIZE_MASK;
        pivot->t_info |= MAXALIGN(BTreeTupleGetPostingOffset(firstright));
    }

    // If key attributes provide sufficient separation, we're done
    if (keepnatts <= nkeyatts) {
        BTreeTupleSetNAtts(pivot, keepnatts, false);
        return pivot;
    }

    // Need heap TID as tiebreaker - create enlarged pivot tuple
    Size newsize = MAXALIGN(IndexTupleSize(pivot)) + MAXALIGN(sizeof(ItemPointerData));
    IndexTuple tidpivot = palloc0(newsize);
    memcpy(tidpivot, pivot, MAXALIGN(IndexTupleSize(pivot)));
    pfree(pivot);

    // Set up enlarged tuple with heap TID
    tidpivot->t_info &= ~INDEX_SIZE_MASK;
    tidpivot->t_info |= newsize;
    BTreeTupleSetNAtts(tidpivot, nkeyatts, true);

    // Use lastleft's heap TID as tiebreaker value
    ItemPointer pivotheaptid = BTreeTupleGetHeapTID(tidpivot);
    ItemPointerCopy(BTreeTupleGetMaxHeapTID(lastleft), pivotheaptid);

    return tidpivot;
}
```