# comparetup_index_btree_tiebreak

## Location
[src/backend/utils/sort/tuplesortvariants.c:1466-1587](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1466-L1587)

## Overview
A specialized comparison function for B-tree index sorting that performs tiebreaking comparisons when the primary sort keys are equal, including uniqueness enforcement and ItemPointer-based ordering.

## Definition

```c
static int
comparetup_index_btree_tiebreak(const SortTuple *a, const SortTuple *b,
								Tuplesortstate *state)
```
## Detailed Description
This function serves as a tiebreaker comparison routine for B-tree index tuple sorting. It performs a comprehensive comparison when initial sort keys are equal, handling:

1. **Abbreviated key comparison**: If an abbreviation converter is available, it performs a full comparison on the first key using the abbreviated comparator
2. **Multi-key comparison**: Iterates through all sort keys (starting from key 2) to find differences
3. **Uniqueness enforcement**: When uniqueness is required, detects and reports duplicate key violations (respecting NULL handling rules)
4. **ItemPointer tiebreaking**: Uses heap TID (tuple identifier) as the final comparison criterion to ensure deterministic ordering

The function ensures that B-tree indexes maintain their required physical uniqueness property by treating heap TID as an implicit last key attribute.

## Parameters / Member Variables
- `*a`: First SortTuple to compare containing an IndexTuple
- `*b`: Second SortTuple to compare containing an IndexTuple
- `*state`: Tuplesortstate containing sort configuration and context information
## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [index_getattr](../i/index_getattr.md)
  - [ApplySortAbbrevFullComparator](../A/ApplySortAbbrevFullComparator.md)
  - [ApplySortComparator](../A/ApplySortComparator.md)
  - [index_deform_tuple](../i/index_deform_tuple.md)
  - [BuildIndexValueDescription](../B/BuildIndexValueDescription.md)
  - [ItemPointerGetBlockNumber](../I/ItemPointerGetBlockNumber.md)
  - [ItemPointerGetOffsetNumber](../I/ItemPointerGetOffsetNumber.md)
  - [errtableconstraint](../e/errtableconstraint.md)
- Called from (representative examples):
  - [tuplesort_begin_index_btree](../t/tuplesort_begin_index_btree.md)
  - [tuplesort_begin_index_gist](../t/tuplesort_begin_index_gist.md)
  - [comparetup_index_btree](comparetup_index_btree.md)
  - CLUSTER_SORT

## Notes and Other Information
- The function assumes that primary key comparison has already been performed and found the tuples to be equal
- Uniqueness violations are only reported when enforceUnique is true and appropriate NULL handling rules are met
- The final ItemPointer comparison should never result in equality for valid tuples, hence the Assert(false) at the end
- This function is critical for maintaining B-tree index integrity and ensuring deterministic sort order
- NULL values in keys are tracked to properly handle uniqueness constraints with NULLS NOT DISTINCT semantics

## Simplified Source

```c
static int
comparetup_index_btree_tiebreak(const SortTuple *a, const SortTuple *b,
                                Tuplesortstate *state)
{
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    TuplesortIndexBTreeArg *arg = (TuplesortIndexBTreeArg *) base->arg;
    IndexTuple tuple1 = (IndexTuple) a->tuple;
    IndexTuple tuple2 = (IndexTuple) b->tuple;
    TupleDesc tupDes = RelationGetDescr(arg->index.indexRel);
    bool equal_hasnull = false;
    int32 compare;

    // Handle abbreviated key comparison if enabled
    if (base->sortKeys->abbrev_converter) {
        Datum datum1 = index_getattr(tuple1, 1, tupDes, &isnull1);
        Datum datum2 = index_getattr(tuple2, 1, tupDes, &isnull2);

        compare = ApplySortAbbrevFullComparator(datum1, isnull1, datum2, isnull2,
                                                base->sortKeys);
        if (compare != 0)
            return compare;
    }

    // Track if any equal keys have NULLs
    if (a->isnull1)
        equal_hasnull = true;

    // Compare remaining sort keys (2 through nKeys)
    SortSupport sortKey = base->sortKeys + 1;
    for (int nkey = 2; nkey <= base->nKeys; nkey++, sortKey++) {
        Datum datum1 = index_getattr(tuple1, nkey, tupDes, &isnull1);
        Datum datum2 = index_getattr(tuple2, nkey, tupDes, &isnull2);

        compare = ApplySortComparator(datum1, isnull1, datum2, isnull2, sortKey);
        if (compare != 0)
            return compare;

        if (isnull1)
            equal_hasnull = true;
    }

    // Check for uniqueness violations if required
    if (arg->enforceUnique && !(!arg->uniqueNullsNotDistinct && equal_hasnull)) {
        // Build error message and report unique violation
        Datum values[INDEX_MAX_KEYS];
        bool isnull[INDEX_MAX_KEYS];

        index_deform_tuple(tuple1, tupDes, values, isnull);
        char *key_desc = BuildIndexValueDescription(arg->index.indexRel, values, isnull);

        ereport(ERROR, (errcode(ERRCODE_UNIQUE_VIOLATION),
                       errmsg("could not create unique index \"%s\"",
                             RelationGetRelationName(arg->index.indexRel)),
                       key_desc ? errdetail("Key %s is duplicated.", key_desc) :
                                 errdetail("Duplicate keys exist.")));
    }

    // Final tiebreaker: compare ItemPointers (heap TIDs)
    BlockNumber blk1 = ItemPointerGetBlockNumber(&tuple1->t_tid);
    BlockNumber blk2 = ItemPointerGetBlockNumber(&tuple2->t_tid);
    if (blk1 != blk2)
        return (blk1 < blk2) ? -1 : 1;

    OffsetNumber pos1 = ItemPointerGetOffsetNumber(&tuple1->t_tid);
    OffsetNumber pos2 = ItemPointerGetOffsetNumber(&tuple2->t_tid);
    if (pos1 != pos2)
        return (pos1 < pos2) ? -1 : 1;

    return 0;  // Should never reach here
}
```