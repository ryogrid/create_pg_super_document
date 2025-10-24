# tuplesort_begin_index_btree

## Location
[src/backend/utils/sort/tuplesortvariants.c:352-436](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L352-L436)

## Overview
Initializes a Tuplesortstate for sorting index tuples during B-tree index creation, with support for uniqueness enforcement and parallel index building operations.

## Definition

```c
Tuplesortstate *
tuplesort_begin_index_btree(Relation heapRel,
							Relation indexRel,
							bool enforceUnique,
							bool uniqueNullsNotDistinct,
							int workMem,
							SortCoordinate coordinate,
							int sortopt)
```
## Detailed Description
This function creates a specialized tuplesort state for B-tree index creation operations. It configures the sorting infrastructure to handle index tuples according to the index's key attributes, with specific support for uniqueness constraints. The function sets up comparison functions optimized for index tuples, prepares sort support data for each index key, and configures uniqueness enforcement when required. It respects the index's ordering properties including collation, null handling, and ascending/descending sort directions.

## Parameters / Member Variables
- `heapRel`: The heap relation being indexed
- `indexRel`: The B-tree index relation being created
- `enforceUnique`: Whether to enforce uniqueness constraints during sorting
- `uniqueNullsNotDistinct`: Whether NULL values should be considered distinct for uniqueness
- `workMem`: Amount of memory (in KB) available for sorting operations
- `coordinate`: Coordination structure for parallel sorting operations
- `sortopt`: Sorting options bitmask (e.g., TUPLESORT_RANDOMACCESS)
## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_begin_common](tuplesort_begin_common.md)
  - TuplesortstateGetPublic
  - IndexRelationGetNumberOfKeyAttributes
  - [_bt_mkscankey](../b/_bt_mkscankey.md)
  - [removeabbrev_index](../r/removeabbrev_index.md)
  - [comparetup_index_btree](../c/comparetup_index_btree.md)
  - [comparetup_index_btree_tiebreak](../c/comparetup_index_btree_tiebreak.md)
  - [writetup_index](../w/writetup_index.md)
  - [readtup_index](../r/readtup_index.md)
  - [PrepareSortSupportFromIndexRel](../P/PrepareSortSupportFromIndexRel.md)
- Called from (representative examples):
  - [_bt_spools_heapscan](../b/_bt_spools_heapscan.md) (nbtsort.c:428, nbtsort.c:469)
  - [_bt_parallel_scan_and_sort](../b/_bt_parallel_scan_and_sort.md) (nbtsort.c:1879, nbtsort.c:1905)

## Notes and Other Information
- Creates a TuplesortIndexBTreeArg structure to store index-specific parameters including heap and index relations
- Supports uniqueness enforcement with configurable handling of NULL values
- Uses index scan key information to configure sort support for proper ordering
- Enables datum1 optimization for improved performance with the first sort key
- Respects index-specific properties like DESC ordering and NULLS FIRST/LAST from scan keys
- Used primarily during CREATE INDEX operations and parallel index building
- The function handles both regular and parallel index creation scenarios through the coordinate parameter

## Simplified Source

```c
Tuplesortstate *
tuplesort_begin_index_btree(Relation heapRel, Relation indexRel,
                           bool enforceUnique, bool uniqueNullsNotDistinct,
                           int workMem, SortCoordinate coordinate, int sortopt)
{
    // Initialize common tuplesort state
    Tuplesortstate *state = tuplesort_begin_common(workMem, coordinate, sortopt);
    TuplesortPublic *base = TuplesortstateGetPublic(state);

    // Switch to sort context and allocate index-specific args
    MemoryContext oldcontext = MemoryContextSwitchTo(base->maincontext);
    TuplesortIndexBTreeArg *arg = palloc(sizeof(TuplesortIndexBTreeArg));

    // Set up basic properties
    base->nKeys = IndexRelationGetNumberOfKeyAttributes(indexRel);

    // Configure index-specific function pointers
    base->removeabbrev = removeabbrev_index;
    base->comparetup = comparetup_index_btree;
    base->comparetup_tiebreak = comparetup_index_btree_tiebreak;
    base->writetup = writetup_index;
    base->readtup = readtup_index;
    base->haveDatum1 = true;
    base->arg = arg;

    // Store index parameters
    arg->index.heapRel = heapRel;
    arg->index.indexRel = indexRel;
    arg->enforceUnique = enforceUnique;
    arg->uniqueNullsNotDistinct = uniqueNullsNotDistinct;

    // Get index scan key for sort configuration
    BTScanInsert indexScanKey = _bt_mkscankey(indexRel, NULL);

    // Configure sort support for each key
    base->sortKeys = palloc0(base->nKeys * sizeof(SortSupportData));
    for (int i = 0; i < base->nKeys; i++)
    {
        SortSupport sortKey = base->sortKeys + i;
        ScanKey scanKey = indexScanKey->scankeys + i;

        // Set basic sort key properties
        sortKey->ssup_cxt = CurrentMemoryContext;
        sortKey->ssup_collation = scanKey->sk_collation;
        sortKey->ssup_nulls_first = (scanKey->sk_flags & SK_BT_NULLS_FIRST) != 0;
        sortKey->ssup_attno = scanKey->sk_attno;
        sortKey->abbreviate = (i == 0 && base->haveDatum1);

        // Determine sort strategy (ASC/DESC)
        int16 strategy = (scanKey->sk_flags & SK_BT_DESC) != 0 ?
            BTGreaterStrategyNumber : BTLessStrategyNumber;

        PrepareSortSupportFromIndexRel(indexRel, strategy, sortKey);
    }

    pfree(indexScanKey);
    MemoryContextSwitchTo(oldcontext);

    return state;
}
```