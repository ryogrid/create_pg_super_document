# comparetup_cluster_tiebreak

## Location
[src/backend/utils/sort/tuplesortvariants.c:1248-1354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesortvariants.c#L1248-L1354)

## Overview
Performs comprehensive multi-column comparison of cluster tuples, handling both simple attribute-based indexes and complex expression-based indexes for complete tuple ordering.

## Definition
```c
static int comparetup_cluster_tiebreak(const SortTuple *a, const SortTuple *b, Tuplesortstate *state)
```

## Detailed Description
The `comparetup_cluster_tiebreak` function implements the comprehensive comparison logic for CLUSTER operations when either the leading sort key comparison results in equality or when no cached datum is available. It handles two distinct scenarios:

1. **Simple Attribute Indexes**: For regular indexes based on table columns, it iterates through all index attributes, extracting values using `heap_getattr` and comparing them using appropriate sort comparators.

2. **Expression Indexes**: For indexes based on expressions or functions, it computes the complete index tuple values using `FormIndexDatum` and then compares the computed values. This requires setting up an expression context and may involve complex expression evaluation.

The function also handles abbreviation comparators for the leading sort key when available, providing optimized comparison for abbreviated keys while falling back to full comparison when necessary.

## Parameters / Member Variables
- `a`: First SortTuple to compare
- `b`: Second SortTuple to compare
- `state`: Tuplesortstate containing sorting context and configuration

## Dependencies
- Functions called/Symbols referenced:
  - TuplesortstateGetPublic
  - [heap_getattr](../h/heap_getattr.md)
  - [ApplySortAbbrevFullComparator](../A/ApplySortAbbrevFullComparator.md)
  - [ApplySortComparator](../A/ApplySortComparator.md)
  - ResetPerTupleExprContext
  - GetPerTupleExprContext
  - [ExecStoreHeapTuple](../E/ExecStoreHeapTuple.md)
  - [FormIndexDatum](../F/FormIndexDatum.md)
  - INDEX_MAX_KEYS
  - TuplesortClusterArg
- Called from (representative examples):
  - [comparetup_cluster](comparetup_cluster.md)
  - [tuplesort_begin_cluster](../t/tuplesort_begin_cluster.md)
  - CLUSTER_SORT operations

## Notes and Other Information
- Returns negative, zero, or positive integer indicating the relative ordering of the tuples
- Handles both simple column-based indexes and complex expression-based indexes
- For expression indexes, memory context is reset between comparisons to prevent memory leaks
- The function optimizes by starting comparison from the second key when the first key has already been compared
- Uses INDEX_MAX_KEYS arrays to store computed index values for expression-based comparisons
- Part of PostgreSQL's CLUSTER implementation that physically reorganizes table data according to index ordering
- The tiebreak mechanism ensures stable and complete ordering even for complex multi-column indexes with expressions

## Simplified Source

```c
static int comparetup_cluster_tiebreak(const SortTuple *a, const SortTuple *b, Tuplesortstate *state)
{
    TuplesortPublic *base = TuplesortstateGetPublic(state);
    TuplesortClusterArg *arg = (TuplesortClusterArg *) base->arg;
    SortSupport sortKey = base->sortKeys;
    HeapTuple ltup = (HeapTuple) a->tuple;
    HeapTuple rtup = (HeapTuple) b->tuple;
    int nkey = 0;
    int32 compare = 0;

    // Handle first column with abbreviation if available
    if (base->haveDatum1)
    {
        if (sortKey->abbrev_converter)
        {
            AttrNumber leading = arg->indexInfo->ii_IndexAttrNumbers[0];
            Datum datum1, datum2;
            bool isnull1, isnull2;

            datum1 = heap_getattr(ltup, leading, arg->tupDesc, &isnull1);
            datum2 = heap_getattr(rtup, leading, arg->tupDesc, &isnull2);

            compare = ApplySortAbbrevFullComparator(datum1, isnull1, datum2, isnull2, sortKey);
        }
        if (compare != 0 || base->nKeys == 1)
            return compare;

        sortKey++;
        nkey = 1;
    }

    if (arg->indexInfo->ii_Expressions == NULL)
    {
        // Simple attribute comparison
        for (; nkey < base->nKeys; nkey++, sortKey++)
        {
            AttrNumber attno = arg->indexInfo->ii_IndexAttrNumbers[nkey];
            Datum datum1, datum2;
            bool isnull1, isnull2;

            datum1 = heap_getattr(ltup, attno, arg->tupDesc, &isnull1);
            datum2 = heap_getattr(rtup, attno, arg->tupDesc, &isnull2);

            compare = ApplySortComparator(datum1, isnull1, datum2, isnull2, sortKey);
            if (compare != 0)
                return compare;
        }
    }
    else
    {
        // Expression index comparison - compute index values
        Datum l_index_values[INDEX_MAX_KEYS];
        bool l_index_isnull[INDEX_MAX_KEYS];
        Datum r_index_values[INDEX_MAX_KEYS];
        bool r_index_isnull[INDEX_MAX_KEYS];

        ResetPerTupleExprContext(arg->estate);
        TupleTableSlot *ecxt_scantuple = GetPerTupleExprContext(arg->estate)->ecxt_scantuple;

        // Compute left tuple index values
        ExecStoreHeapTuple(ltup, ecxt_scantuple, false);
        FormIndexDatum(arg->indexInfo, ecxt_scantuple, arg->estate, l_index_values, l_index_isnull);

        // Compute right tuple index values
        ExecStoreHeapTuple(rtup, ecxt_scantuple, false);
        FormIndexDatum(arg->indexInfo, ecxt_scantuple, arg->estate, r_index_values, r_index_isnull);

        // Compare computed values
        for (; nkey < base->nKeys; nkey++, sortKey++)
        {
            compare = ApplySortComparator(l_index_values[nkey], l_index_isnull[nkey],
                                         r_index_values[nkey], r_index_isnull[nkey],
                                         sortKey);
            if (compare != 0)
                return compare;
        }
    }

    return 0;
}
```