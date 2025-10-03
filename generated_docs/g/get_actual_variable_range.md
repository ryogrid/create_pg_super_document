# get_actual_variable_range

## Location
[src/backend/utils/adt/selfuncs.c:6153-6332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L6153-L6332)

## Overview
Attempts to identify the current actual minimum and/or maximum values of a specified variable by searching for a suitable B-tree index and fetching its low and/or high values from the actual table data.

## Definition
```c
static bool get_actual_variable_range(PlannerInfo *root, VariableStatData *vardata,
                                     Oid sortop, Oid collation,
                                     Datum *min, Datum *max)
```

## Detailed Description
This function provides a mechanism to obtain real-time minimum and maximum values for a column by performing actual index scans rather than relying solely on stored statistics. It searches through available B-tree indexes on the relation to find one that matches the specified variable, sort operator, and collation. Once a suitable index is found, it performs index scans in both directions (forward for minimum, backward for maximum) to retrieve the actual extreme values. The function handles various edge cases including partitioned tables, partial indexes, and hypothetical indexes, ensuring it only uses indexes that provide complete coverage of the relation.

## Parameters / Member Variables
- `root`: PlannerInfo containing query planning context and relation information
- `vardata`: VariableStatData structure containing information about the variable being analyzed
- `sortop`: OID of the "<" comparison operator to use for ordering
- `collation`: Required collation for the comparison operations
- `min`: Pointer to store the minimum value found (can be NULL if not needed)
- `max`: Pointer to store the maximum value found (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - [VariableStatData](../V/VariableStatData.md)
  - RTE_RELATION
  - [IndexOptInfo](../I/IndexOptInfo.md)
  - [match_index_to_operand](../m/match_index_to_operand.md)
  - [get_op_opfamily_strategy](get_op_opfamily_strategy.md)
  - AllocSetContextCreate
  - [index_open](../i/index_open.md)
  - [table_slot_create](../t/table_slot_create.md)
  - [get_typlenbyval](get_typlenbyval.md)
  - [ScanKeyEntryInitialize](../S/ScanKeyEntryInitialize.md)
  - get_actual_variable_endpoint
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [index_close](../i/index_close.md)
- Called from (representative examples):
  - [ineq_histogram_selectivity](../i/ineq_histogram_selectivity.md)
  - [get_variable_range](get_variable_range.md)

## Notes and Other Information
This function is particularly useful when stored statistics are outdated or when precise range information is critical for query optimization. It creates a temporary memory context to ensure proper cleanup of resources used during index scanning. The function only considers B-tree indexes since they maintain ordered data, and it skips partial indexes to ensure complete relation coverage. When both minimum and maximum values are requested, it performs two separate index scans in opposite directions. The function respects existing locks and uses NoLock when opening relations, assuming appropriate locks are already held by the calling context.

## Simplified Source

```c
static bool
get_actual_variable_range(PlannerInfo *root, VariableStatData *vardata,
                         Oid sortop, Oid collation,
                         Datum *min, Datum *max)
{
    RelOptInfo *rel = vardata->rel;

    // Check if we have indexes to work with
    if (rel == NULL || rel->indexlist == NIL)
        return false;

    // Skip partitioned tables (no real indexes)
    RangeTblEntry *rte = root->simple_rte_array[rel->relid];
    if (rte->relkind == RELKIND_PARTITIONED_TABLE)
        return false;

    // Search for suitable B-tree index
    foreach(lc, rel->indexlist)
    {
        IndexOptInfo *index = (IndexOptInfo *) lfirst(lc);

        // Must be B-tree, complete coverage, not hypothetical
        if (index->relam != BTREE_AM_OID ||
            index->indpred != NIL ||
            index->hypothetical)
            continue;

        // First column must match our variable and sort operator
        if (collation != index->indexcollations[0] ||
            !match_index_to_operand(vardata->var, 0, index))
            continue;

        // Determine scan direction based on operator strategy
        ScanDirection indexscandir;
        switch (get_op_opfamily_strategy(sortop, index->sortopfamily[0]))
        {
            case BTLessStrategyNumber:
                indexscandir = index->reverse_sort[0] ? BackwardScanDirection : ForwardScanDirection;
                break;
            case BTGreaterStrategyNumber:
                indexscandir = index->reverse_sort[0] ? ForwardScanDirection : BackwardScanDirection;
                break;
            default:
                continue;  // Index doesn't match sort operator
        }

        // Set up for index scanning
        MemoryContext tmpcontext = AllocSetContextCreate(CurrentMemoryContext, "get_actual_variable_range workspace", ...);

        Relation heapRel = table_open(rte->relid, NoLock);
        Relation indexRel = index_open(index->indexoid, NoLock);
        TupleTableSlot *slot = table_slot_create(heapRel, NULL);

        // Set up scan key to ignore NULLs
        ScanKeyData scankeys[1];
        ScanKeyEntryInitialize(&scankeys[0], SK_ISNULL | SK_SEARCHNOTNULL, 1, ...);

        bool have_data = false;

        // Get minimum value if requested
        if (min)
            have_data = get_actual_variable_endpoint(heapRel, indexRel, indexscandir, scankeys, ..., min);

        // Get maximum value if requested and previous scan succeeded
        if (max && have_data)
            have_data = get_actual_variable_endpoint(heapRel, indexRel, -indexscandir, scankeys, ..., max);

        // Cleanup
        ExecDropSingleTupleTableSlot(slot);
        index_close(indexRel, NoLock);
        table_close(heapRel, NoLock);
        MemoryContextDelete(tmpcontext);

        return have_data;
    }

    return false;  // No suitable index found
}
```

This function finds actual min/max values by:
1. **Index Selection**: Finding a suitable B-tree index covering the variable
2. **Direction Setup**: Determining forward/backward scan based on sort operator
3. **Endpoint Scanning**: Performing index scans to get actual current extreme values
4. **Resource Management**: Using temporary memory context for proper cleanup
5. **Validation**: Ensuring indexes provide complete coverage and match requirements