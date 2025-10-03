# acquire_inherited_sample_rows

## Location
[src/backend/commands/analyze.c:1345-1608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/analyze.c#L1345-L1608)

## Overview
Acquires sample rows from an inheritance tree by collecting samples proportionally from all inheritance children, handling tuple conversion between different table structures as needed.

## Definition

```c
static int
acquire_inherited_sample_rows(Relation onerel, int elevel,
							  HeapTuple *rows, int targrows,
							  double *totalrows, double *totaldeadrows)
```
## Detailed Description
The acquire_inherited_sample_rows function extends the sampling capability to inheritance hierarchies, collecting rows from all tables in an inheritance tree rather than just a single table. It discovers all inheritance children using find_all_inheritors, then samples from each child proportionally to its block count relative to the total blocks across all children.

The function handles several complex scenarios: it validates that analyzable children exist, manages different table types (regular tables, foreign tables, materialized views), and performs tuple conversion when child tables have different column structures than the parent. For foreign tables, it consults the Foreign Data Wrapper (FDW) to determine if analysis is supported.

Sampling is distributed proportionally based on each child's block count, ensuring that larger child tables contribute more samples. When child table schemas differ from the parent, the function converts tuples using column name matching to maintain compatibility.

## Parameters / Member Variables
- `onerel`: The parent relation of the inheritance tree
- `elevel`: Error reporting level for progress messages
- `*rows`: Caller-allocated array to store sampled tuples from all children
- `targrows`: Target total number of rows to sample across all children
- `*totalrows`: Output parameter for estimated total live rows across all children
- `*totaldeadrows`: Output parameter for estimated total dead rows across all children
## Dependencies
- Functions called/Symbols referenced:
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [acquire_sample_rows](acquire_sample_rows.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - [SetRelationHasSubclass](../S/SetRelationHasSubclass.md)
  - [GetFdwRoutineForRelation](../G/GetFdwRoutineForRelation.md)
  - [equalRowTypes](../e/equalRowTypes.md)
  - [convert_tuples_by_name](../c/convert_tuples_by_name.md)
  - [execute_attr_map_tuple](../e/execute_attr_map_tuple.md)
  - [free_conversion_map](../f/free_conversion_map.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [do_analyze_rel](../d/do_analyze_rel.md)

## Notes and Other Information
- Fails if no analyzable child tables exist in the inheritance hierarchy
- Handles foreign tables by consulting their FDW analyze hooks
- Performs automatic tuple conversion when child schemas differ from parent schema
- Distributes sample size proportionally based on relative block counts of children
- Updates relhassubclass catalog flag if no children are found
- Maintains table locks on children to preserve TOAST table references
- Provides detailed progress reporting for multi-child table analysis
- Ignores temp tables from other backends and non-analyzable table types

## Simplified Source

```c
static int acquire_inherited_sample_rows(Relation onerel, int elevel,
                                        HeapTuple *rows, int targrows,
                                        double *totalrows, double *totaldeadrows) {
    List *tableOIDs;
    Relation *rels;
    AcquireSampleRowsFunc *acquirefuncs;
    double *relblocks;
    double totalblocks = 0;
    int numrows = 0, nrels = 0;
    bool has_child = false;

    // Initialize output parameters
    *totalrows = 0;
    *totaldeadrows = 0;

    // Find all inheritance children with AccessShareLock
    tableOIDs = find_all_inheritors(RelationGetRelid(onerel), AccessShareLock, NULL);

    // Check if we have at least one child table
    if (list_length(tableOIDs) < 2) {
        CommandCounterIncrement();
        SetRelationHasSubclass(RelationGetRelid(onerel), false);
        ereport(elevel, (errmsg("skipping analyze - no child tables")));
        return 0;
    }

    // Allocate arrays for relations, functions, and block counts
    rels = palloc(list_length(tableOIDs) * sizeof(Relation));
    acquirefuncs = palloc(list_length(tableOIDs) * sizeof(AcquireSampleRowsFunc));
    relblocks = palloc(list_length(tableOIDs) * sizeof(double));

    // Process each child table to determine which are analyzable
    foreach(lc, tableOIDs) {
        Oid childOID = lfirst_oid(lc);
        Relation childrel = table_open(childOID, NoLock);
        AcquireSampleRowsFunc acquirefunc = NULL;
        BlockNumber relpages = 0;

        // Skip temp tables from other backends
        if (RELATION_IS_OTHER_TEMP(childrel)) {
            table_close(childrel, AccessShareLock);
            continue;
        }

        // Handle different table types
        if (childrel->rd_rel->relkind == RELKIND_RELATION ||
            childrel->rd_rel->relkind == RELKIND_MATVIEW) {
            // Regular table - use standard acquisition function
            acquirefunc = acquire_sample_rows;
            relpages = RelationGetNumberOfBlocks(childrel);
        }
        else if (childrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE) {
            // Foreign table - check if FDW supports analysis
            FdwRoutine *fdwroutine = GetFdwRoutineForRelation(childrel, false);
            if (fdwroutine->AnalyzeForeignTable != NULL) {
                bool ok = fdwroutine->AnalyzeForeignTable(childrel, &acquirefunc, &relpages);
                if (!ok) {
                    table_close(childrel, AccessShareLock);
                    continue;
                }
            }
        }
        else {
            // Skip partitioned tables and other unsupported types
            if (childrel != onerel)
                table_close(childrel, AccessShareLock);
            else
                table_close(childrel, NoLock);
            continue;
        }

        // Add this child to our processing list
        has_child = true;
        rels[nrels] = childrel;
        acquirefuncs[nrels] = acquirefunc;
        relblocks[nrels] = (double) relpages;
        totalblocks += (double) relpages;
        nrels++;
    }

    // Ensure we have at least one analyzable child
    if (!has_child) {
        ereport(elevel, (errmsg("skipping analyze - no analyzable child tables")));
        return 0;
    }

    // Sample rows from each child proportionally to its block count
    pgstat_progress_update_param(PROGRESS_ANALYZE_CHILD_TABLES_TOTAL, nrels);

    for (int i = 0; i < nrels; i++) {
        Relation childrel = rels[i];
        AcquireSampleRowsFunc acquirefunc = acquirefuncs[i];
        double childblocks = relblocks[i];

        // Update progress reporting
        pgstat_progress_update_multi_param(3, progress_index, progress_vals);

        if (childblocks > 0) {
            // Calculate proportional sample size for this child
            int childtargrows = (int) rint(targrows * childblocks / totalblocks);
            childtargrows = Min(childtargrows, targrows - numrows);

            if (childtargrows > 0) {
                double trows, tdrows;

                // Acquire sample from this child
                int childrows = (*acquirefunc)(childrel, elevel,
                                             rows + numrows, childtargrows,
                                             &trows, &tdrows);

                // Convert tuples if child schema differs from parent
                if (childrows > 0 &&
                    !equalRowTypes(RelationGetDescr(childrel), RelationGetDescr(onerel))) {
                    TupleConversionMap *map = convert_tuples_by_name(
                        RelationGetDescr(childrel), RelationGetDescr(onerel));
                    if (map != NULL) {
                        for (int j = 0; j < childrows; j++) {
                            HeapTuple newtup = execute_attr_map_tuple(rows[numrows + j], map);
                            heap_freetuple(rows[numrows + j]);
                            rows[numrows + j] = newtup;
                        }
                        free_conversion_map(map);
                    }
                }

                // Accumulate results
                numrows += childrows;
                *totalrows += trows;
                *totaldeadrows += tdrows;
            }
        }

        table_close(childrel, NoLock);
        pgstat_progress_update_param(PROGRESS_ANALYZE_CHILD_TABLES_DONE, i + 1);
    }

    return numrows;
}
```