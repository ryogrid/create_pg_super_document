# union_tuples

## Location
[src/backend/access/brin/brin.c:2022-2162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L2022-L2162)

## Overview
Merges two BRIN tuples by adjusting the first deformed tuple to be consistent with the summary values in both tuples, effectively creating a union of their summarized data ranges.

## Definition
static void union_tuples(BrinDesc *bdesc, BrinMemTuple *a, BrinTuple *b)

## Detailed Description
This function performs a sophisticated merge operation between two BRIN tuples, combining their summary information to create a unified summary that encompasses both data ranges. The function modifies the first tuple (a) in-place to contain the union of both summaries.

The merge process handles several important cases:
1. **Empty range optimization**: If either tuple represents an empty range, the function can skip expensive per-key union operations
2. **Null handling**: Properly manages null values and tracks whether nulls are present in the combined summary
3. **Data copying**: When one tuple is empty and the other is not, it efficiently copies data instead of performing union operations
4. **Per-key merging**: For non-empty ranges, calls index-specific union functions for each key to properly merge summary values

The function uses a temporary memory context to avoid memory leaks during the deformation of tuple b and subsequent operations.

## Parameters / Member Variables
- : BRIN descriptor containing index metadata and operator information for each indexed column
- : Target BrinMemTuple (in-memory deformed tuple) that will be modified to contain the union result
- : Source BrinTuple (on-disk format) whose summary values will be merged into tuple a

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate: Creates temporary memory context for safe memory management
  - [brin_deform_tuple](../b/brin_deform_tuple.md): Converts on-disk tuple b to in-memory format for processing
  - [datumCopy](../d/datumCopy.md): Creates copies of datum values with proper memory management
  - [index_getprocinfo](../i/index_getprocinfo.md): Retrieves index-specific union function for each key
  - [FunctionCall3Coll](../F/FunctionCall3Coll.md): Calls the union function with collation support
  - [MemoryContextDelete](../M/MemoryContextDelete.md): Cleans up temporary memory context
  - [BrinDesc](../B/BrinDesc.md), BrinMemTuple, BrinTuple: Core BRIN data structures
  - [BrinValues](../B/BrinValues.md), BrinOpcInfo: Per-column summary and operator information structures

- Called from (representative examples):
  - [summarize_range](../s/summarize_range.md): During BRIN index maintenance and summarization
  - [_brin_parallel_merge](../b/_brin_parallel_merge.md): During parallel BRIN index construction merge phase

## Notes and Other Information
- This is a static function, only accessible within the brin.c file
- The function is optimized for common cases where one or both ranges are empty, avoiding unnecessary computation
- Uses a temporary memory context ("brin union") to ensure proper cleanup and avoid memory fragmentation
- Handles both regular null semantics and special BRIN null handling based on operator class configuration
- The union operation is performed per-key using index access method specific union functions
- Critical for BRIN index maintenance operations like vacuuming and parallel index construction
- The first parameter (a) is modified in-place, making this function destructive to its first argument
- Properly handles type-specific copying using typbyval and typlen information from the type cache

## Simplified Source
```c
static void
union_tuples(BrinDesc *bdesc, BrinMemTuple *a, BrinTuple *b)
{
    int         keyno;
    BrinMemTuple *db;
    MemoryContext cxt;
    MemoryContext oldcxt;

    // Create temporary memory context for safe operations
    cxt = AllocSetContextCreate(CurrentMemoryContext, "brin union",
                               ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(cxt);
    db = brin_deform_tuple(bdesc, b, NULL);
    MemoryContextSwitchTo(oldcxt);

    // Case 1: If "b" is empty, use "a" as result (even if "a" is empty)
    if (db->bt_empty_range)
    {
        MemoryContextDelete(cxt);
        return;
    }

    // Case 2: If "a" is empty but "b" is not, copy "b" into "a"
    if (a->bt_empty_range)
    {
        for (keyno = 0; keyno < bdesc->bd_tupdesc->natts; keyno++)
        {
            BrinValues *col_a = &a->bt_columns[keyno];
            BrinValues *col_b = &db->bt_columns[keyno];
            BrinOpcInfo *opcinfo = bdesc->bd_info[keyno];

            // Copy null flags
            col_a->bv_allnulls = col_b->bv_allnulls;
            col_a->bv_hasnulls = col_b->bv_hasnulls;

            // Copy values if not all nulls
            if (!col_b->bv_allnulls)
            {
                for (int i = 0; i < opcinfo->oi_nstored; i++)
                    col_a->bv_values[i] = datumCopy(col_b->bv_values[i],
                                                   opcinfo->oi_typcache[i]->typbyval,
                                                   opcinfo->oi_typcache[i]->typlen);
            }
        }
        a->bt_empty_range = false;
        MemoryContextDelete(cxt);
        return;
    }

    // Case 3: Both ranges are non-empty, perform per-key union
    for (keyno = 0; keyno < bdesc->bd_tupdesc->natts; keyno++)
    {
        BrinValues *col_a = &a->bt_columns[keyno];
        BrinValues *col_b = &db->bt_columns[keyno];
        BrinOpcInfo *opcinfo = bdesc->bd_info[keyno];

        // Handle null values properly
        if (opcinfo->oi_regular_nulls)
        {
            bool b_has_nulls = (col_b->bv_hasnulls || col_b->bv_allnulls);

            // Update null flags
            if (!col_a->bv_allnulls && b_has_nulls)
                col_a->bv_hasnulls = true;

            if (col_b->bv_allnulls)
                continue;

            // Handle case where "a" has no values but "b" does
            if (col_a->bv_allnulls)
            {
                col_a->bv_allnulls = false;
                col_a->bv_hasnulls = true;
                for (int i = 0; i < opcinfo->oi_nstored; i++)
                    col_a->bv_values[i] = datumCopy(col_b->bv_values[i],
                                                   opcinfo->oi_typcache[i]->typbyval,
                                                   opcinfo->oi_typcache[i]->typlen);
                continue;
            }
        }

        // Call index-specific union function to merge values
        FmgrInfo *unionFn = index_getprocinfo(bdesc->bd_index, keyno + 1,
                                             BRIN_PROCNUM_UNION);
        FunctionCall3Coll(unionFn,
                          bdesc->bd_index->rd_indcollation[keyno],
                          PointerGetDatum(bdesc),
                          PointerGetDatum(col_a),
                          PointerGetDatum(col_b));
    }

    MemoryContextDelete(cxt);
}
```