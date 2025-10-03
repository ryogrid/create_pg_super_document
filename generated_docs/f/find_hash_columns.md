# find_hash_columns

## Location
[src/backend/executor/nodeAgg.c:1563-1693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeAgg.c#L1563-L1693)

## Overview
Computes and configures which columns need to be stored in hash table entries for aggregation, optimizing storage by eliminating unnecessary columns while preserving all required data for grouping and aggregation operations.

## Definition

```c
union(base_colnos, aggregated_colnos);
```
## Detailed Description
The  function performs crucial optimization for hashed aggregation by determining the minimal set of columns that must be stored in hash table entries. Input tuples from child plan nodes typically contain grouping columns, columns referenced in target lists and qualifications, columns needed for aggregate function computation, and potentially unused columns. This function identifies and retains only the first two types, significantly reducing hash table entry size and improving performance.

The function operates in several phases: First, it calls  to identify which columns are referenced in target lists and qualifications, categorizing them as aggregated or unaggregated. Then, for each grouping set requiring hashing, it builds a comprehensive column mapping that includes both directly-hashed grouping columns and additional needed columns like functionally dependent columns and row-mark ctids.

Special handling is provided for grouping sets scenarios where certain variables might be referenced in target lists for other grouping sets but not needed for the current hash table. The function uses prepare_projection_slot logic to determine which columns can be safely omitted. Column mappings are built to translate between input tuple positions and hash table positions, ensuring efficient access during hash value computation and tuple comparison operations.

## Parameters / Member Variables
- : The AggState execution node containing aggregation configuration, grouping set information, and hash table data structures

## Dependencies
- Functions called/Symbols referenced:
  - [find_cols](find_cols.md)
  - outerPlanState
  - [bms_union](../b/bms_union.md)
  - [bms_copy](../b/bms_copy.md)
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_del_member](../b/bms_del_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_next_member](../b/bms_next_member.md)
  - [bms_num_members](../b/bms_num_members.md)
  - [bms_free](../b/bms_free.md)
  - [list_nth](../l/list_nth.md)
  - [list_free](../l/list_free.md)
  - lfirst_int
  - [ExecTypeFromTL](../E/ExecTypeFromTL.md)
  - [execTuplesHashPrepare](../e/execTuplesHashPrepare.md)
  - [ExecAllocTableSlot](../E/ExecAllocTableSlot.md)
- Types referenced:
  - [AggState](../A/AggState.md)
  - [AggStatePerHash](../A/AggStatePerHash.md)
  - [Bitmapset](../B/Bitmapset.md)
  - [TupleDesc](../T/TupleDesc.md)
  - AttrNumber
  - [EState](../E/EState.md)
- Called from (representative examples):
  - [ExecInitAgg](../E/ExecInitAgg.md)

## Notes and Other Information
- Critical for hash aggregation performance as it minimizes hash table entry size by eliminating unused columns
- Handles complex grouping sets scenarios by determining column requirements per grouping set
- Builds column mapping arrays (hashGrpColIdxInput and hashGrpColIdxHash) for efficient translation between input and hash table column positions
- Places grouping columns first in hash table layout for optimal access during hash computation and tuple comparison
- Uses ExecTypeFromTL to create appropriate tuple descriptors for hash table entries
- Supports edge cases like duplicate columns in grpColIdx arrays that can occur with semijoins and DISTINCT operations
- Memory allocation is done in per-query context since the structures persist across ExecReScanAgg calls
- Sets up hash and equality functions through execTuplesHashPrepare for proper tuple comparison
- Creates optimized tuple slots using TTSOpsMinimalTuple for efficient hash table storage

## Simplified Source

```c
static void find_hash_columns(AggState *aggstate) {
    Bitmapset *base_colnos;
    Bitmapset *aggregated_colnos;
    TupleDesc scanDesc = aggstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    List *outerTlist = outerPlanState(aggstate)->plan->targetlist;
    int numHashes = aggstate->num_hashes;
    EState *estate = aggstate->ss.ps.state;

    // Find columns needed in target list and quals
    find_cols(aggstate, &aggregated_colnos, &base_colnos);
    aggstate->colnos_needed = bms_union(base_colnos, aggregated_colnos);
    aggstate->max_colno_needed = 0;
    aggstate->all_cols_needed = true;

    // Determine maximum column number needed and if all columns are required
    for (int i = 0; i < scanDesc->natts; i++) {
        int colno = i + 1;
        if (bms_is_member(colno, aggstate->colnos_needed)) {
            aggstate->max_colno_needed = colno;
        } else {
            aggstate->all_cols_needed = false;
        }
    }

    // Process each hash grouping set
    for (int j = 0; j < numHashes; ++j) {
        AggStatePerHash perhash = &aggstate->perhash[j];
        Bitmapset *colnos = bms_copy(base_colnos);
        AttrNumber *grpColIdx = perhash->aggnode->grpColIdx;
        List *hashTlist = NIL;
        TupleDesc hashDesc;

        perhash->largestGrpColIdx = 0;

        // Handle grouping sets - remove columns not needed for this set
        if (aggstate->phases[0].grouped_cols) {
            Bitmapset *grouped_cols = aggstate->phases[0].grouped_cols[j];
            ListCell *lc;
            foreach(lc, aggstate->all_grouped_cols) {
                int attnum = lfirst_int(lc);
                if (!bms_is_member(attnum, grouped_cols)) {
                    colnos = bms_del_member(colnos, attnum);
                }
            }
        }

        // Calculate maximum columns including potential duplicates
        int maxCols = bms_num_members(colnos) + perhash->numCols;

        // Allocate column mapping arrays
        perhash->hashGrpColIdxInput = palloc(maxCols * sizeof(AttrNumber));
        perhash->hashGrpColIdxHash = palloc(perhash->numCols * sizeof(AttrNumber));

        // Add all grouping columns to the set
        for (int i = 0; i < perhash->numCols; i++) {
            colnos = bms_add_member(colnos, grpColIdx[i]);
        }

        // Build mapping for directly hashed columns (grouping columns first)
        perhash->numhashGrpCols = 0;
        for (int i = 0; i < perhash->numCols; i++) {
            perhash->hashGrpColIdxInput[i] = grpColIdx[i];
            perhash->hashGrpColIdxHash[i] = i + 1;
            perhash->numhashGrpCols++;
            // Remove already mapped columns
            colnos = bms_del_member(colnos, grpColIdx[i]);
        }

        // Add remaining needed columns
        int column_id = -1;
        while ((column_id = bms_next_member(colnos, column_id)) >= 0) {
            perhash->hashGrpColIdxInput[perhash->numhashGrpCols] = column_id;
            perhash->numhashGrpCols++;
        }

        // Build tuple descriptor for hash table
        for (int i = 0; i < perhash->numhashGrpCols; i++) {
            int varNumber = perhash->hashGrpColIdxInput[i] - 1;
            hashTlist = lappend(hashTlist, list_nth(outerTlist, varNumber));
            perhash->largestGrpColIdx = Max(varNumber + 1, perhash->largestGrpColIdx);
        }

        // Create hash table tuple descriptor and prepare hash functions
        hashDesc = ExecTypeFromTL(hashTlist);
        execTuplesHashPrepare(perhash->numCols,
                             perhash->aggnode->grpOperators,
                             &perhash->eqfuncoids,
                             &perhash->hashfunctions);

        // Allocate hash table slot
        perhash->hashslot = ExecAllocTableSlot(&estate->es_tupleTable, hashDesc,
                                              &TTSOpsMinimalTuple);

        // Cleanup temporary structures
        list_free(hashTlist);
        bms_free(colnos);
    }

    bms_free(base_colnos);
}
```