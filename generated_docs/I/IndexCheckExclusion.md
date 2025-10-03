# IndexCheckExclusion

## Location
[src/backend/catalog/index.c:3133-3288](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L3133-L3288)

## Overview
Verifies that a new exclusion constraint is satisfied by scanning the heap relation and checking for conflicts with the newly created exclusion index.

## Definition

```c
static void
IndexCheckExclusion(Relation heapRelation,
					Relation indexRelation,
					IndexInfo *indexInfo)
```
## Detailed Description
IndexCheckExclusion performs the validation phase of exclusion constraint creation. After an exclusion index is built normally, this function rescans the heap to ensure no existing tuples violate the exclusion constraint. It validates only tuples that are live according to an up-to-date snapshot, assuming they were correctly indexed even with broken HOT chains. The function holds at least ShareLock on the table to prevent uncommitted updates from other transactions.

The validation process involves:
1. Setting up executor state for expression evaluation and partial-index predicates
2. Scanning all live tuples in the base relation using the latest snapshot
3. For each tuple, checking partial-index predicates if applicable
4. Extracting index column values and computing expressions
5. Verifying no exclusion constraint conflicts exist using check_exclusion_constraint

## Parameters / Member Variables
- `heapRelation`: The base table relation being indexed
- `indexRelation`: The exclusion index relation to validate
- `*indexInfo`: Index metadata containing expressions, predicates, and other index information
## Dependencies
- Functions called/Symbols referenced:
  - [CreateExecutorState](../C/CreateExecutorState.md)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
  - GetPerTupleExprContext
  - [ExecPrepareQual](../E/ExecPrepareQual.md)
  - [table_beginscan_strat](../t/table_beginscan_strat.md)
  - [table_scan_getnextslot](../t/table_scan_getnextslot.md)
  - [FormIndexDatum](../F/FormIndexDatum.md)
  - [check_exclusion_constraint](../c/check_exclusion_constraint.md)
  - [ReindexIsCurrentlyProcessingIndex](../R/ReindexIsCurrentlyProcessingIndex.md)
  - [ResetReindexProcessing](../R/ResetReindexProcessing.md)
- Called from (representative examples):
  - [index_build](../i/index_build.md)

## Notes and Other Information
- This function is static and only used internally within the index creation process
- Assumes ShareLock is held on the table to prevent concurrent modifications
- Handles reindexing scenarios by marking the index as no longer being reindexed
- Uses executor state for complex expression evaluation and partial-index predicates
- Memory context is reset after each tuple to prevent memory leaks during long scans
- The function would not work correctly for system catalogs where write locks are released early

## Simplified Source

```c
static void
IndexCheckExclusion(Relation heapRelation,
                   Relation indexRelation,
                   IndexInfo *indexInfo)
{
    TableScanDesc scan;
    Datum values[INDEX_MAX_KEYS];
    bool isnull[INDEX_MAX_KEYS];
    ExprState *predicate;
    TupleTableSlot *slot;
    EState *estate;
    ExprContext *econtext;
    Snapshot snapshot;

    // Handle reindexing case - mark index as no longer being reindexed
    if (ReindexIsCurrentlyProcessingIndex(RelationGetRelid(indexRelation)))
        ResetReindexProcessing();

    // Set up executor state for expression evaluation
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);
    slot = table_slot_create(heapRelation, NULL);
    econtext->ecxt_scantuple = slot;

    // Prepare predicate for partial indexes
    predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

    // Scan all live tuples using latest snapshot
    snapshot = RegisterSnapshot(GetLatestSnapshot());
    scan = table_beginscan_strat(heapRelation, snapshot, 0, NULL, true, true);

    while (table_scan_getnextslot(scan, ForwardScanDirection, slot)) {
        CHECK_FOR_INTERRUPTS();

        // Skip tuples that don't satisfy partial-index predicate
        if (predicate != NULL) {
            if (!ExecQual(predicate, econtext))
                continue;
        }

        // Extract index column values and compute expressions
        FormIndexDatum(indexInfo, slot, estate, values, isnull);

        // Check for exclusion constraint violations
        check_exclusion_constraint(heapRelation, indexRelation, indexInfo,
                                  &(slot->tts_tid), values, isnull,
                                  estate, true);

        // Reset memory context to prevent leaks
        MemoryContextReset(econtext->ecxt_per_tuple_memory);
    }

    // Clean up scan and executor state
    table_endscan(scan);
    UnregisterSnapshot(snapshot);
    ExecDropSingleTupleTableSlot(slot);
    FreeExecutorState(estate);

    // Clear expression states that pointed to the estate
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_PredicateState = NULL;
}
```