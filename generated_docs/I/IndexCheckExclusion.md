# IndexCheckExclusion

## Location
src/backend/catalog/index.c: 3133 - 3288

## Overview
Verifies that a new exclusion constraint is satisfied by scanning the heap relation and checking for conflicts with the newly created exclusion index.

## Definition


## Detailed Description
IndexCheckExclusion performs the validation phase of exclusion constraint creation. After an exclusion index is built normally, this function rescans the heap to ensure no existing tuples violate the exclusion constraint. It validates only tuples that are live according to an up-to-date snapshot, assuming they were correctly indexed even with broken HOT chains. The function holds at least ShareLock on the table to prevent uncommitted updates from other transactions.

The validation process involves:
1. Setting up executor state for expression evaluation and partial-index predicates
2. Scanning all live tuples in the base relation using the latest snapshot
3. For each tuple, checking partial-index predicates if applicable
4. Extracting index column values and computing expressions
5. Verifying no exclusion constraint conflicts exist using check_exclusion_constraint

## Parameters / Member Variables
- : The base table relation being indexed
- : The exclusion index relation to validate
- : Index metadata containing expressions, predicates, and other index information

## Dependencies
- Functions called/Symbols referenced:
  - CreateExecutorState
  - ExecDropSingleTupleTableSlot
  - FreeExecutorState
  - GetPerTupleExprContext
  - ExecPrepareQual
  - table_beginscan_strat
  - table_scan_getnextslot
  - FormIndexDatum
  - check_exclusion_constraint
  - ReindexIsCurrentlyProcessingIndex
  - ResetReindexProcessing
- Called from (representative examples):
  - index_build

## Notes and Other Information
- This function is static and only used internally within the index creation process
- Assumes ShareLock is held on the table to prevent concurrent modifications
- Handles reindexing scenarios by marking the index as no longer being reindexed
- Uses executor state for complex expression evaluation and partial-index predicates
- Memory context is reset after each tuple to prevent memory leaks during long scans
- The function would not work correctly for system catalogs where write locks are released early