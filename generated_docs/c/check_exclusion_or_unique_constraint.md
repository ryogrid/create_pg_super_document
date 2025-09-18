# check_exclusion_or_unique_constraint

## Location
[src/backend/executor/execIndexing.c:689-914](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execIndexing.c#L689-L914)

## Overview
Performs comprehensive checking for violations of unique or exclusion constraints by scanning index entries and handling concurrent transaction interactions, with support for different waiting behaviors and conflict resolution modes.

## Definition
```c
static bool check_exclusion_or_unique_constraint(Relation heap, 
                                               Relation index,
                                               IndexInfo *indexInfo,
                                               ItemPointer tupleid,
                                               const Datum *values, 
                                               const bool *isnull,
                                               EState *estate, 
                                               bool newIndex,
                                               CEOUC_WAIT_MODE waitMode,
                                               bool violationOK,
                                               ItemPointer conflictTid)
```

## Detailed Description
This static function is the core implementation for checking both unique and exclusion constraint violations in PostgreSQL. It performs a thorough scan of the index to detect conflicts with existing tuples, handling complex scenarios involving concurrent transactions, speculative insertions, and different constraint types.

The function operates through several key phases:
1. **Setup Phase**: Determines constraint operators and strategies based on whether this is a unique or exclusion constraint, and handles NULL value semantics
2. **Scan Preparation**: Initializes an index scan with appropriate scan keys based on the constraint type and input values
3. **Conflict Detection Loop**: Scans through potentially conflicting index entries, extracting values and comparing them against the new tuple
4. **Transaction Handling**: Deals with concurrent transactions by waiting for or detecting speculative insertions and other in-progress operations
5. **Conflict Resolution**: Returns conflict information to caller or raises appropriate constraint violation errors

Key features:
- Supports both unique constraints (equality-based) and exclusion constraints (operator-based)
- Handles NULL semantics correctly based on nulls-distinct vs. nulls-not-distinct settings
- Provides sophisticated concurrency control with multiple wait modes
- Supports speculative insertion detection and livelock prevention
- Can operate in violation-reporting mode or violation-detection mode
- Handles lossy index scans with proper rechecking

## Parameters / Member Variables
- `heap`: The heap relation containing the tuple being checked
- `index`: The index relation supporting the constraint being checked
- `indexInfo`: IndexInfo structure containing constraint metadata, operators, and strategies
- `tupleid`: ItemPointer of the tuple being checked (invalid if not yet inserted)
- `values`: Array of index column values for the new tuple
- `isnull`: Array of NULL flags corresponding to the values array
- `estate`: Executor state providing expression evaluation context
- `newIndex`: Boolean indicating if this is during index creation (affects error messages)
- `waitMode`: CEOUC_WAIT_MODE controlling behavior when conflicts with concurrent transactions are detected
- `violationOK`: Boolean indicating whether to return false on violations instead of throwing errors
- `conflictTid`: Output parameter receiving the ItemPointer of any conflicting tuple found

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes: Gets number of key attributes in the index
  - InitDirtySnapshot: Initializes snapshot for seeing uncommitted changes
  - [ScanKeyEntryInitialize](../S/ScanKeyEntryInitialize.md): Sets up scan keys for index scanning
  - [table_slot_create](../t/table_slot_create.md): Creates tuple table slot for existing tuples
  - GetPerTupleExprContext: Gets expression evaluation context
  - [index_beginscan](../i/index_beginscan.md)/index_rescan: Initiates and configures index scans
  - [index_getnext_slot](../i/index_getnext_slot.md): Retrieves tuples from index scan
  - [FormIndexDatum](../F/FormIndexDatum.md): Extracts index values from heap tuples
  - [index_recheck_constraint](../i/index_recheck_constraint.md): Rechecks constraints for lossy index scans
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md): Compares transaction IDs for ordering
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md): Gets current transaction ID
  - [SpeculativeInsertionWait](../S/SpeculativeInsertionWait.md): Waits for speculative insertions to complete
  - [XactLockTableWait](../X/XactLockTableWait.md): Waits for transaction to complete
  - [BuildIndexValueDescription](../B/BuildIndexValueDescription.md): Creates human-readable descriptions of index values for error messages
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md): Cleans up tuple table slot
- Called from (representative examples):
  - [ExecInsertIndexTuples](../E/ExecInsertIndexTuples.md): Checks exclusion constraints during index tuple insertion
  - [ExecCheckIndexConstraints](../E/ExecCheckIndexConstraints.md): Performs pre-insertion conflict detection
  - [check_exclusion_constraint](check_exclusion_constraint.md): Higher-level exclusion constraint checking

## Notes and Other Information
- This is a static function, internal to the execIndexing.c module
- The function implements a retry mechanism that restarts the entire scan when waiting for concurrent transactions
- NULL handling depends on the ii_NullsNotDistinct setting - with traditional nulls-distinct behavior, any NULL input causes the constraint to pass
- The DirtySnapshot allows seeing uncommitted changes from other transactions, which is essential for detecting conflicts
- Speculative insertion support enables INSERT ... ON CONFLICT functionality by detecting conflicts before final commitment
- The function can operate in different modes: immediate error reporting, conflict detection with return value, or waiting for transaction resolution
- Lossy index scans require additional rechecking using the index_recheck_constraint function
- Error messages are differentiated between new index creation scenarios and runtime constraint violations
- The found_self mechanism ensures the function properly handles the case where the tuple being checked is already in the index
- Livelock prevention logic helps avoid infinite waiting scenarios in concurrent speculative insertion situations