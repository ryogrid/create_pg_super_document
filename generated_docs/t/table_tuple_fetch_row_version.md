# table_tuple_fetch_row_version

## Location
src/include/access/tableam.h: 1289 - 1314

## Overview
Fetches a tuple at a specific TID with visibility testing, evaluating only the exact tuple at that location without traversing HOT chains.

## Definition


## Detailed Description
This function is part of PostgreSQL's table access method (tableam) interface designed for non-modifying operations on individual tuples. Unlike table_index_fetch_tuple(), this function fetches and evaluates only the specific tuple version located exactly at the given TID, without any traversal of HOT (Heap-Only Tuple) chains or consideration of multiple row versions.

The function performs a visibility test using the provided snapshot to determine if the tuple should be visible to the current transaction. This makes it suitable for operations outside of index entry→table tuple lookups, where you need to access a specific known tuple version rather than finding the currently visible version of a row.

The function includes protection against unexpected usage during logical decoding operations, ensuring data consistency in replication scenarios.

## Parameters / Member Variables
- : Relation (table) from which to fetch the tuple
- : ItemPointer identifying the exact tuple location to fetch
- : Snapshot used for visibility testing to determine if the tuple is visible to the current transaction
- : TupleTableSlot where the fetched tuple data will be stored

## Dependencies
- Functions called/Symbols referenced:
  - Relation (table relation structure)
  - ItemPointer (tuple identifier type)
  - Snapshot (visibility testing context)
  - TupleTableSlot (tuple storage)
  - TransactionIdIsValid (transaction validation)
  - CheckXidAlive (logical decoding check)
  - bsysscan (system scan flag)
  - rd_tableam->tuple_fetch_row_version (table access method function pointer)
- Called from (representative examples):
  - GetTupleForTrigger (src/backend/commands/trigger.c:3497)
  - AfterTriggerExecute (src/backend/commands/trigger.c:4456, 4498)
  - EvalPlanQualFetchRowMark (src/backend/executor/execMain.c:2709)
  - ExecCheckTIDVisible (src/backend/executor/nodeModifyTable.c:354)
  - ExecDelete (src/backend/executor/nodeModifyTable.c:1721)
  - ExecUpdate (src/backend/executor/nodeModifyTable.c:2461)
  - ExecMergeMatched (src/backend/executor/nodeModifyTable.c:2953, 3281)
  - TidNext (src/backend/executor/nodeTidscan.c:380)

## Notes and Other Information
- This is an inline function defined in the tableam.h header file
- Part of the table access method abstraction layer supporting pluggable storage engines
- Designed for non-index-based tuple access where you need a specific tuple version
- The key difference from table_index_fetch_tuple() is that this function does NOT traverse HOT chains
- Includes protection against unexpected calls during logical decoding operations
- Returns true if the tuple was found and passed visibility test, false otherwise
- Commonly used in DML operations (INSERT, UPDATE, DELETE, MERGE) and trigger execution
- Suitable for operations that need to examine a specific tuple version rather than finding the current visible version