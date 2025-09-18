# apply_handle_insert_internal

## Location
src/backend/replication/logical/worker.c: 2464 - 2484

## Overview
A low-level workhorse function that performs the actual tuple insertion operation with privilege checking and index handling for logical replication.

## Definition
```c
static void apply_handle_insert_internal(ApplyExecutionData *edata,
                                        ResultRelInfo *relinfo,
                                        TupleTableSlot *remoteslot)
```

## Detailed Description
This function serves as the core insertion engine for logical replication, handling the actual tuple insertion after all preprocessing has been completed. It operates at a lower level than `apply_handle_insert`, focusing specifically on the database insertion mechanics rather than message processing or routing logic.

The function performs essential validation by checking that indexes are properly opened (if they exist) and ensures the operation has appropriate privileges before executing the insertion. It delegates the actual insertion work to PostgreSQL's standard tuple insertion mechanisms, maintaining consistency with how regular INSERT operations are handled in the database.

This function is designed to be called from multiple contexts, including direct insertions and partition routing scenarios, making it a reusable component in the logical replication insertion pipeline.

## Parameters / Member Variables
- `edata`: ApplyExecutionData structure containing executor state and replication context information
- `relinfo`: ResultRelInfo structure for the specific relation being inserted into (may be a partition of the original target relation)
- `remoteslot`: TupleTableSlot containing the processed tuple data ready for insertion

## Dependencies
- Functions called/Symbols referenced:
  - Assert (macro)
  - RelationGetIndexList
  - TargetPrivilegesCheck
  - ExecSimpleRelationInsert
  - ApplyExecutionData (data structure)
  - ResultRelInfo (data structure)
  - TupleTableSlot (data structure)
  - ACL_INSERT (constant)
- Called from (representative examples):
  - apply_handle_insert
  - apply_handle_tuple_routing

## Notes and Other Information
- This is a static function within the logical replication worker module
- Designed as a reusable workhorse function for different insertion scenarios
- Assumes caller has properly opened indexes, as validated by assertions
- Performs privilege checking using the standard PostgreSQL ACL system
- Uses PostgreSQL's standard tuple insertion infrastructure (ExecSimpleRelationInsert)
- Supports both direct insertions and partition routing scenarios
- Part of the separation between high-level message processing and low-level database operations
- Critical component in maintaining consistency between replicated and local insertions