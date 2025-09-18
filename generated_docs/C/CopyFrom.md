# CopyFrom

## Location
src/backend/commands/copyfrom.c: 628 - 1367

## Overview
Executes the COPY FROM operation to transfer data from a file or data source into a PostgreSQL relation, handling parsing, validation, constraint checking, and tuple insertion with performance optimizations.

## Definition
```c
uint64
CopyFrom(CopyFromState cstate)
```

## Detailed Description
CopyFrom is the main execution engine for COPY FROM operations in PostgreSQL. It reads tuples from the configured data source (file, stdin, etc.), validates and transforms them according to table constraints and triggers, and inserts them into the target relation. The function implements several optimization strategies including multi-insert buffering for improved performance, partition tuple routing for partitioned tables, and specialized handling for foreign tables.

The function supports various advanced features including:
- COPY FREEZE optimization for new tables to avoid WAL overhead
- Multi-insert buffering to reduce individual insert costs
- Partition tuple routing for partitioned tables  
- Error handling modes (STOP, IGNORE) for data quality issues
- WHERE clause filtering to selectively import data
- Trigger execution (BEFORE/AFTER INSERT, INSTEAD OF INSERT)
- Constraint validation and partition constraint checking
- Foreign data wrapper integration for foreign tables

## Parameters / Member Variables
- `cstate`: CopyFromState containing all configuration, parsing state, target relation, and execution context for the COPY operation

## Dependencies
- Functions called/Symbols referenced:
  - CreateExecutorState
  - GetCurrentCommandId  
  - ExecInitRangeTable
  - ExecInitResultRelation
  - CheckValidResultRel
  - ExecOpenIndices
  - MakeTransitionCaptureState
  - ExecSetupPartitionTupleRouting
  - CopyMultiInsertInfoInit
  - GetBulkInsertState
  - table_slot_create
  - NextCopyFrom
  - ExecFindPartition
  - ExecGetRootToChildMap
  - execute_attr_map_slot
  - ExecBRInsertTriggers
  - ExecIRInsertTriggers
  - ExecComputeStoredGenerated
  - ExecConstraints
  - ExecPartitionCheck
  - CopyMultiInsertInfoNextFreeSlot
  - CopyMultiInsertInfoStore
  - CopyMultiInsertInfoFlush
  - table_tuple_insert
  - ExecInsertIndexTuples
  - ExecARInsertTriggers
  - FreeExecutorState
- Called from (representative examples):
  - DoCopy (main entry point for COPY command)
  - copy_table (logical replication)

## Notes and Other Information
- Returns the number of tuples successfully processed and inserted
- Implements three insertion methods: CIM_SINGLE (row-by-row), CIM_MULTI (batched), and CIM_MULTI_CONDITIONAL (dynamic based on partition capabilities)
- Multi-insert optimization is disabled for tables with BEFORE/INSTEAD OF triggers or volatile expressions
- COPY FREEZE requires the table to be created/truncated in the current subtransaction and no prior transaction activity
- Supports ON ERROR IGNORE mode to skip malformed rows rather than aborting the entire operation
- Maintains detailed progress reporting through pgstat_progress_update_param calls
- Handles memory management by switching between per-tuple and query contexts as needed
- Integrates with PostgreSQL's trigger system, constraint checking, and partition pruning mechanisms