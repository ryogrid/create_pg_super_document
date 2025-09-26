# ExecInitRecursiveUnion

## Location
src/backend/executor/nodeRecursiveunion.c: 167 - 271

## Overview
Initializes the execution state for a RecursiveUnion plan node, setting up all necessary data structures, memory contexts, and child nodes required for recursive UNION query execution.

## Definition


## Detailed Description
The `ExecInitRecursiveUnion` function performs comprehensive initialization of a RecursiveUnion plan node, preparing it for recursive query execution. The function creates and configures the RecursiveUnionState structure, which maintains the execution context throughout the recursive process.

Key initialization steps include:
1. **State Structure Creation**: Allocates and initializes the RecursiveUnionState with proper plan references and execution function pointer
2. **Tuple Storage Setup**: Creates working and intermediate tuple stores using `tuplestore_begin_heap` for managing recursive iterations
3. **Memory Context Management**: When duplicate elimination is needed (numCols > 0), creates specialized memory contexts for hash table operations and temporary comparisons
4. **Parameter Registration**: Makes the state available to descendant WorkTableScan nodes through the executor parameter mechanism
5. **Child Node Initialization**: Recursively initializes outer (non-recursive) and inner (recursive) plan nodes
6. **Hash Table Preparation**: When duplicate elimination is required, prepares hash functions and creates the deduplication hash table

The function ensures proper memory management by creating separate contexts for different purposes and validates that unsupported execution flags are not set.

## Parameters / Member Variables
- `node`: Pointer to the RecursiveUnion plan node containing configuration and child plans
- `estate`: Pointer to the executor state containing global execution context and parameters
- `eflags`: Execution flags specifying special execution requirements (backward scan and mark/restore are not supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - tuplestore_begin_heap
  - AllocSetContextCreate
  - ExecInitResultTypeTL
  - ExecInitNode
  - execTuplesHashPrepare
  - build_hash_table
  - outerPlan
  - innerPlan
  - outerPlanState
  - innerPlanState
- Called from (representative examples):
  - ExecInitNode

## Notes and Other Information
- Validates that backward scan and mark/restore execution flags are not set, as these are unsupported
- Creates separate memory contexts for hash operations when duplicate elimination is needed
- Registers the state structure with the executor parameter system for WorkTableScan node access
- Does not create expression contexts since RecursiveUnion nodes don't evaluate expressions
- Sets up result slot types before initializing children, as WorkTableScan nodes depend on this
- Supports both hashed (with duplicate elimination) and non-hashed recursive execution modes
- Critical for PostgreSQL's WITH RECURSIVE implementation infrastructure