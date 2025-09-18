# ExecInitSetOp

## Location
[src/backend/executor/nodeSetOp.c:481-582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L481-L582)

## Overview
ExecInitSetOp initializes the execution state for a SetOp plan node, setting up the necessary data structures, memory contexts, and execution functions for set operations.

## Definition


## Detailed Description
ExecInitSetOp performs comprehensive initialization of a SetOp node's execution state. The function:

1. **State Structure Creation**: Allocates and initializes a SetOpState structure with all necessary fields
2. **Strategy-Specific Setup**: Configures different components based on whether the operation uses direct or hashed strategy:
   - **Hashed Strategy**: Creates hash table memory context, prepares hash and equality functions, builds hash table
   - **Direct Strategy**: Prepares equality functions for tuple comparison, allocates per-group state structure
3. **Child Plan Initialization**: Initializes the outer child plan with appropriate execution flags
4. **Result Slot Setup**: Configures result tuple slot with appropriate tuple table slot operations (minimal tuple for hashed, heap tuple for direct)
5. **Function Preparation**: Pre-computes function manager lookup data for equality and hashing operations to optimize inner loop performance

The initialization process adapts to the specific set operation command and strategy, ensuring optimal performance for the chosen execution approach.

## Parameters / Member Variables
- : Pointer to the SetOp plan node containing operation configuration, column information, strategy, and child plan references
- : Pointer to the EState (executor state) containing global execution context and memory management
- : Execution flags controlling behavior like backward scanning and mark/restore capabilities

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates SetOpState node)
  - ExecAssignExprContext (sets up expression evaluation context)
  - AllocSetContextCreate (creates memory context for hash table)
  - [ExecInitNode](ExecInitNode.md) (initializes child plan node)
  - [ExecGetResultType](ExecGetResultType.md) (gets result tuple descriptor from child)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md) (initializes result tuple slot)
  - execTuplesHashPrepare (prepares hash functions for hashed strategy)
  - execTuplesMatchPrepare (prepares equality functions for direct strategy)
  - [build_hash_table](../b/build_hash_table.md) (constructs hash table for hashed strategy)
  - [palloc0](../p/palloc0.md) (allocates zeroed memory for per-group state)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (as part of plan node initialization)

## Notes and Other Information
- Returns pointer to the fully initialized SetOpState structure
- Validates execution flags and asserts unsupported combinations
- Sets up different tuple table slot operations based on strategy (TTSOpsMinimalTuple for hashed, TTSOpsHeapTuple for direct)
- Disables REWIND capability for child plan when using hashed strategy for efficiency
- No projection info is set since SetOp nodes don't perform projections
- Pre-computation of function lookup data optimizes performance during execution
- Part of PostgreSQL's executor initialization framework for set operations