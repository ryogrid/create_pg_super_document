# ExecInitHashJoin

## Location
src/backend/executor/nodeHashjoin.c: 710 - 858

## Overview
ExecInitHashJoin initializes a HashJoin node during query plan startup, setting up all necessary state structures, child nodes, expression contexts, and tuple slots required for hash join execution.

## Definition


## Detailed Description
ExecInitHashJoin is the initialization routine for HashJoin nodes in PostgreSQL's executor. It performs comprehensive setup of the hash join execution state, including:

1. **State Structure Creation**: Allocates and initializes a HashJoinState structure
2. **Child Node Initialization**: Recursively initializes outer and inner plan nodes
3. **Expression Context Setup**: Creates expression evaluation contexts for the join
4. **Tuple Slot Management**: Sets up various tuple slots for different join phases
5. **Join-Specific Configuration**: Configures behavior based on join type (inner, left, right, anti, semi, full)
6. **Hash-Specific Initialization**: Sets up hash-related state variables and structures

The function handles special cases for different join types, particularly around null tuple slot creation for outer joins. It also performs an optimization trick where the hash join node reuses the Hash node's result tuple slot as its internal hash tuple slot, since Hash nodes don't return tuples through the normal ExecProcNode() interface.

## Parameters / Member Variables
- : The HashJoin plan node containing join configuration and child plans
- : The execution state containing global execution context
- : Execution flags controlling behavior (EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK are not supported)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode: Creates new HashJoinState structure  
  - ExecAssignExprContext: Sets up expression evaluation context
  - ExecInitNode: Recursively initializes child plan nodes
  - ExecGetResultType: Retrieves tuple descriptor from child nodes
  - ExecInitResultTupleSlotTL: Initializes result tuple slot
  - ExecAssignProjectionInfo: Sets up tuple projection
  - ExecInitExtraTupleSlot: Creates additional tuple slots
  - ExecInitNullTupleSlot: Creates null tuple slots for outer joins
  - ExecInitQual: Initializes expression trees for qualifiers
  - ExecInitExprList: Initializes expression lists

- Called from:
  - ExecInitNode: General node initialization dispatcher

## Notes and Other Information
Key aspects of the initialization process:

- **Join Type Handling**: The function creates appropriate null tuple slots based on join type - left/anti joins need null inner slots, right/right-anti need null outer slots, and full joins need both
- **Single Match Optimization**: Determines if only the first matching inner tuple needs to be considered (for inner_unique joins or semi-joins)
- **Hash Tuple Slot Trick**: Reuses the Hash node's result tuple slot as the hash tuple slot since Hash nodes don't return tuples via normal execution
- **Expression Initialization**: Sets up all expression trees (join quals, other quals, hash clauses) for runtime evaluation
- **State Reset**: Initializes all hash join state variables to their starting values

The function sets the initial join state to HJ_BUILD_HASHTABLE, indicating that hash table construction is the first operation to perform during execution.

Location: src/backend/executor/nodeHashjoin.c:710-858