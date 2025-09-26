# ExecInitHashJoin

## Location
[src/backend/executor/nodeHashjoin.c:710-858](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHashjoin.c#L710-L858)

## Overview
ExecInitHashJoin initializes a HashJoin node during query plan startup, setting up all necessary state structures, child nodes, expression contexts, and tuple slots required for hash join execution.

## Definition

```c
structure
	 */
	hjstate = makeNode(HashJoinState);
```
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
  - [ExecAssignExprContext](ExecAssignExprContext.md): Sets up expression evaluation context
  - [ExecInitNode](ExecInitNode.md): Recursively initializes child plan nodes
  - [ExecGetResultType](ExecGetResultType.md): Retrieves tuple descriptor from child nodes
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md): Initializes result tuple slot
  - [ExecAssignProjectionInfo](ExecAssignProjectionInfo.md): Sets up tuple projection
  - [ExecInitExtraTupleSlot](ExecInitExtraTupleSlot.md): Creates additional tuple slots
  - [ExecInitNullTupleSlot](ExecInitNullTupleSlot.md): Creates null tuple slots for outer joins
  - [ExecInitQual](ExecInitQual.md): Initializes expression trees for qualifiers
  - [ExecInitExprList](ExecInitExprList.md): Initializes expression lists

- Called from:
  - [ExecInitNode](ExecInitNode.md): General node initialization dispatcher

## Notes and Other Information
Key aspects of the initialization process:

- **Join Type Handling**: The function creates appropriate null tuple slots based on join type - left/anti joins need null inner slots, right/right-anti need null outer slots, and full joins need both
- **Single Match Optimization**: Determines if only the first matching inner tuple needs to be considered (for inner_unique joins or semi-joins)
- **Hash Tuple Slot Trick**: Reuses the Hash node's result tuple slot as the hash tuple slot since Hash nodes don't return tuples via normal execution
- **Expression Initialization**: Sets up all expression trees (join quals, other quals, hash clauses) for runtime evaluation
- **State Reset**: Initializes all hash join state variables to their starting values

The function sets the initial join state to HJ_BUILD_HASHTABLE, indicating that hash table construction is the first operation to perform during execution.

Location: src/backend/executor/nodeHashjoin.c:710-858