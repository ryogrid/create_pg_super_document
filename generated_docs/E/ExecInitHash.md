# ExecInitHash

## Location
[src/backend/executor/nodeHash.c:360-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeHash.c#L360-L412)

## Overview
ExecInitHash is the initialization function for Hash plan nodes that sets up the HashState structure, initializes child nodes, and prepares expression contexts and hash key expressions for hash table construction.

## Definition

```c
structure
	 */
	hashstate = makeNode(HashState);
```
## Detailed Description
ExecInitHash performs comprehensive initialization of Hash plan nodes within PostgreSQL's execution framework. It creates and configures a HashState structure that will manage hash table construction during query execution. The function handles all necessary setup including expression context creation, child node initialization, and hash key expression preparation.

Key initialization tasks include setting up the execution function pointer (to ExecHash), initializing the child plan node that will provide input tuples, and preparing hash key expressions that will be used for tuple hashing. The function also sets up result tuple slots using minimal tuple operations for efficiency, though Hash nodes don't perform projections themselves.

The function enforces execution flag restrictions, rejecting backward scan and mark/restore capabilities since Hash nodes operate by consuming all input to build complete hash tables rather than supporting incremental tuple access patterns.

## Parameters / Member Variables
- : Hash plan node containing configuration and hash key specifications
- : EState providing execution context and shared query state
- : Execution flags controlling scan behavior and optimization options

## Dependencies
- Functions called/Symbols referenced:
  - [Hash](../H/Hash.md) (plan node parameter type)
  - [HashState](../H/HashState.md) (return type and state structure)
  - EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK (unsupported execution flags)
  - [HashState](../H/HashState.md) (state allocation)
  - [ExecHash](ExecHash.md) (execution function assignment)
  - [ExecProcNode](ExecProcNode.md) (execution interface)
  - [ExecAssignExprContext](ExecAssignExprContext.md) (expression context setup)
  - [ExecInitNode](ExecInitNode.md) (child node initialization)
  - outerPlanState, outerPlan (child plan access)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md) (result slot initialization)
  - [ExecInitExprList](ExecInitExprList.md) (hash key expression initialization)
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md) (executor node initialization dispatch)
  - NODEHASH_H (header declaration)

## Notes and Other Information
- Rejects EXEC_FLAG_BACKWARD and EXEC_FLAG_MARK since Hash nodes don't support these scan modes
- Sets ExecProcNode to ExecHash (which will error if called, as Hash nodes use MultiExecHash instead)
- Initializes hashkeys as NIL initially - they will be set later by the parent HashJoin node
- Uses ExecInitResultTupleSlotTL with TTSOpsMinimalTuple for efficient tuple storage
- Does not set up projection info since Hash nodes don't perform tuple projection
- Expects parent HashJoin to coordinate the actual hash table construction process
- Located in src/backend/executor/nodeHash.c:360-412