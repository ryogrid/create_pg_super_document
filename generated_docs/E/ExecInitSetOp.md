# ExecInitSetOp

## Location
[src/backend/executor/nodeSetOp.c:481-582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L481-L582)

## Overview
ExecInitSetOp initializes the execution state for a SetOp plan node, setting up the necessary data structures, memory contexts, and execution functions for set operations.

## Definition

```c
SetOpState *ExecInitSetOp(SetOp *node, EState *estate, int eflags)
```
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
- `node`: Pointer to the SetOp plan node containing operation configuration, column information, strategy, and child plan references
- `estate`: Pointer to the EState (executor state) containing global execution context and memory management
- `eflags`: Execution flags controlling behavior like backward scanning and mark/restore capabilities

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates SetOpState node)
  - [ExecAssignExprContext](ExecAssignExprContext.md) (sets up expression evaluation context)
  - AllocSetContextCreate (creates memory context for hash table)
  - [ExecInitNode](ExecInitNode.md) (initializes child plan node)
  - [ExecGetResultType](ExecGetResultType.md) (gets result tuple descriptor from child)
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md) (initializes result tuple slot)
  - [execTuplesHashPrepare](../e/execTuplesHashPrepare.md) (prepares hash functions for hashed strategy)
  - [execTuplesMatchPrepare](../e/execTuplesMatchPrepare.md) (prepares equality functions for direct strategy)
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

## Simplified Source

```c
SetOpState *
ExecInitSetOp(SetOp *node, EState *estate, int eflags)
{
    SetOpState *setopstate;
    TupleDesc outerDesc;

    // Validate execution flags
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // Create and initialize state structure
    setopstate = makeNode(SetOpState);
    setopstate->ps.plan = (Plan *) node;
    setopstate->ps.state = estate;
    setopstate->ps.ExecProcNode = ExecSetOp;

    // Initialize state fields
    setopstate->eqfuncoids = NULL;
    setopstate->hashfunctions = NULL;
    setopstate->setop_done = false;
    setopstate->numOutput = 0;
    setopstate->pergroup = NULL;
    setopstate->grp_firstTuple = NULL;
    setopstate->hashtable = NULL;
    setopstate->tableContext = NULL;

    // Create expression context
    ExecAssignExprContext(estate, &setopstate->ps);

    // Create hash table context for hashed strategy
    if (node->strategy == SETOP_HASHED)
        setopstate->tableContext = AllocSetContextCreate(CurrentMemoryContext,
                                                        "SetOp hash table",
                                                        ALLOCSET_DEFAULT_SIZES);

    // Initialize child node with appropriate flags
    if (node->strategy == SETOP_HASHED)
        eflags &= ~EXEC_FLAG_REWIND;
    outerPlanState(setopstate) = ExecInitNode(outerPlan(node), estate, eflags);
    outerDesc = ExecGetResultType(outerPlanState(setopstate));

    // Initialize result slot based on strategy
    ExecInitResultTupleSlotTL(&setopstate->ps,
                             node->strategy == SETOP_HASHED ?
                             &TTSOpsMinimalTuple : &TTSOpsHeapTuple);
    setopstate->ps.ps_ProjInfo = NULL;

    // Prepare comparison and hash functions
    if (node->strategy == SETOP_HASHED)
    {
        execTuplesHashPrepare(node->numCols, node->dupOperators,
                            &setopstate->eqfuncoids, &setopstate->hashfunctions);
        build_hash_table(setopstate);
        setopstate->table_filled = false;
    }
    else
    {
        setopstate->eqfunction = execTuplesMatchPrepare(outerDesc, node->numCols,
                                                       node->dupColIdx, node->dupOperators,
                                                       node->dupCollations, &setopstate->ps);
        setopstate->pergroup = (SetOpStatePerGroup) palloc0(sizeof(SetOpStatePerGroupData));
    }

    return setopstate;
}
```