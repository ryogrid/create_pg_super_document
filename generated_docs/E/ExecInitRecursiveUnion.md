# ExecInitRecursiveUnion

## Location
[src/backend/executor/nodeRecursiveunion.c:167-271](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeRecursiveunion.c#L167-L271)

## Overview
Initializes the execution state for a RecursiveUnion plan node, setting up all necessary data structures, memory contexts, and child nodes required for recursive UNION query execution.

## Definition

```c
RecursiveUnionState *ExecInitRecursiveUnion(RecursiveUnion *node, EState *estate, int eflags)
```
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
  - [tuplestore_begin_heap](../t/tuplestore_begin_heap.md)
  - AllocSetContextCreate
  - [ExecInitResultTypeTL](ExecInitResultTypeTL.md)
  - [ExecInitNode](ExecInitNode.md)
  - [execTuplesHashPrepare](../e/execTuplesHashPrepare.md)
  - [build_hash_table](../b/build_hash_table.md)
  - outerPlan
  - innerPlan
  - outerPlanState
  - innerPlanState
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- Validates that backward scan and mark/restore execution flags are not set, as these are unsupported
- Creates separate memory contexts for hash operations when duplicate elimination is needed
- Registers the state structure with the executor parameter system for WorkTableScan node access
- Does not create expression contexts since RecursiveUnion nodes don't evaluate expressions
- Sets up result slot types before initializing children, as WorkTableScan nodes depend on this
- Supports both hashed (with duplicate elimination) and non-hashed recursive execution modes
- Critical for PostgreSQL's WITH RECURSIVE implementation infrastructure

## Simplified Source

```c
RecursiveUnionState *
ExecInitRecursiveUnion(RecursiveUnion *node, EState *estate, int eflags)
{
    RecursiveUnionState *rustate;
    ParamExecData *prmdata;

    // Validate execution flags
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    // Create and initialize state structure
    rustate = makeNode(RecursiveUnionState);
    rustate->ps.plan = (Plan *) node;
    rustate->ps.state = estate;
    rustate->ps.ExecProcNode = ExecRecursiveUnion;

    // Initialize hash-related fields
    rustate->eqfuncoids = NULL;
    rustate->hashfunctions = NULL;
    rustate->hashtable = NULL;
    rustate->tempContext = NULL;
    rustate->tableContext = NULL;

    // Initialize processing state and tuple stores
    rustate->recursing = false;
    rustate->intermediate_empty = true;
    rustate->working_table = tuplestore_begin_heap(false, false, work_mem);
    rustate->intermediate_table = tuplestore_begin_heap(false, false, work_mem);

    // Create memory contexts for hashing if needed
    if (node->numCols > 0)
    {
        rustate->tempContext = AllocSetContextCreate(CurrentMemoryContext,
                                                    "RecursiveUnion",
                                                    ALLOCSET_DEFAULT_SIZES);
        rustate->tableContext = AllocSetContextCreate(CurrentMemoryContext,
                                                     "RecursiveUnion hash table",
                                                     ALLOCSET_DEFAULT_SIZES);
    }

    // Make state available to WorkTableScan nodes via parameter slot
    prmdata = &(estate->es_param_exec_vals[node->wtParam]);
    Assert(prmdata->execPlan == NULL);
    prmdata->value = PointerGetDatum(rustate);
    prmdata->isnull = false;

    // Validate no qualification expressions
    Assert(node->plan.qual == NIL);

    // Initialize result type and slots
    ExecInitResultTypeTL(&rustate->ps);
    rustate->ps.ps_ProjInfo = NULL;

    // Initialize child nodes
    outerPlanState(rustate) = ExecInitNode(outerPlan(node), estate, eflags);
    innerPlanState(rustate) = ExecInitNode(innerPlan(node), estate, eflags);

    // Set up hashing infrastructure if duplicate elimination is needed
    if (node->numCols > 0)
    {
        execTuplesHashPrepare(node->numCols, node->dupOperators,
                            &rustate->eqfuncoids, &rustate->hashfunctions);
        build_hash_table(rustate);
    }

    return rustate;
}
```