# ExecInitMaterial

## Location
[src/backend/executor/nodeMaterial.c:164-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMaterial.c#L164-L239)

## Overview
ExecInitMaterial initializes a MaterialState node for buffering subplan output, setting up the tuplestore and configuring execution flags for backward scanning, rewinding, and mark/restore operations.

## Definition

```c
structure
	 */
	matstate = makeNode(MaterialState);
```
## Detailed Description
ExecInitMaterial creates and configures a MaterialState node that will buffer subplan output in a tuplestore when needed. The function determines whether buffering is necessary based on the execution flags - buffering is required for backward scanning, mark/restore operations, or when rewinding might be needed frequently.

The function intelligently shields the child node from complex execution flags (REWIND, BACKWARD, MARK) since the Material node handles these requirements through its tuplestore. It also handles the subtle differences between executor flags and tuplestore flags, ensuring proper tuplestore behavior for backward scans.

## Parameters / Member Variables
- : The Material plan node containing the plan structure and configuration
- : The execution state providing context and resources for execution
- : Execution flags indicating required capabilities (REWIND, BACKWARD, MARK)

## Dependencies
- Functions called/Symbols referenced:
  - [Material](../M/Material.md)
  - [MaterialState](../M/MaterialState.md)
  - makeNode
  - [ExecMaterial](ExecMaterial.md)
  - EXEC_FLAG_REWIND
  - EXEC_FLAG_BACKWARD
  - EXEC_FLAG_MARK
  - outerPlan
  - [ExecInitNode](ExecInitNode.md)
  - outerPlanState
  - [ExecInitResultTupleSlotTL](ExecInitResultTupleSlotTL.md)
  - [ExecCreateScanSlotFromOuterPlan](ExecCreateScanSlotFromOuterPlan.md)
  - TTSOpsMinimalTuple
- Called from (representative examples):
  - [ExecInitNode](ExecInitNode.md)

## Notes and Other Information
- Tuplestore initialization is deferred until actually needed during execution
- The function strips REWIND, BACKWARD, and MARK flags when initializing child nodes
- [Material](../M/Material.md) nodes don't need ExprContext since they don't perform qualification or projection
- Uses minimal tuple slots for efficient memory usage in the materialized relation
- Handles the semantic difference between executor BACKWARD flag and tuplestore BACKWARD support by adding REWIND when BACKWARD is requested

## Simplified Source

```c
MaterialState *
ExecInitMaterial(Material *node, EState *estate, int eflags)
{
    MaterialState *matstate;
    Plan *outerPlan;

    // Create and initialize state structure
    matstate = makeNode(MaterialState);
    matstate->ss.ps.plan = (Plan *) node;
    matstate->ss.ps.state = estate;
    matstate->ss.ps.ExecProcNode = ExecMaterial;

    // Determine if tuplestore buffering is needed based on execution flags
    matstate->eflags = (eflags & (EXEC_FLAG_REWIND |
                                  EXEC_FLAG_BACKWARD |
                                  EXEC_FLAG_MARK));

    // Handle tuplestore flag semantics: BACKWARD requires REWIND to prevent
    // premature trimming of stored tuples
    if (eflags & EXEC_FLAG_BACKWARD)
        matstate->eflags |= EXEC_FLAG_REWIND;

    // Initialize tuplestore state
    matstate->eof_underlying = false;
    matstate->tuplestorestate = NULL;

    // Initialize child node with simplified flags
    // Shield child from complex execution requirements that Material handles
    eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

    outerPlan = outerPlan(node);
    outerPlanState(matstate) = ExecInitNode(outerPlan, estate, eflags);

    // Initialize result tuple slot using minimal tuple operations
    ExecInitResultTupleSlotTL(&matstate->ss.ps, &TTSOpsMinimalTuple);
    matstate->ss.ps.ps_ProjInfo = NULL; // No projection needed

    // Create scan slot from outer plan
    ExecCreateScanSlotFromOuterPlan(estate, &matstate->ss, &TTSOpsMinimalTuple);

    return matstate;
}
```