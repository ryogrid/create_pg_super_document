# ProjectSetState

## Location
src/include/nodes/execnodes.h: 1335 - 1343

## Overview
ProjectSetState is an execution state structure for ProjectSet nodes in PostgreSQL's executor, which handle projection operations involving set-returning functions (SRFs) that can produce multiple output tuples from a single input tuple.

## Definition
```c
typedef struct ProjectSetState
{
    PlanState      ps;                     /* its first field is NodeTag */
    Node         **elems;                  /* array of expression states */
    ExprDoneCond  *elemdone;              /* array of per-SRF is-done states */
    int            nelems;                 /* length of elemdone[] array */
    bool           pending_srf_tuples;     /* still evaluating srfs in tlist? */
    MemoryContext  argcontext;            /* context for SRF arguments */
} ProjectSetState;
```

## Detailed Description
ProjectSetState manages the execution state for ProjectSet nodes, which are specialized projection nodes that handle set-returning functions (SRFs) in the target list. These nodes are created when a query contains SRFs that can return multiple rows for each input tuple. The structure maintains arrays to track the state of each expression in the target list, distinguishing between regular expressions and SRFs. It coordinates the evaluation of multiple SRFs to ensure proper tuple generation and manages the memory context for SRF argument evaluation.

## Parameters / Member Variables
- `ps`: Base PlanState structure containing common execution state fields like the node tag and plan information
- `elems`: Array of expression states, where at least one element is a SetExprState for SRFs and the rest are regular ExprStates
- `elemdone`: Array of ExprDoneCond values indicating the completion state of each SRF in the target list
- `nelems`: Length of the elemdone array, indicating the number of expressions being tracked
- `pending_srf_tuples`: Boolean flag indicating whether there are still SRF evaluations pending that could produce more tuples
- `argcontext`: Memory context used for allocating SRF arguments, allowing for proper memory management during SRF evaluation

## Dependencies
- Functions called/Symbols referenced:
  - [PlanState](PlanState.md) (inherited base structure)
  - ExprDoneCond (for tracking SRF completion states)
  - [Node](../N/Node.md) (for expression state array)
  - [MemoryContext](../M/MemoryContext.md) (for SRF argument memory management)
- Called from (representative examples):
  - [ExecProjectSet](../E/ExecProjectSet.md)
  - [ExecInitProjectSet](../E/ExecInitProjectSet.md)
  - [ExecEndProjectSet](../E/ExecEndProjectSet.md)
  - [ExecReScanProjectSet](../E/ExecReScanProjectSet.md)
  - [ExecProjectSRF](../E/ExecProjectSRF.md)

## Notes and Other Information
ProjectSet nodes are crucial for handling complex queries involving set-returning functions like generate_series(), unnest(), or custom SRFs. The pending_srf_tuples flag is essential for coordinating multiple SRFs that may finish at different times. The argcontext provides memory isolation for SRF arguments, preventing memory leaks during repeated SRF evaluations. This node type represents a key component in PostgreSQL's ability to handle lateral joins and complex projections involving functions that return multiple rows.