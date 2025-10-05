# InitPartitionPruneContext

## Location
[src/backend/executor/execPartition.c:2097-2191](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execPartition.c#L2097-L2191)

## Overview
Initializes a PartitionPruneContext structure for a given list of pruning steps, setting up expression states and partition metadata needed for partition pruning evaluation.

## Definition
```c
static void InitPartitionPruneContext(PartitionPruneContext *context, List *pruning_steps, PartitionDesc partdesc, PartitionKey partkey, PlanState *planstate, ExprContext *econtext)
```

## Detailed Description
This function initializes a PartitionPruneContext structure that contains all the metadata and expression states needed to evaluate partition pruning steps. It copies essential partition information from the PartitionKey and PartitionDesc, allocates arrays for comparison functions and expression states, and initializes expression states for non-constant expressions in the pruning steps. The function handles different expression initialization methods depending on whether a planstate is available, supporting both regular plan execution and external parameter contexts.

## Parameters / Member Variables
- `context`: PartitionPruneContext structure to be initialized
- `pruning_steps`: List of PartitionPruneStep structures containing pruning logic
- `partdesc`: Partition descriptor containing partition bounds and metadata
- `partkey`: Partition key containing partitioning strategy and support functions
- `planstate`: Plan state node (may be NULL for external parameter contexts)
- `econtext`: Expression context for parameter evaluation

## Dependencies
- Functions called/Symbols referenced:
  - [list_head](../l/list_head.md)
  - [bms_is_member](../b/bms_is_member.md)
  - PruneCxtStateIdx
  - [ExecInitExprWithParams](../E/ExecInitExprWithParams.md)
  - [ExecInitExpr](../E/ExecInitExpr.md)
  - [lnext](../l/lnext.md)
- Called from (representative examples):
  - [CreatePartitionPruneState](../C/CreatePartitionPruneState.md) (twice - for initial and exec contexts)

## Notes and Other Information
- Static function only accessible within execPartition.c
- Allocates arrays for comparison functions and expression states based on number of steps and partition attributes
- Only initializes expression states for non-constant expressions (skips Const nodes)
- Supports two expression initialization modes: with planstate (normal execution) and without planstate (external parameters via econtext)
- Creates indexed storage for expression states using PruneCxtStateIdx for efficient lookup during pruning evaluation
- The initialized context is used later during actual partition pruning to evaluate which partitions match the pruning criteria
- Handles null keys by checking the nullkeys bitmapset and skipping initialization for those keys

## Simplified Source

```c
static void
InitPartitionPruneContext(PartitionPruneContext *context,
                          List *pruning_steps,
                          PartitionDesc partdesc,
                          PartitionKey partkey,
                          PlanState *planstate,
                          ExprContext *econtext)
{
    int n_steps = list_length(pruning_steps);
    int partnatts = partkey->partnatts;

    // Copy partition metadata from partkey and partdesc
    context->strategy = partkey->strategy;
    context->partnatts = partnatts;
    context->nparts = partdesc->nparts;
    context->boundinfo = partdesc->boundinfo;
    context->partcollation = partkey->partcollation;
    context->partsupfunc = partkey->partsupfunc;

    // Allocate arrays for comparison functions and expression states
    context->stepcmpfuncs = palloc0(sizeof(FmgrInfo) * n_steps * partnatts);
    context->exprstates = palloc0(sizeof(ExprState *) * n_steps * partnatts);

    // Set context information
    context->ppccontext = CurrentMemoryContext;
    context->planstate = planstate;
    context->exprcontext = econtext;

    // Initialize expression states for each pruning step
    foreach(lc, pruning_steps)
    {
        PartitionPruneStepOp *step = (PartitionPruneStepOp *) lfirst(lc);
        ListCell *lc2 = list_head(step->exprs);

        // Skip non-operation steps
        if (!IsA(step, PartitionPruneStepOp))
            continue;

        // Process each partition key attribute
        for (int keyno = 0; keyno < partnatts; keyno++)
        {
            // Skip null keys
            if (bms_is_member(keyno, step->nullkeys))
                continue;

            if (lc2 != NULL)
            {
                Expr *expr = lfirst(lc2);

                // Only initialize non-constant expressions
                if (!IsA(expr, Const))
                {
                    int stateidx = PruneCxtStateIdx(partnatts, step->step.step_id, keyno);

                    // Initialize expression with or without planstate
                    if (planstate == NULL)
                        context->exprstates[stateidx] =
                            ExecInitExprWithParams(expr, econtext->ecxt_param_list_info);
                    else
                        context->exprstates[stateidx] =
                            ExecInitExpr(expr, context->planstate);
                }
                lc2 = lnext(step->exprs, lc2);
            }
        }
    }
}
```