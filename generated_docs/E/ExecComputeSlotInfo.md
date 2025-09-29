# ExecComputeSlotInfo

## Location
[src/backend/executor/execExpr.c:2896-2993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L2896-L2993)

## Overview
Computes optimization information for tuple slot deformation operations by determining slot characteristics and whether deformation steps are actually needed.

## Definition

```c
static bool
ExecComputeSlotInfo(ExprState *state, ExprEvalStep *op)
```
## Detailed Description
ExecComputeSlotInfo is a sophisticated optimization function in PostgreSQL's expression evaluation system that analyzes tuple slot access patterns to determine the most efficient way to handle tuple deformation operations. Its primary goal is to determine whether a slot is 'fixed' (has consistent type and descriptor across all evaluations) and whether deformation is actually necessary.

The function performs several key optimizations:

1. **Fixed slot detection**: Determines if a slot will always have the same tuple descriptor and slot operations across all expression evaluations, enabling compile-time optimizations.

2. **Slot operation identification**: Identifies the specific TupleTableSlotOps being used, which affects how tuples are stored and accessed.

3. **Virtual slot optimization**: Recognizes when slots use virtual tuple storage (TTSOpsVirtual), which doesn't require deformation since the data is already in Datum form.

4. **Deformation necessity**: Returns false if no deformation step is needed (e.g., for virtual slots), allowing the caller to skip unnecessary work.

The function handles three types of slots (inner, outer, scan) and uses different strategies for each based on the plan state configuration and slot operation settings.

## Parameters / Member Variables
- : The ExprState containing the expression evaluation context and parent plan information
- : The ExprEvalStep representing a FETCHSOME operation that needs slot information computed

## Dependencies
- Functions called/Symbols referenced:
  - [ExecGetResultSlotOps](ExecGetResultSlotOps.md) (retrieves slot operations and fixedness information)
  - [ExecGetResultType](ExecGetResultType.md) (gets tuple descriptor for plan state results)
  - innerPlanState/outerPlanState (plan state navigation macros)
  - TTSOpsVirtual (virtual slot operations constant)
- Called from (representative examples):
  - [ExecPushExprSetupSteps](ExecPushExprSetupSteps.md) (during setup step generation for all slot types)
  - [ExecBuildGroupingEqual](ExecBuildGroupingEqual.md) (for grouping comparison operations)
  - [ExecBuildParamSetEqual](ExecBuildParamSetEqual.md) (for parameter set comparison operations)

## Notes and Other Information
- Returns true if a deformation step is required, false if it can be skipped
- The function only operates on FETCHSOME opcodes (EEOP_INNER_FETCHSOME, EEOP_OUTER_FETCHSOME, EEOP_SCAN_FETCHSOME)
- Virtual slots never require deformation since their data is already in Datum form, providing significant performance benefits
- Fixed slots enable compile-time optimizations by providing stable type information
- The function considers both explicitly set slot operations (via parent->innerops, etc.) and dynamically determined operations
- Slot fixedness depends on the plan configuration and can vary based on whether operations are explicitly set or inherited from child plans
- This optimization is crucial for performance as unnecessary tuple deformation can be expensive in tight expression evaluation loops

## Simplified Source

```c
static bool
ExecComputeSlotInfo(ExprState *state, ExprEvalStep *op)
{
    PlanState *parent = state->parent;
    TupleDesc desc = NULL;
    const TupleTableSlotOps *tts_ops = NULL;
    bool isfixed = false;
    ExprEvalOp opcode = op->opcode;

    // Validate operation type
    Assert(opcode == EEOP_INNER_FETCHSOME ||
           opcode == EEOP_OUTER_FETCHSOME ||
           opcode == EEOP_SCAN_FETCHSOME);

    // Use pre-computed slot info if available
    if (op->d.fetch.known_desc != NULL)
    {
        desc = op->d.fetch.known_desc;
        tts_ops = op->d.fetch.kind;
        isfixed = op->d.fetch.kind != NULL;
    }
    else if (!parent)
    {
        isfixed = false;
    }
    // Compute slot info based on slot type
    else if (opcode == EEOP_INNER_FETCHSOME)
    {
        PlanState *is = innerPlanState(parent);

        if (parent->inneropsset && !parent->inneropsfixed)
            isfixed = false;
        else if (parent->inneropsset && parent->innerops)
        {
            isfixed = true;
            tts_ops = parent->innerops;
            desc = ExecGetResultType(is);
        }
        else if (is)
        {
            tts_ops = ExecGetResultSlotOps(is, &isfixed);
            desc = ExecGetResultType(is);
        }
    }
    else if (opcode == EEOP_OUTER_FETCHSOME)
    {
        PlanState *os = outerPlanState(parent);

        if (parent->outeropsset && !parent->outeropsfixed)
            isfixed = false;
        else if (parent->outeropsset && parent->outerops)
        {
            isfixed = true;
            tts_ops = parent->outerops;
            desc = ExecGetResultType(os);
        }
        else if (os)
        {
            tts_ops = ExecGetResultSlotOps(os, &isfixed);
            desc = ExecGetResultType(os);
        }
    }
    else if (opcode == EEOP_SCAN_FETCHSOME)
    {
        desc = parent->scandesc;
        if (parent->scanops)
            tts_ops = parent->scanops;
        if (parent->scanopsset)
            isfixed = parent->scanopsfixed;
    }

    // Store computed slot information
    if (isfixed && desc != NULL && tts_ops != NULL)
    {
        op->d.fetch.fixed = true;
        op->d.fetch.kind = tts_ops;
        op->d.fetch.known_desc = desc;
    }
    else
    {
        op->d.fetch.fixed = false;
        op->d.fetch.kind = NULL;
        op->d.fetch.known_desc = NULL;
    }

    // Skip deformation for virtual slots
    if (op->d.fetch.fixed && op->d.fetch.kind == &TTSOpsVirtual)
        return false;

    return true; // Deformation step needed
}
```