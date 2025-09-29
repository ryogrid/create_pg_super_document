# ExecPushExprSetupSteps

## Location
[src/backend/executor/execExpr.c:2744-2826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L2744-L2826)

## Overview
Generates and adds specific setup steps to an expression state based on collected setup requirements, handling slot deformation and MULTIEXPR subplan initialization.

## Definition

```c
static void
ExecPushExprSetupSteps(ExprState *state, ExprSetupInfo *info)
```

## Detailed Description
ExecPushExprSetupSteps is a crucial function in PostgreSQL's expression evaluation system that translates setup requirements collected during expression analysis into actual executable steps. It handles two primary categories of setup operations that must occur before main expression evaluation can begin.

The function performs setup in a carefully orchestrated sequence:

1. **Slot deformation setup**: Creates steps to fetch and deform tuple slots (inner, outer, scan) as needed by Var references in the expression. This ensures that required tuple attributes are accessible before expression evaluation begins.

2. **MULTIEXPR subplan initialization**: Sets up any MULTIEXPR subplans that appear in the expression. These subplans must be evaluated before their output parameters are referenced, but after any Var references they contain have been prepared.

For each slot type (inner, outer, scan), the function creates appropriate FETCHSOME steps using ExecComputeSlotInfo to optimize the deformation process. MULTIEXPR subplans are initialized using ExecInitSubPlan and added to the parent plan's subplan list before creating execution steps.

## Parameters / Member Variables
- state: The ExprState structure that will receive the generated setup steps
- info: ExprSetupInfo structure containing analyzed setup requirements including slot access patterns and subplan lists

## Dependencies
- Functions called/Symbols referenced:
  - [ExecComputeSlotInfo](ExecComputeSlotInfo.md) (slot information computation and optimization)
  - [ExprEvalPushStep](ExprEvalPushStep.md) (step addition to expression state)
  - [ExecInitSubPlan](ExecInitSubPlan.md) (subplan state initialization)
  - [lappend](../l/lappend.md) (list manipulation for subplan management)
- Called from (representative examples):
  - [ExecCreateExprSetupSteps](ExecCreateExprSetupSteps.md) (primary setup step generation)
  - [ExecBuildUpdateProjection](ExecBuildUpdateProjection.md) (update projection setup)
  - [ExecBuildAggTrans](ExecBuildAggTrans.md) (aggregate transition setup)

## Notes and Other Information
- The function creates steps with specific opcodes (EEOP_INNER_FETCHSOME, EEOP_OUTER_FETCHSOME, EEOP_SCAN_FETCHSOME, EEOP_SUBPLAN) optimized for different setup operations
- [ExecComputeSlotInfo](ExecComputeSlotInfo.md) is called to determine if slot deformation steps are actually needed and to optimize them
- MULTIEXPR subplans are handled specially because they can reference Vars but cannot cross-reference each other
- The function ensures proper ordering: slot preparation before subplan execution before main expression evaluation
- Setup steps are added to the expression state's step list and will be executed before the main expression steps during runtime
- This function is static and only used internally within the expression evaluation system

## Simplified Source

```c
static void
ExecPushExprSetupSteps(ExprState *state, ExprSetupInfo *info)
{
    ExprEvalStep scratch = {0};

    // Setup tuple slot deformation for each slot type as needed

    // Inner slot deformation
    if (info->last_inner > 0)
    {
        scratch.opcode = EEOP_INNER_FETCHSOME;
        scratch.d.fetch.last_var = info->last_inner;
        scratch.d.fetch.fixed = false;
        scratch.d.fetch.kind = NULL;
        scratch.d.fetch.known_desc = NULL;

        // Check if deformation step is actually needed
        if (ExecComputeSlotInfo(state, &scratch))
            ExprEvalPushStep(state, &scratch);
    }

    // Outer slot deformation
    if (info->last_outer > 0)
    {
        scratch.opcode = EEOP_OUTER_FETCHSOME;
        scratch.d.fetch.last_var = info->last_outer;
        scratch.d.fetch.fixed = false;
        scratch.d.fetch.kind = NULL;
        scratch.d.fetch.known_desc = NULL;

        if (ExecComputeSlotInfo(state, &scratch))
            ExprEvalPushStep(state, &scratch);
    }

    // Scan slot deformation
    if (info->last_scan > 0)
    {
        scratch.opcode = EEOP_SCAN_FETCHSOME;
        scratch.d.fetch.last_var = info->last_scan;
        scratch.d.fetch.fixed = false;
        scratch.d.fetch.kind = NULL;
        scratch.d.fetch.known_desc = NULL;

        if (ExecComputeSlotInfo(state, &scratch))
            ExprEvalPushStep(state, &scratch);
    }

    // Setup MULTIEXPR subplans
    foreach(ListCell *lc, info->multiexpr_subplans)
    {
        SubPlan *subplan = (SubPlan *) lfirst(lc);
        SubPlanState *sstate;

        // Initialize subplan state
        if (!state->parent)
            elog(ERROR, "SubPlan found with no parent plan");

        sstate = ExecInitSubPlan(subplan, state->parent);

        // Add subplan to parent's subplan list
        state->parent->subPlan = lappend(state->parent->subPlan, sstate);

        // Create subplan execution step
        scratch.opcode = EEOP_SUBPLAN;
        scratch.d.subplan.sstate = sstate;
        scratch.resvalue = &state->resvalue;
        scratch.resnull = &state->resnull;

        ExprEvalPushStep(state, &scratch);
    }
}
```