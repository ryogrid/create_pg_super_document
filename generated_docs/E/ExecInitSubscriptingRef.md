# ExecInitSubscriptingRef

## Location
[src/backend/executor/execExpr.c:3067-3308](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L3067-L3308)

## Overview
Prepares evaluation of a SubscriptingRef expression for both array/container access and assignment operations, handling subscript validation and setting up appropriate execution steps.

## Definition

```c
struct with function pointers for us to possibly use in
	 * execution steps below;
```
## Detailed Description
ExecInitSubscriptingRef initializes the execution framework for subscripting operations on container types (arrays, JSON, etc.). It handles both fetch operations (reading elements) and assignment operations (modifying elements). The function sets up a SubscriptingRefState structure containing all necessary subscript information and configures the appropriate execution steps based on the container type's supported operations.

For assignments, it supports nested assignment situations where the replacement expression itself needs the old value (via CaseTestExpr mechanism). The function validates that the container type supports the required operations and creates execution steps for subscript checking, old value fetching (if needed), and final fetch/assignment operations.

## Parameters / Member Variables
- : ExprEvalStep structure to be configured for the subscripting operation
- : SubscriptingRef node containing the subscripting expression details
- : ExprState providing the expression evaluation context
- : Pointer to store the result Datum value
- : Pointer to store the result null flag

## Dependencies
- Functions called/Symbols referenced:
  - [getSubscriptingRoutines](../g/getSubscriptingRoutines.md) (to get container-specific methods)
  - [executor_errposition](../e/executor_errposition.md) (for error position reporting)
  - [ExecInitExprRec](ExecInitExprRec.md) (to initialize sub-expressions)
  - [ExprEvalPushStep](ExprEvalPushStep.md) (to add execution steps)
  - [isAssignmentIndirectionExpr](../i/isAssignmentIndirectionExpr.md) (to check for nested assignments)
  - [exprLocation](../e/exprLocation.md) (to get expression location for errors)
- Called from (representative examples):
  - [ExecInitExprRec](ExecInitExprRec.md) (during expression tree initialization)

## Notes and Other Information
- Handles both upper and lower subscript bounds for slicing operations
- Supports omitted subscript bounds in slicing expressions
- Uses container-type-specific routines for actual subscript operations
- Implements strict mode where NULL containers yield NULL results
- Manages jump targets for conditional execution steps
- Allocates SubscriptingRefState with space for all subscript arrays in single allocation
- Reuses CaseTestExpr mechanism for nested assignment value passing

## Simplified Source

```c
static void
ExecInitSubscriptingRef(ExprEvalStep *scratch, SubscriptingRef *sbsref,
                       ExprState *state, Datum *resv, bool *resnull)
{
    bool isAssignment = (sbsref->refassgnexpr != NULL);
    int nupper = list_length(sbsref->refupperindexpr);
    int nlower = list_length(sbsref->reflowerindexpr);
    const SubscriptRoutines *sbsroutines;
    SubscriptingRefState *sbsrefstate;
    SubscriptExecSteps methods;
    List *adjust_jumps = NIL;

    // Get container-specific subscripting routines
    sbsroutines = getSubscriptingRoutines(sbsref->refcontainertype, NULL);
    if (!sbsroutines)
        ereport(ERROR, "type does not support subscripting");

    // Allocate state structure with space for subscript arrays
    sbsrefstate = palloc0(sizeof(SubscriptingRefState) +
                         (nupper + nlower) * subscript_arrays_size);

    // Initialize state structure
    setup_subscripting_ref_state(sbsrefstate, isAssignment, nupper, nlower);

    // Let container-specific code initialize methods
    memset(&methods, 0, sizeof(methods));
    sbsroutines->exec_setup(sbsref, sbsrefstate, &methods);

    // Evaluate the container expression
    ExecInitExprRec(sbsref->refexpr, state, resv, resnull);

    // Add strictness check for fetch operations
    if (!isAssignment && sbsroutines->fetch_strict)
    {
        scratch->opcode = EEOP_JUMP_IF_NULL;
        ExprEvalPushStep(state, scratch);
        adjust_jumps = lappend_int(adjust_jumps, state->steps_len - 1);
    }

    // Evaluate upper subscript indices
    for (int i = 0; i < nupper; i++)
    {
        Expr *expr = list_nth(sbsref->refupperindexpr, i);
        if (expr)
        {
            sbsrefstate->upperprovided[i] = true;
            ExecInitExprRec(expr, state,
                           &sbsrefstate->upperindex[i],
                           &sbsrefstate->upperindexnull[i]);
        }
        else
        {
            sbsrefstate->upperprovided[i] = false;
            sbsrefstate->upperindexnull[i] = true;
        }
    }

    // Evaluate lower subscript indices similarly
    evaluate_lower_subscripts(sbsref, sbsrefstate, state);

    // Add subscript validation step if needed
    if (methods.sbs_check_subscripts)
    {
        scratch->opcode = EEOP_SBSREF_SUBSCRIPTS;
        scratch->d.sbsref_subscript.subscriptfunc = methods.sbs_check_subscripts;
        scratch->d.sbsref_subscript.state = sbsrefstate;
        ExprEvalPushStep(state, scratch);
        adjust_jumps = lappend_int(adjust_jumps, state->steps_len - 1);
    }

    if (isAssignment)
    {
        // Handle assignment operation
        if (!methods.sbs_assign)
            ereport(ERROR, "type does not support subscripted assignment");

        // Handle nested assignment case
        if (isAssignmentIndirectionExpr(sbsref->refassgnexpr))
        {
            // Fetch old value for nested assignment
            setup_old_value_fetch(scratch, &methods, sbsrefstate, state);
        }

        // Evaluate replacement expression with saved context
        Datum *save_caseval = state->innermost_caseval;
        bool *save_casenull = state->innermost_casenull;
        state->innermost_caseval = &sbsrefstate->prevvalue;
        state->innermost_casenull = &sbsrefstate->prevnull;

        ExecInitExprRec(sbsref->refassgnexpr, state,
                       &sbsrefstate->replacevalue, &sbsrefstate->replacenull);

        state->innermost_caseval = save_caseval;
        state->innermost_casenull = save_casenull;

        // Add assignment step
        scratch->opcode = EEOP_SBSREF_ASSIGN;
        scratch->d.sbsref.subscriptfunc = methods.sbs_assign;
        scratch->d.sbsref.state = sbsrefstate;
        ExprEvalPushStep(state, scratch);
    }
    else
    {
        // Simple fetch operation
        scratch->opcode = EEOP_SBSREF_FETCH;
        scratch->d.sbsref.subscriptfunc = methods.sbs_fetch;
        scratch->d.sbsref.state = sbsrefstate;
        ExprEvalPushStep(state, scratch);
    }

    // Fix up jump targets
    finalize_jump_targets(adjust_jumps, state);
}
```