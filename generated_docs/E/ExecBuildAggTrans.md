# ExecBuildAggTrans

## Location
[src/backend/executor/execExpr.c:3501-3839](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L3501-L3839)

## Overview
Builds transition/combine function invocations for all aggregate transition and combination functions in a grouping sets phase, supporting both sort-based and hash-based aggregation strategies.

## Definition

```c
structions for each transition value / grouping set combination.
	 */
	for (int transno = 0;
```
## Detailed Description
ExecBuildAggTrans constructs a compiled expression that efficiently evaluates aggregate transition functions across multiple grouping sets. It handles the complete aggregate evaluation pipeline including filter evaluation, argument processing, strict function null checking, distinct value handling, and the actual transition function calls.

The function supports different aggregation modes: combine operations (for parallel aggregation), regular transitions with pre-sorted input, and transitions requiring sorting/distinct processing. For each transition function, it generates appropriate execution steps for filter evaluation, argument preparation, null checking for strict functions, distinct value checking for pre-sorted distinct aggregates, and finally the transition function calls for all applicable grouping sets in both sort and hash contexts.

## Parameters / Member Variables
- : AggState containing all aggregate execution state and configuration
- : AggStatePerPhase describing the current phase of grouping sets processing
- : Whether to generate code for sort-based aggregation
- : Whether to generate code for hash-based aggregation  
- : Whether to check for NULL AggStatePerGroup array pointer

## Dependencies
- Functions called/Symbols referenced:
  - [expr_setup_walker](../e/expr_setup_walker.md) (to analyze expression slot requirements)
  - [ExecPushExprSetupSteps](ExecPushExprSetupSteps.md) (to emit slot deforming steps)
  - [ExecInitExprRec](ExecInitExprRec.md) (to initialize sub-expressions for filters and arguments)
  - [ExprEvalPushStep](ExprEvalPushStep.md) (to add execution steps)
  - [ExecBuildAggTransCall](ExecBuildAggTransCall.md) (to generate transition function calls)
  - [ExecReadyExpr](ExecReadyExpr.md) (to finalize the expression for execution)
- Called from (representative examples):
  - [ExecInitAgg](ExecInitAgg.md) (during aggregate node initialization)
  - [hashagg_recompile_expressions](../h/hashagg_recompile_expressions.md) (when recompiling for hash aggregation)

## Notes and Other Information
- Handles both combine and regular transition modes based on aggsplit setting
- Supports complex argument handling for combine operations with optional deserialization
- Generates efficient null-checking code for strict transition functions
- Handles DISTINCT aggregates with pre-sorted input via specialized distinct checking steps
- Manages filter evaluation before argument evaluation to avoid unnecessary computation
- Creates separate execution paths for sort-based and hash-based aggregation when both are needed
- Uses jump target adjustment mechanism to handle conditional execution flow

## Simplified Source

```c
ExprState *ExecBuildAggTrans(AggState *aggstate, AggStatePerPhase phase,
                           bool doSort, bool doHash, bool nullcheck) {
    ExprState *state = makeNode(ExprState);
    bool isCombine = DO_AGGSPLIT_COMBINE(aggstate->aggsplit);

    // Initialize expression state
    state->expr = (Expr *) aggstate;
    state->parent = &aggstate->ss.ps;

    // Setup slot deformation for all transition inputs
    ExprSetupInfo deform = {0, 0, 0, NIL};
    for (int transno = 0; transno < aggstate->numtrans; transno++) {
        AggStatePerTrans pertrans = &aggstate->pertrans[transno];
        expr_setup_walker((Node *) pertrans->aggref->aggdirectargs, &deform);
        expr_setup_walker((Node *) pertrans->aggref->args, &deform);
        // ... setup other fields
    }
    ExecPushExprSetupSteps(state, &deform);

    // Build evaluation steps for each transition
    for (int transno = 0; transno < aggstate->numtrans; transno++) {
        AggStatePerTrans pertrans = &aggstate->pertrans[transno];

        // Handle filter evaluation first
        if (pertrans->aggref->aggfilter && !isCombine) {
            ExecInitExprRec(pertrans->aggref->aggfilter, state, ...);
            // Add jump if filter fails
        }

        // Evaluate arguments based on mode (combine vs regular vs sorted)
        if (isCombine) {
            // Handle combining transition values
            // ... combine-specific logic
        } else if (!pertrans->aggsortrequired) {
            // Regular transition with pre-sorted input
            // ... regular argument processing
        } else {
            // Sorted/distinct processing
            // ... sorted argument processing
        }

        // Add strict input checking for strict functions
        if (trans_fcinfo->flinfo->fn_strict && pertrans->numTransInputs > 0) {
            // Add null-checking steps
        }

        // Generate transition function calls for sort/hash modes
        if (doSort) {
            // Generate sort-based transition calls
        }
        if (doHash) {
            // Generate hash-based transition calls
        }
    }

    // Finalize expression
    ExecReadyExpr(state);
    return state;
}
```