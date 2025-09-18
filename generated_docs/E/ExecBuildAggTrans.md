# ExecBuildAggTrans

## Location
src/backend/executor/execExpr.c: 3501 - 3839

## Overview
Builds transition/combine function invocations for all aggregate transition and combination functions in a grouping sets phase, supporting both sort-based and hash-based aggregation strategies.

## Definition


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
  - expr_setup_walker (to analyze expression slot requirements)
  - ExecPushExprSetupSteps (to emit slot deforming steps)
  - ExecInitExprRec (to initialize sub-expressions for filters and arguments)
  - ExprEvalPushStep (to add execution steps)
  - ExecBuildAggTransCall (to generate transition function calls)
  - ExecReadyExpr (to finalize the expression for execution)
- Called from (representative examples):
  - ExecInitAgg (during aggregate node initialization)
  - hashagg_recompile_expressions (when recompiling for hash aggregation)

## Notes and Other Information
- Handles both combine and regular transition modes based on aggsplit setting
- Supports complex argument handling for combine operations with optional deserialization
- Generates efficient null-checking code for strict transition functions
- Handles DISTINCT aggregates with pre-sorted input via specialized distinct checking steps
- Manages filter evaluation before argument evaluation to avoid unnecessary computation
- Creates separate execution paths for sort-based and hash-based aggregation when both are needed
- Uses jump target adjustment mechanism to handle conditional execution flow