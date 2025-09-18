# ExprEvalPushStep

## Location
src/backend/executor/execExpr.c: 2602 - 2627

## Overview
ExprEvalPushStep adds a new evaluation step to an ExprState's step array, handling dynamic memory allocation and array resizing as needed.

## Definition
void ExprEvalPushStep(ExprState *es, const ExprEvalStep *s)

## Detailed Description
ExprEvalPushStep is a utility function responsible for appending evaluation steps to an ExprState during expression compilation. The function manages a dynamically-sized array of ExprEvalStep structures, automatically allocating initial memory (16 steps) when needed and doubling the allocation size when the current capacity is exceeded. Each step represents a single operation in the expression evaluation sequence. The function performs a deep copy of the provided step using memcpy, ensuring that the step data is preserved independently of the original source. This function is critical to the expression compilation process as it builds the execution plan that will be used during runtime evaluation.

## Parameters / Member Variables
- es: The ExprState structure to which the step will be added
- s: Pointer to the ExprEvalStep to be copied and appended to the steps array

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (for initial memory allocation)
  - [repalloc](../r/repalloc.md) (for expanding existing allocation)
  - memcpy (for copying step data)
  - ExprEvalStep (structure type)
- Called from (representative examples):
  - [ExecInitExpr](ExecInitExpr.md)
  - [ExecInitExprWithParams](ExecInitExprWithParams.md)
  - [ExecInitQual](ExecInitQual.md)
  - [ExecBuildProjectionInfo](ExecBuildProjectionInfo.md)
  - [ExecBuildUpdateProjection](ExecBuildUpdateProjection.md)
  - [ExecInitExprRec](ExecInitExprRec.md) (extensively, for all expression node types)
  - [ExecBuildAggTrans](ExecBuildAggTrans.md)
  - [ExecBuildGroupingEqual](ExecBuildGroupingEqual.md)
  - [ExecInitJsonExpr](ExecInitJsonExpr.md)

## Notes and Other Information
- Uses exponential growth strategy (doubling) for memory allocation efficiency
- Initial allocation size is 16 steps, which handles most simple expressions without reallocation
- The function modifies the ExprState in-place by incrementing steps_len after adding the step
- Memory reallocation means that pointers into the steps array become invalid after calling this function
- Called extensively during expression compilation - typically multiple times per expression node
- The deep copy approach ensures step data persistence regardless of the source step's lifetime
- Critical for building the execution sequence that drives PostgreSQL's expression evaluation engine