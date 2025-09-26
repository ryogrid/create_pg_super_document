# SupportRequestSimplify

## Location
src/include/nodes/supportnodes.h: 64 - 70

## Overview
SupportRequestSimplify is a structure used to request plan-time simplification of function calls from PostgreSQL's support functions, enabling optimizations like replacing unnecessary operations with more efficient equivalents.

## Definition


## Detailed Description
The SupportRequestSimplify structure enables PostgreSQL's planner to perform compile-time optimizations by allowing support functions to simplify calls to their target functions. This mechanism is invoked during the planner's constant-folding pass when function arguments have already been simplified. Support functions can analyze the function call and potentially replace it with a more efficient equivalent operation.

Examples of simplifications include:
- Replacing varchar length coercion that doesn't decrease allowed length with a RelabelType node
- Simplifying mathematical operations like "x + 0" to just "x"
- Other semantically-equivalent transformations that improve execution efficiency

The simplification process operates on FuncExpr nodes, even when the original parse tree contained operator calls (a FuncExpr is synthesized for this purpose).

## Parameters / Member Variables
- : NodeTag identifying this as a SupportRequestSimplify structure
- : Pointer to PlannerInfo containing planner infrastructure; may be NULL in some contexts but can be consulted to obtain information about Vars in the node tree
- : FuncExpr representing the function call to be simplified; contains the target function invocation that the support function should analyze for potential optimizations

## Dependencies
- Functions called/Symbols referenced:
  - FuncExpr
  - NodeTag
  - PlannerInfo

- Called from (representative examples):
  - simplify_function (src/backend/optimizer/util/clauses.c:4118)
  - time_support (src/backend/utils/adt/date.c:1610)
  - numeric_support (src/backend/utils/adt/numeric.c:1199)
  - timestamp_support (src/backend/utils/adt/timestamp.c:330)
  - varchar_support (src/backend/utils/adt/varchar.c:570)

## Notes and Other Information
- The result of simplification should be a semantically-equivalent transformed node tree, or NULL if no simplification is possible
- The original fcall should not be returned or modified directly, as it's not a separately allocated Node
- It's safe to use fcall->args or parts of it in the result tree
- This mechanism is part of PostgreSQL's broader support function infrastructure that allows data types to provide custom optimization logic
- The simplification occurs during plan-time, not execution time, making it a compile-time optimization