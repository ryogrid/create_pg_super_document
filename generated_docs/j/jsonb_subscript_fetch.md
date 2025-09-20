# jsonb_subscript_fetch

## Location
[src/backend/utils/adt/jsonbsubs.c:235-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonbsubs.c#L235-L260)

## Overview
Evaluates a SubscriptingRef fetch operation to extract an element from a JSONB container using the processed subscripts.

## Definition

```c
static void
jsonb_subscript_fetch(ExprState *state,
					  ExprEvalStep *op,
					  ExprContext *econtext)
```
## Detailed Description
This function performs the actual fetching of an element from a JSONB value during expression evaluation. It takes a source JSONB container (which is guaranteed to be non-NULL) and uses the previously processed subscripts stored in the workspace to extract the desired element. 

The function serves as a thin wrapper around the  function, providing the interface needed for the expression evaluation framework. It converts the source datum to a JSONB pointer and calls the core JSONB element extraction function with the appropriate parameters.

The  mode is set to true for this operation, meaning that NULL source containers would have been handled earlier in the evaluation chain, so this function can assume the source is valid.

## Parameters / Member Variables
- : Expression evaluation state (not directly used in this function)
- : Expression evaluation step containing the SubscriptingRefState and result storage locations
- : Expression context for evaluation (not directly used in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetJsonbP](../D/DatumGetJsonbP.md)
  - [jsonb_get_element](jsonb_get_element.md)
- Called from:
  - [jsonb_exec_setup](jsonb_exec_setup.md)

## Notes and Other Information
- Assumes the source JSONB value is not NULL (enforced by fetch_strict=true setting)
- Uses the workspace index array populated by jsonb_subscript_check_subscripts
- Delegates the actual element extraction logic to jsonb_get_element
- The result is stored directly in op->resvalue, with NULL status in op->resnull
- Part of the expression evaluation framework for JSONB subscripting operations
- The false parameter passed to jsonb_get_element likely controls some aspect of the extraction behavior