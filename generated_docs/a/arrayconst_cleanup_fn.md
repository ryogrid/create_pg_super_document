# arrayconst_cleanup_fn

## Location
[src/backend/optimizer/util/predtest.c:1021-1038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L1021-L1038)

## Overview
A static cleanup function that deallocates memory and resources used by an array constant iterator during predicate testing operations in PostgreSQL's query optimizer.

## Definition
```c
static void arrayconst_cleanup_fn(PredIterInfo info)
```

## Detailed Description
This function serves as the cleanup callback in PostgreSQL's predicate iterator framework for array constants. It is responsible for properly deallocating all memory and resources that were allocated during the array constant iteration process. The function ensures that no memory leaks occur by freeing the element values array, element nulls array, operator expression arguments list, and the iterator state structure itself.

This function is typically called when the predicate testing operation is complete or when an error occurs that requires early termination of the iteration process.

## Parameters / Member Variables
- `info`: A `PredIterInfo` structure containing iterator state information, specifically pointing to an `ArrayConstIterState` that needs to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [PredIterInfo](../P/PredIterInfo.md) (parameter type)
  - ArrayConstIterState (state structure)
  - [pfree](../p/pfree.md) (memory deallocation function)
  - [list_free](../l/list_free.md) (list deallocation function)
  - [OpExpr](../O/OpExpr.md) (operator expression structure)
- Called from (representative examples):
  - iterate_end (src/backend/optimizer/util/predtest.c:94)
  - [predicate_classify](../p/predicate_classify.md) (src/backend/optimizer/util/predtest.c:884)

## Notes and Other Information
- This is a static function, accessible only within the predtest.c file
- Part of the iterator pattern used in PostgreSQL's predicate testing system
- Ensures proper memory management by freeing all allocated resources
- Must be paired with corresponding setup/initialization functions to prevent memory leaks
- The function handles cleanup of both scalar arrays (elem_values, elem_nulls) and complex structures (opexpr.args)
- Location: src/backend/optimizer/util/predtest.c:1021-1038