# arrayexpr_next_fn

## Location
[src/backend/optimizer/util/predtest.c:1069-1080](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/predtest.c#L1069-L1080)

## Overview
A static iterator function that advances through elements of an array expression during predicate testing operations in PostgreSQL's query optimizer.

## Definition
```c
static Node *arrayexpr_next_fn(PredIterInfo info)
```

## Detailed Description
This function serves as the iteration callback in PostgreSQL's predicate iterator framework for array expressions. It advances through the elements of an ArrayExpr node, updating the operator expression to represent the current array element for predicate testing purposes.

The function modifies the second argument of the stored OpExpr to point to the current array element, then advances the iterator position to the next element. When all elements have been processed, it returns NULL to signal the end of iteration.

## Parameters / Member Variables
- `info`: A PredIterInfo structure containing iterator state information, specifically pointing to an ArrayExprIterState that tracks the current position within the array expression

## Dependencies
- Functions called/Symbols referenced:
  - [PredIterInfo](../P/PredIterInfo.md) (parameter type)
  - ArrayExprIterState (state structure)
  - lsecond (list access function for second element)
  - lfirst (list access function for current element)
  - [lnext](../l/lnext.md) (list advance function)
- Called from (representative examples):
  - iterate_end (src/backend/optimizer/util/predtest.c:96)
  - [predicate_classify](../p/predicate_classify.md) (src/backend/optimizer/util/predtest.c:893)

## Notes and Other Information
- This is a static function, accessible only within the predtest.c file
- Part of the iterator pattern used for array expression processing in predicate testing
- Updates the second argument of the OpExpr to point to the current array element
- Uses PostgreSQL's list manipulation functions for safe iteration
- Returns the modified OpExpr wrapped as a Node for predicate testing
- Works in conjunction with arrayexpr_startup_fn and arrayexpr_cleanup_fn
- Location: src/backend/optimizer/util/predtest.c:1069-1080

## Simplified Source

```c
static Node *
arrayexpr_next_fn(PredIterInfo info)
{
    ArrayExprIterState *state = (ArrayExprIterState *) info->state;

    // Check if iteration is complete
    if (state->next == NULL)
        return NULL;

    // Update second argument to current array element
    lsecond(state->opexpr.args) = lfirst(state->next);

    // Advance to next element
    state->next = lnext(info->state_list, state->next);

    return (Node *) &(state->opexpr);
}
```