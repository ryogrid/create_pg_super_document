# array_subscript_fetch_old

## Location
src/backend/utils/adt/arraysubs.c: 399 - 438

## Overview
Fetches the old array element value during assignment expression evaluation, specifically handling cases where the new-value subexpression contains SubscriptingRef or FieldStore operations.

## Definition
```c
static void array_subscript_fetch_old(ExprState *state,
                                     ExprEvalStep *op,
                                     ExprContext *econtext)
```

## Detailed Description
This function retrieves the existing value of an array element before assignment, which is necessary when the assignment expression's new-value contains SubscriptingRef or FieldStore operations that need to reference the previous value. It handles NULL arrays gracefully by setting the previous value as NULL. For non-NULL arrays, it delegates to `array_get_element` to extract the current element value at the specified subscript position. The retrieved value is stored in the SubscriptingRefState's prevvalue/prevnull fields for use by subsequent operations.

## Parameters / Member Variables
- `state`: ExprState pointer containing expression evaluation state information
- `op`: ExprEvalStep pointer containing the operation details and workspace data
- `econtext`: ExprContext pointer providing the expression evaluation context

## Dependencies
- Functions called/Symbols referenced:
  - ExprEvalStep (struct)
  - SubscriptingRefState (struct)
  - ArraySubWorkspace (struct)
  - array_get_element
- Called from (representative examples):
  - array_exec_setup

## Notes and Other Information
- This is a static function used internally within the array subscripting implementation
- Specifically designed for assignment expressions that need access to the old element value
- Handles NULL arrays by setting both prevvalue to 0 and prevnull to true
- The fetched previous value is stored in SubscriptingRefState for later reference
- Only called when new-value subexpression contains SubscriptingRef or FieldStore operations