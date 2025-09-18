# executePredicate

## Location
[src/backend/utils/adt/jsonpath_exec.c:2025-2104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L2025-L2104)

## Overview
Executes unary or binary predicates with existence semantics by evaluating pairs of items from left and right operand sequences, returning TRUE if any satisfying pair is found.

## Definition
```c
static JsonPathBool executePredicate(JsonPathExecContext *cxt, JsonPathItem *pred, JsonPathItem *larg, JsonPathItem *rarg, JsonbValue *jb, bool unwrapRightArg, JsonPathPredicateCallback exec, void *param)
```

## Detailed Description
This function implements the core logic for executing predicates in JSON path expressions with sophisticated existence semantics. It operates by:

1. **Sequence Evaluation**: Evaluates left and right operands to produce sequences of JSON values
2. **Cartesian Product Logic**: Examines pairs of items from left and right sequences using a callback function
3. **Three-Valued Logic**: Returns jpbTrue, jpbFalse, or jpbUnknown (analogous to SQL NULL)
4. **Mode-Sensitive Behavior**: Handles both strict and lax modes differently:
   - **Lax mode**: Returns immediately on first TRUE result, treats errors as UNKNOWN
   - **Strict mode**: Must examine all pairs even after finding TRUE to check for errors

The function uses existence semantics where TRUE is returned if ANY pair satisfies the predicate condition. Error handling follows SQL semantics where any error results in UNKNOWN unless in lax mode where partial results may be acceptable.

## Parameters / Member Variables
- `cxt`: Pointer to JSON path execution context containing mode settings and state
- `pred`: Pointer to the predicate JSON path item being executed  
- `larg`: Pointer to left operand JSON path item (always auto-unwrapped)
- `rarg`: Pointer to right operand JSON path item (NULL for unary predicates)
- `jb`: Pointer to current JsonbValue context for evaluation
- `unwrapRightArg`: Boolean flag controlling whether right argument should be auto-unwrapped
- `exec`: Callback function pointer for executing the specific predicate logic
- `param`: Generic parameter pointer passed through to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - [executeItemOptUnwrapResultNoThrow](executeItemOptUnwrapResultNoThrow.md)
  - [JsonValueListInitIterator](../J/JsonValueListInitIterator.md)
  - [JsonValueListNext](../J/JsonValueListNext.md)
  - jperIsError
  - jspStrictAbsenceOfErrors
  - [exec](exec.md) (callback function parameter)
- Called from (representative examples):
  - [executeBoolItem](executeBoolItem.md) (multiple call sites for different predicate types)

## Notes and Other Information
- This is a static function used only within the jsonpath_exec.c compilation unit
- Implements SQL-compatible three-valued logic (TRUE/FALSE/UNKNOWN)
- The left argument is always auto-unwrapped while right argument unwrapping is configurable
- Supports both unary predicates (rarg = NULL) and binary predicates
- Critical for implementing comparison operators, existence tests, and other predicate operations in JSON path expressions
- The strict/lax mode distinction affects both error handling and early termination behavior
- Uses callback pattern to allow different predicate implementations to reuse the same sequence iteration logic