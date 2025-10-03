# array_subscript_check_subscripts

## Location
[src/backend/utils/adt/arraysubs.c:180-235](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arraysubs.c#L180-L235)

## Overview
Processes and validates subscript expressions during execution of a SubscriptingRef expression, converting evaluated Datum subscripts to integers and handling NULL values appropriately.

## Definition

```c
static bool
array_subscript_check_subscripts(ExprState *state,
								 ExprEvalStep *op,
								 ExprContext *econtext)
```
## Detailed Description
This function is executed during runtime to validate and process array subscripts that have already been evaluated into Datum form. It operates as part of PostgreSQL's expression evaluation framework and is responsible for converting subscript Datums to plain integers while enforcing NULL-handling rules.

The function processes both upper and lower subscripts (for slice operations) and applies different behaviors based on whether the operation is an assignment or a fetch:
- For assignments: NULL subscripts cause an error
- For fetch operations: NULL subscripts cause the entire operation to return NULL

The converted integer subscripts are stored in the workspace for use by subsequent array operations. The function returns false if any subscript is NULL in a fetch context, signaling the caller to skip the remaining SubscriptingRef sequence.

## Parameters / Member Variables
- `*state`: Expression state context (not directly used in this function)
- `*op`: Expression evaluation step containing the SubscriptingRef state and workspace information
- `*econtext`: Expression evaluation context (not directly used in this function)
## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md) (converts Datum values to 32-bit integers)
  - ereport (for error reporting when NULL subscripts are found in assignments)
- Called from (representative examples):
  - [array_exec_setup](array_exec_setup.md) (sets up this function as part of the expression evaluation sequence)

## Notes and Other Information
- This is a static function internal to the array subscripting module
- Designed to work within PostgreSQL's expression evaluation framework
- Returns boolean to indicate success/failure - false means NULL result due to NULL subscript
- Part of the execution phase, not the parse phase (unlike array_subscript_transform)
- Enforces stricter NULL handling for assignments than for fetch operations
- Stores converted subscripts in ArraySubWorkspace structure for efficient access
- The function signature follows the standard ExprEvalStep function pointer pattern used throughout PostgreSQL's expression evaluator