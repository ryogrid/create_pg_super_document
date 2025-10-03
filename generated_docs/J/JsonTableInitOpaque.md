# JsonTableInitOpaque

## Location
[src/backend/utils/adt/jsonpath_exec.c:4111-4175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L4111-L4175)

## Overview
Initializes the opaque context for JSON_TABLE processing by setting up execution state, PASSING arguments, and the JsonTablePlanState for the root plan.

## Definition

```c
static void
JsonTableInitOpaque(TableFuncScanState *state, int natts)
```
## Detailed Description
JsonTableInitOpaque is a static function responsible for filling in the TableFuncScanState->opaque field when processing JSON_TABLE operations. It performs several key initialization tasks:

1. Creates and initializes a JsonTableExecContext structure to hold execution state
2. Evaluates JSON_TABLE() PASSING arguments and converts them to JsonPathVariable structures for use by the jsonpath executor
3. Sets up an array of JsonTablePlanState pointers for column plans
4. Recursively initializes the root JsonTablePlan and any nested child plans that compute NESTED paths

The function bridges the SQL execution infrastructure with the JSON table processing system, ensuring all necessary context is properly established before JSON_TABLE evaluation begins.

## Parameters / Member Variables
- `*state`: TableFuncScanState pointer containing the scan state for the table function
- `natts`: Integer representing the number of attributes (columns) in the table function
## Dependencies
- Functions called/Symbols referenced:
  - [JsonTableExecContext](JsonTableExecContext.md) (struct allocation)
  - castNode (for TableFuncScan and JsonExpr casting)
  - [palloc0](../p/palloc0.md)/palloc (memory allocation)
  - [ExecEvalExpr](../E/ExecEvalExpr.md) (expression evaluation)
  - [JsonTableInitPlan](JsonTableInitPlan.md) (recursive plan initialization)
  - [exprType](../e/exprType.md)/exprTypmod (expression type information)
- Called from (representative examples):
  - Table function scan initialization routines

## Notes and Other Information
- This is a static function within jsonpath_exec.c, indicating it's internal to JSON path execution
- The function sets a magic number (JSON_TABLE_EXEC_CONTEXT_MAGIC) in the context for validation purposes
- PASSING arguments are evaluated immediately and stored as JsonPathVariable structures for later use
- The function handles the recursive initialization of nested plans through JsonTableInitPlan
- Memory allocation uses the current memory context for the root plan state
- The function assumes that passing value expressions and passing names lists have matching lengths when both are present