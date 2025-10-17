# JsonTableGetValue

## Location
[src/backend/utils/adt/jsonpath_exec.c:4454-4494](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L4454-L4494)

## Overview
JsonTableGetValue is a static function that retrieves the value for a specific column in the current row during JSON_TABLE() function execution, handling different column types including JsonExpr evaluations and ORDINAL columns.

## Definition

```c
static Datum
JsonTableGetValue(TableFuncScanState *state, int colnum,
				  Oid typid, int32 typmod, bool *isnull)
```
## Detailed Description
This function is a core component of PostgreSQL's JSON_TABLE() functionality, responsible for extracting values from JSON data for specific columns. It operates within the context of table function scanning and handles three distinct scenarios:

1. **NULL row pattern values**: When the current row pattern evaluates to NULL, the function returns a NULL Datum
2. **JsonExpr evaluation**: For columns with associated expressions, it evaluates JsonExpr using the expression context, passing the row pattern value through CaseTestExpr
3. **ORDINAL columns**: For ordinal columns, it returns the current row's ordinal number

The function implements careful context management, preserving and restoring the expression context's case values during JsonExpr evaluation. It includes a memory leak warning, indicating that the calling context should be reset frequently to prevent memory accumulation.

## Parameters / Member Variables
- `*state`: TableFuncScanState pointer containing the scan state for the table function execution
- `colnum`: Integer specifying the column number (0-based index) for which to retrieve the value
- `typid`: OID of the target data type for the column value
- `typmod`: Type modifier for the target data type
- `*isnull`: Pointer to boolean flag that will be set to indicate if the returned value is NULL
## Dependencies
- Functions called/Symbols referenced:
  - [GetJsonTableExecContext](../G/GetJsonTableExecContext.md)
  - [list_nth](../l/list_nth.md)
  - [ExecEvalExpr](../E/ExecEvalExpr.md)
  - [Int32GetDatum](../I/Int32GetDatum.md)
- Types referenced:
  - [TableFuncScanState](../T/TableFuncScanState.md)
  - [JsonTableExecContext](JsonTableExecContext.md)
  - [JsonTablePlanState](JsonTablePlanState.md)
  - [JsonTablePlanRowSource](JsonTablePlanRowSource.md)
  - [ExprContext](../E/ExprContext.md)
  - [ExprState](../E/ExprState.md)
- Called from (representative examples):
  - Used internally within JSON_TABLE() execution framework

## Notes and Other Information
- **Memory Management**: The function documentation explicitly warns about memory leaks and recommends frequent context resets
- **Expression Context Handling**: Carefully preserves and restores caseValue_datum and caseValue_isNull to avoid side effects
- **Column Type Support**: Handles both expression-based columns and special ORDINAL columns
- **Location**: src/backend/utils/adt/jsonpath_exec.c:4454-4494
- **Static Function**: Internal implementation detail of JSON path execution, not exposed in public API
- **Return Value**: Returns a Datum representing the column value, with NULL indication through the isnull parameter

## Simplified Source

```c
static Datum
JsonTableGetValue(TableFuncScanState *state, int colnum,
                  Oid typid, int32 typmod, bool *isnull)
{
    // Get execution context and column plan state
    JsonTableExecContext *cxt = GetJsonTableExecContext(state, "JsonTableGetValue");
    ExprState *estate = list_nth(state->colvalexprs, colnum);
    JsonTablePlanState *planstate = cxt->colplanstates[colnum];
    JsonTablePlanRowSource *current = &planstate->current;

    // Case 1: Row pattern value is NULL
    if (current->isnull) {
        *isnull = true;
        return (Datum) 0;
    }

    // Case 2: Evaluate JsonExpr for regular columns
    if (estate) {
        ExprContext *econtext = state->ss.ps.ps_ExprContext;

        // Save current context values
        Datum saved_caseValue = econtext->caseValue_datum;
        bool saved_caseIsNull = econtext->caseValue_isNull;

        // Set row pattern value for evaluation
        econtext->caseValue_datum = current->value;
        econtext->caseValue_isNull = false;

        // Evaluate the expression
        Datum result = ExecEvalExpr(estate, econtext, isnull);

        // Restore original context values
        econtext->caseValue_datum = saved_caseValue;
        econtext->caseValue_isNull = saved_caseIsNull;

        return result;
    }

    // Case 3: ORDINAL column - return row number
    *isnull = false;
    return Int32GetDatum(planstate->ordinal);
}
```