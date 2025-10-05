# generate_series_int4_support

## Location
[src/backend/utils/adt/int.c:1585-1649](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1585-L1649)

## Overview
A planner support function that provides row count estimation for the generate_series(int4, int4 [, int4]) function to help the PostgreSQL query planner optimize queries.

## Definition
```c
Datum generate_series_int4_support(PG_FUNCTION_ARGS)
```

## Detailed Description
The `generate_series_int4_support` function serves as a planner support function specifically designed to help PostgreSQL's query planner estimate the number of rows that will be returned by `generate_series()` calls with integer arguments. This estimation is crucial for the planner to make informed decisions about query execution strategies, join ordering, and memory allocation. The function examines the function arguments (start, finish, and optional step values) and calculates the expected number of rows using the mathematical formula: `floor((finish - start + step) / step)`. It handles various edge cases including NULL arguments (which return zero rows) and ensures proper estimation for both positive and negative step values.

## Parameters / Member Variables
- `rawreq`: A pointer to the support request node from the planner
- `req`: Cast version of rawreq as SupportRequestRows for row estimation requests
- `args`: List of function arguments from the FuncExpr node
- `arg1`: The start value argument (estimated)
- `arg2`: The finish value argument (estimated)
- `arg3`: The optional step value argument (estimated, defaults to NULL)
- `start`: Double precision start value extracted from constant argument
- `finish`: Double precision finish value extracted from constant argument
- `step`: Double precision step value extracted from constant argument (defaults to 1)

## Dependencies
- Functions called/Symbols referenced:
  - `[SupportRequestRows](../S/SupportRequestRows.md)` - Structure for row estimation support requests
  - [is_funcclause](../i/is_funcclause.md) - Check if the node is a function call expression
  - [estimate_expression_value](../e/estimate_expression_value.md) - Estimate the value of an expression
  - `linitial` - Get the first element from a list
  - `lsecond` - Get the second element from a list
  - `lthird` - Get the third element from a list
  - [DatumGetInt32](../D/DatumGetInt32.md) - Extract int32 value from a Datum
  - `[FuncExpr](../F/FuncExpr.md)` - Function expression node structure
- Called from (representative examples):
  - No direct references found (called by query planner via support function infrastructure)

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:1585-1649`
- Only processes `SupportRequestRows` type support requests
- Returns zero rows if any argument is a constant NULL value
- Uses double arithmetic to avoid overflow hazards during calculation
- The row estimation formula `floor((finish - start + step) / step)` works for both positive and negative step values
- Returns NULL (PG_RETURN_POINTER(ret)) if estimation cannot be performed
- Part of PostgreSQL's planner support function infrastructure for better query optimization
- Validates step != 0 to avoid division by zero errors

## Simplified Source

```c
Datum generate_series_int4_support(PG_FUNCTION_ARGS) {
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);
    Node *ret = NULL;

    // Handle row count estimation requests
    if (IsA(rawreq, SupportRequestRows)) {
        SupportRequestRows *req = (SupportRequestRows *) rawreq;

        // Verify this is a function call and extract arguments
        if (is_funcclause(req->node)) {
            List *args = ((FuncExpr *) req->node)->args;

            // Estimate argument values
            Node *arg1 = estimate_expression_value(req->root, linitial(args));
            Node *arg2 = estimate_expression_value(req->root, lsecond(args));
            Node *arg3 = (list_length(args) >= 3) ?
                        estimate_expression_value(req->root, lthird(args)) : NULL;

            // If any argument is NULL, return zero rows
            if ((IsA(arg1, Const) && ((Const *) arg1)->constisnull) ||
                (IsA(arg2, Const) && ((Const *) arg2)->constisnull) ||
                (arg3 != NULL && IsA(arg3, Const) && ((Const *) arg3)->constisnull)) {
                req->rows = 0;
                ret = (Node *) req;
            }
            // If all arguments are constants, calculate row count
            else if (IsA(arg1, Const) && IsA(arg2, Const) &&
                    (arg3 == NULL || IsA(arg3, Const))) {

                double start = DatumGetInt32(((Const *) arg1)->constvalue);
                double finish = DatumGetInt32(((Const *) arg2)->constvalue);
                double step = arg3 ? DatumGetInt32(((Const *) arg3)->constvalue) : 1;

                // Calculate rows using formula: floor((finish - start + step) / step)
                if (step != 0) {
                    req->rows = floor((finish - start + step) / step);
                    ret = (Node *) req;
                }
            }
        }
    }

    PG_RETURN_POINTER(ret);
}
```