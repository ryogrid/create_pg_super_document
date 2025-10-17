# interval_support

## Location
[src/backend/utils/adt/timestamp.c:1274-1336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L1274-L1336)

## Overview
Planner support function that optimizes calls to `interval_scale()` by identifying and eliminating redundant type conversions when the effective granularity and precision remain unchanged.

## Definition
```c
Datum interval_support(PG_FUNCTION_ARGS)
```

## Detailed Description
The `interval_support` function is a planner support function designed to optimize interval type conversions by detecting when calls to `interval_scale()` are unnecessary. It analyzes the source and target type modifiers to determine if a conversion would actually change the interval's effective resolution or precision.

The function implements sophisticated logic to flatten superfluous scaling operations:
1. Compares the least significant field between source and target typmod
2. Evaluates precision changes for second-containing intervals  
3. Determines if the conversion is a no-op based on granularity and precision rules

A conversion is considered redundant (no-op) when:
- The new least field is the same or coarser than the old least field
- For intervals including seconds: precision stays the same or increases
- For intervals not including seconds: precision changes don't matter

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `rawreq`: Node pointer to the support request
  - `ret`: Return node (NULL unless optimization applies)
  - `req`: SupportRequestSimplify structure when applicable
  - `expr`: FuncExpr representing the interval_scale call
  - `typmod`: Target type modifier constant
  - `source`: Source expression node
  - `new_typmod`: Target type modifier value
  - `old_typmod`: Source type modifier value

## Dependencies
- Functions called/Symbols referenced:
  - IsA (node type checking)
  - [SupportRequestSimplify](../S/SupportRequestSimplify.md)
  - [FuncExpr](../F/FuncExpr.md)
  - [list_length](../l/list_length.md), lsecond, linitial (list operations)
  - [DatumGetInt32](../D/DatumGetInt32.md)
  - [exprTypmod](../e/exprTypmod.md)
  - [intervaltypmodleastfield](intervaltypmodleastfield.md) (called twice)
  - INTERVAL_FULL_PRECISION
  - INTERVAL_PRECISION  
  - MAX_INTERVAL_PRECISION
  - [relabel_to_typmod](../r/relabel_to_typmod.md)
  - PG_RETURN_POINTER
- Called from (representative examples):
  - PostgreSQL planner during query optimization
  - Type conversion optimization passes

## Notes and Other Information
- Part of PostgreSQL's planner support infrastructure for optimizing type conversions
- Only handles SupportRequestSimplify requests, returns NULL for other request types
- Requires constant (non-null) type modifier values to perform optimization analysis
- The optimization logic accounts for the fact that sub-second precision only matters for intervals that include SECOND fields
- Uses `relabel_to_typmod()` to create optimized expressions when conversions are redundant
- Critical for performance in queries with multiple interval type conversions or casts

## Simplified Source

```c
Datum interval_support(PG_FUNCTION_ARGS) {
    Node *rawreq = (Node *) PG_GETARG_POINTER(0);
    Node *ret = NULL;

    // Only handle simplification requests
    if (IsA(rawreq, SupportRequestSimplify)) {
        SupportRequestSimplify *req = (SupportRequestSimplify *) rawreq;
        FuncExpr *expr = req->fcall;
        Node *typmod = (Node *) lsecond(expr->args);

        // Check if typmod is a constant value
        if (IsA(typmod, Const) && !((Const *) typmod)->constisnull) {
            Node *source = (Node *) linitial(expr->args);
            int32 new_typmod = DatumGetInt32(((Const *) typmod)->constvalue);
            bool noop;

            if (new_typmod < 0) {
                noop = true;
            } else {
                // Compare old and new type modifiers
                int32 old_typmod = exprTypmod(source);
                int old_least_field = intervaltypmodleastfield(old_typmod);
                int new_least_field = intervaltypmodleastfield(new_typmod);
                int old_precis = (old_typmod < 0) ? INTERVAL_FULL_PRECISION : INTERVAL_PRECISION(old_typmod);
                int new_precis = INTERVAL_PRECISION(new_typmod);

                // Check if conversion is unnecessary
                // No-op if: new field <= old field AND precision doesn't decrease
                noop = (new_least_field <= old_least_field) &&
                       (old_least_field > 0 /* SECOND */ ||
                        new_precis >= MAX_INTERVAL_PRECISION ||
                        new_precis >= old_precis);
            }

            if (noop)
                ret = relabel_to_typmod(source, new_typmod);
        }
    }

    return PG_RETURN_POINTER(ret);
}
```