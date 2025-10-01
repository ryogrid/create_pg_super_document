# populate_array_report_expected_array

## Location
[src/backend/utils/adt/jsonfuncs.c:2508-2557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2508-L2557)

## Overview
A helper function that generates detailed error messages when JSON array processing encounters unexpected non-array values during array population operations.

## Definition

```c
static void
populate_array_report_expected_array(PopulateArrayContext *ctx, int ndim)
```
## Detailed Description
The  function is a diagnostic helper function used during JSON/JSONB array processing operations. When the array population process expects to find a JSON array but encounters a different JSON value type, this function generates appropriate error messages with contextual information to help users identify the problematic data.

The function provides two types of error reporting:
1. **Simple case (ndim <= 0)**: Reports a basic "expected JSON array" error
2. **Complex case (ndim > 0)**: Reports the error with specific array indices showing exactly where in the multi-dimensional array structure the problem occurred

The function constructs detailed error hints that include:
- The column name (if available)
- Array element indices showing the path to the problematic location
- Context-appropriate error messages

## Parameters / Member Variables
- : PopulateArrayContext pointer containing:
  - : Column name for error context (can be NULL)
  - : Error handling context for soft errors
  - : Total number of dimensions in the array
  - : Array of current dimension counters
- : Current dimension level where the error occurred

## Dependencies
- Functions called/Symbols referenced:
  -  (PostgreSQL error reporting function)
  -  (string buffer initialization)
  -  (string buffer appending)
- Called from (representative examples):
  - 
  - 
  - 
  - 
  - 

## Notes and Other Information
- This is a static helper function used internally within the JSON functions module
- Located in 
- The function uses PostgreSQL's  mechanism for error reporting, which supports both hard errors and soft error contexts
- Error code used: 
- The function constructs array index paths like "[0][1][2]" to show users exactly where in nested arrays the error occurred
- The function always returns void and never returns normally - it always raises an error
- Supports both named columns (when  is set) and anonymous array contexts

## Simplified Source

```c
static void
populate_array_report_expected_array(PopulateArrayContext *ctx, int ndim)
{
    // Simple case: basic error for invalid dimension count
    if (ndim <= 0)
    {
        if (ctx->colname)
            errsave(ctx->escontext,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("expected JSON array"),
                     errhint("See the value of key \"%s\".", ctx->colname)));
        else
            errsave(ctx->escontext,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("expected JSON array")));
        return;
    }
    else
    {
        // Complex case: build array index path for detailed error
        StringInfoData indices;
        int i;

        initStringInfo(&indices);
        Assert(ctx->ndims > 0 && ndim < ctx->ndims);

        // Build index path like "[0][1][2]"
        for (i = 0; i < ndim; i++)
            appendStringInfo(&indices, "[%d]", ctx->sizes[i]);

        if (ctx->colname)
            errsave(ctx->escontext,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("expected JSON array"),
                     errhint("See the array element %s of key \"%s\".",
                            indices.data, ctx->colname)));
        else
            errsave(ctx->escontext,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                     errmsg("expected JSON array"),
                     errhint("See the array element %s.", indices.data)));
        return;
    }
}
```