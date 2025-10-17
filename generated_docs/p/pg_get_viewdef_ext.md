# pg_get_viewdef_ext

## Location
[src/backend/utils/adt/ruleutils.c:676-694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L676-L694)

## Overview
Extended version of pg_get_viewdef that provides user control over pretty-printing options for view definition output formatting.

## Definition
```c
Datum pg_get_viewdef_ext(PG_FUNCTION_ARGS)
```

## Detailed Description
This function extends the basic view definition retrieval capability by allowing users to specify whether the output should be pretty-printed or returned in a more compact format. It maintains the same core functionality as pg_get_viewdef but adds flexibility in output formatting. The function accepts a boolean parameter to control formatting and delegates the actual work to pg_get_viewdef_worker with appropriate flags.

This enhanced interface is particularly useful for applications that need to balance between human readability and space efficiency in the generated SQL.

## Parameters / Member Variables
- `viewoid`: OID of the view to retrieve the definition for (obtained via PG_GETARG_OID(0))
- `pretty`: Boolean flag controlling pretty-printing format (obtained via PG_GETARG_BOOL(1))

## Dependencies
- Functions called/Symbols referenced:
  - [pg_get_viewdef_worker](pg_get_viewdef_worker.md) - Core worker function for view definition generation
  - `GET_PRETTY_FLAGS` - Macro to convert boolean to appropriate formatting flags
  - `[string_to_text](../s/string_to_text.md)` - Converts C string to PostgreSQL text type
  - `WRAP_COLUMN_DEFAULT` - Default column width for line wrapping
  - `PG_RETURN_TEXT_P` - Macro for returning text result
- Called from (representative examples):
  - No direct callers found in the analyzed codebase (likely called via SQL function interface)

## Notes and Other Information
- Located at src/backend/utils/adt/ruleutils.c:676-694
- Returns NULL if the view definition cannot be retrieved
- Provides user control over output formatting through the pretty parameter
- Uses the same column wrapping defaults as the basic version
- Part of PostgreSQL's extended system information functions for flexible database introspection

## Simplified Source

```c
Datum
pg_get_viewdef_ext(PG_FUNCTION_ARGS)
{
    Oid viewoid = PG_GETARG_OID(0);
    bool pretty = PG_GETARG_BOOL(1);
    char *res;

    // Convert pretty flag to formatting options
    int prettyFlags = GET_PRETTY_FLAGS(pretty);

    // Get view definition with user-specified formatting
    res = pg_get_viewdef_worker(viewoid, prettyFlags, WRAP_COLUMN_DEFAULT);

    if (res == NULL)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(string_to_text(res));
}
```