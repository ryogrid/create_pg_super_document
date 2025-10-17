# pg_get_viewdef_name

## Location
[src/backend/utils/adt/ruleutils.c:715-739](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L715-L739)

## Overview
Provides a PostgreSQL function interface to retrieve the SQL definition of a view using the view's qualified name instead of its OID.

## Definition
```c
Datum pg_get_viewdef_name(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL SQL function entry point for retrieving view definitions by view name. It accepts a qualified view name as text input, resolves it to the corresponding view OID, and then delegates to the core worker function `pg_get_viewdef_worker` to generate the actual view definition string. The function uses default pretty printing with indentation and default column wrapping.

## Parameters / Member Variables
- `viewname`: Text parameter containing the qualified name of the view whose definition is to be retrieved
- `prettyFlags`: Internal variable set to PRETTYFLAG_INDENT for formatted output
- `viewrel`: RangeVar structure representing the parsed view name
- `viewoid`: OID of the resolved view
- `res`: Resulting view definition string

## Dependencies
- Functions called/Symbols referenced:
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md)
  - [textToQualifiedNameList](../t/textToQualifiedNameList.md)
  - RangeVarGetRelid
  - [pg_get_viewdef_worker](pg_get_viewdef_worker.md)
  - [string_to_text](../s/string_to_text.md)
  - PG_RETURN_TEXT_P
  - PRETTYFLAG_INDENT
  - WRAP_COLUMN_DEFAULT
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's rule utilities system
- Located in src/backend/utils/adt/ruleutils.c:715-739
- Returns NULL if the view definition cannot be retrieved
- Uses NoLock when looking up the view name since privileges may not be available
- Automatically enables indentation for better formatted output
- Uses default column wrapping behavior

## Simplified Source

```c
Datum
pg_get_viewdef_name(PG_FUNCTION_ARGS)
{
    text *viewname = PG_GETARG_TEXT_PP(0);
    RangeVar *viewrel;
    Oid viewoid;
    char *res;

    // Convert view name to qualified name list and resolve to OID
    viewrel = makeRangeVarFromNameList(textToQualifiedNameList(viewname));
    viewoid = RangeVarGetRelid(viewrel, NoLock, false);

    // Get view definition with pretty printing enabled
    res = pg_get_viewdef_worker(viewoid, PRETTYFLAG_INDENT, WRAP_COLUMN_DEFAULT);

    if (res == NULL)
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(string_to_text(res));
}
```