# pg_get_viewdef_name_ext

## Location
[src/backend/utils/adt/ruleutils.c:740-767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L740-L767)

## Overview
Provides an extended PostgreSQL function interface to retrieve the SQL definition of a view using the view's qualified name with configurable pretty printing options.

## Definition
```c
Datum pg_get_viewdef_name_ext(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as an extended PostgreSQL SQL function entry point for retrieving view definitions by view name with customizable formatting options. It accepts a qualified view name as text input and a boolean flag to control pretty printing behavior. The function resolves the view name to the corresponding view OID and then delegates to the core worker function `pg_get_viewdef_worker` to generate the actual view definition string. This is the most flexible variant among the view definition retrieval functions.

## Parameters / Member Variables
- `viewname`: Text parameter containing the qualified name of the view whose definition is to be retrieved
- `pretty`: Boolean flag controlling whether to enable pretty printing formatting
- `prettyFlags`: Internal variable containing the computed pretty printing flags
- `viewrel`: RangeVar structure representing the parsed view name
- `viewoid`: OID of the resolved view
- `res`: Resulting view definition string

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOOL
  - GET_PRETTY_FLAGS
  - [makeRangeVarFromNameList](../m/makeRangeVarFromNameList.md)
  - [textToQualifiedNameList](../t/textToQualifiedNameList.md)
  - RangeVarGetRelid
  - [pg_get_viewdef_worker](pg_get_viewdef_worker.md)
  - [string_to_text](../s/string_to_text.md)
  - PG_RETURN_TEXT_P
  - WRAP_COLUMN_DEFAULT
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's rule utilities system
- Located in src/backend/utils/adt/ruleutils.c:740-767
- Returns NULL if the view definition cannot be retrieved
- Uses NoLock when looking up the view name since privileges may not be available
- Allows caller to control pretty printing behavior via the pretty parameter
- Uses default column wrapping behavior
- More flexible than pg_get_viewdef_name as it allows disabling pretty printing