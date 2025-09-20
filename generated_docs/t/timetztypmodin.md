# timetztypmodin

## Location
[src/backend/utils/adt/date.c:2383-2390](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L2383-L2390)

## Overview
Processes type modifier input for the TIMETZ (time with time zone) data type, validating and converting precision specifications from SQL DDL statements.

## Definition

```c
Datum
timetztypmodin(PG_FUNCTION_ARGS)
```
## Detailed Description
The `timetztypmodin` function is responsible for parsing and validating type modifier input for TIMETZ columns during table creation or type casting operations. When a user specifies a TIMETZ column with precision (e.g., `TIMETZ(3)` for 3-digit fractional seconds), this function processes that specification.

The function extracts the array of type modifiers from the function arguments and delegates the actual validation to `anytime_typmodin`, which is shared between TIME and TIMETZ types. The `true` parameter indicates this is for a timezone-aware time type.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing the array of type modifiers

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P: Extracts array of type modifiers from function args
  - [anytime_typmodin](../a/anytime_typmodin.md): Common validation logic for TIME/TIMETZ type modifiers (called with istz=true)
  - PG_RETURN_INT32: Returns the validated type modifier value
- Called from (representative examples):
  - PostgreSQL parser and type system (indirectly through function registry)

## Notes and Other Information
- This function is registered in the PostgreSQL type system as the typmodin function for the TIMETZ type
- Type modifiers for TIMETZ typically specify precision (0-6 digits for fractional seconds)
- The function ensures only valid precision values are accepted according to SQL standards
- Shares common validation logic with `timetypmodin` through the `anytime_typmodin` helper function
- Part of PostgreSQL's type modifier validation system used during DDL processing