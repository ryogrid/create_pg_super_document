# pg_get_ruledef_ext

## Location
[src/backend/utils/adt/ruleutils.c:556-574](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L556-L574)

## Overview
Extended version of pg_get_ruledef that allows control over pretty-printing formatting options for rewrite rule definitions.

## Definition

```c
Datum
pg_get_ruledef_ext(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides an extended interface for retrieving PostgreSQL rewrite rule definitions with customizable formatting. Unlike the basic pg_get_ruledef function, this version accepts a boolean parameter to control whether the output should be pretty-printed or returned in a more compact format. It delegates the actual work to pg_get_ruledef_worker with appropriate formatting flags.

## Parameters / Member Variables
- `ruleoid`: OID of the rewrite rule to retrieve the definition for (obtained via PG_GETARG_OID(0))
- `pretty`: Boolean flag controlling pretty-printing format (obtained via PG_GETARG_BOOL(1))

## Dependencies
- Functions called/Symbols referenced:
  - [pg_get_ruledef_worker](pg_get_ruledef_worker.md) - Core worker function that generates the rule definition
  - `GET_PRETTY_FLAGS` - Macro to convert boolean to appropriate formatting flags
  - `string_to_text` - Converts C string to PostgreSQL text type
  - `PG_RETURN_TEXT_P` - Macro for returning text result
- Called from (representative examples):
  - No direct callers found in the analyzed codebase (likely called via SQL function interface)

## Notes and Other Information
- Located at src/backend/utils/adt/ruleutils.c:556-574
- Returns NULL if the rule definition cannot be retrieved
- Provides user control over output formatting through the pretty parameter
- Part of PostgreSQL's extended system information functions