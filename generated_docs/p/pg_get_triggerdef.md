# pg_get_triggerdef

## Location
src/backend/utils/adt/ruleutils.c: 851 - 864

## Overview
Provides a PostgreSQL function interface to retrieve the SQL definition (CREATE TRIGGER statement) of a trigger by its OID.

## Definition
```c
Datum pg_get_triggerdef(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as a PostgreSQL SQL function entry point for retrieving trigger definitions. It takes a trigger OID as input and delegates to the core worker function `pg_get_triggerdef_worker` to generate the actual CREATE TRIGGER statement. The function follows the standard PostgreSQL function interface pattern, extracting the OID parameter, calling the worker function, and returning the result as a PostgreSQL text datum.

## Parameters / Member Variables
- `trigid`: OID of the trigger whose definition is to be retrieved
- `res`: Resulting trigger definition string returned by the worker function

## Dependencies
- Functions called/Symbols referenced:
  - [pg_get_triggerdef_worker](pg_get_triggerdef_worker.md)
  - string_to_text
  - PG_RETURN_TEXT_P
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's rule utilities system
- Located in src/backend/utils/adt/ruleutils.c:851-864
- Returns NULL if the trigger definition cannot be retrieved
- Uses a simple delegation pattern to the worker function
- The worker function is called with `false` parameter, likely controlling some formatting option
- Part of the PostgreSQL function interface accessible via SQL for trigger introspection
- Useful for database administration and schema documentation tasks