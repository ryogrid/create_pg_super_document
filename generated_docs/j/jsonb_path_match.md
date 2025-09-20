# jsonb_path_match

## Location
[src/backend/utils/adt/jsonpath_exec.c:497-502](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L497-L502)

## Overview
SQL function wrapper that evaluates a JSONPath predicate expression against a JSONB value and returns a boolean result.

## Definition

```c
Datum
jsonb_path_match(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a PostgreSQL SQL function wrapper that provides the interface for evaluating JSONPath predicate expressions against JSONB data. This function serves as the entry point for the  SQL function and delegates the actual work to  with timezone handling disabled (false parameter).

The function follows PostgreSQL's standard function calling convention using  and returns a  value that represents the boolean result of the JSONPath predicate evaluation.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_path_match_internal](jsonb_path_match_internal.md)
- Called from (representative examples):
  - SQL function calls through PostgreSQL's function manager

## Notes and Other Information
- This is a thin wrapper function that provides the SQL interface
- The actual logic is implemented in 
- Timezone handling is disabled (false parameter) unlike 
- Located in 