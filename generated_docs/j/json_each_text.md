# json_each_text

## Location
[src/backend/utils/adt/jsonfuncs.c:1960-1965](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1960-L1965)

## Overview
The json_each_text function is a PostgreSQL SQL function that expands a JSON object into a set of key-value pairs, where both keys and values are returned as text.

## Definition

```c
Datum
json_each_text(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a thin wrapper around the each_worker function, specifically designed to handle JSON (not JSONB) objects. It calls each_worker with the as_text parameter set to true, ensuring that all JSON values are converted to their text representation. The function is part of PostgreSQL's JSON processing functionality and allows users to decompose JSON objects into tabular form where both keys and values are text strings.

## Parameters / Member Variables
- This function uses the standard PostgreSQL function call interface (PG_FUNCTION_ARGS) which provides access to:
  - Function arguments through the fcinfo structure
  - Return value handling mechanisms
  - Error reporting context

## Dependencies
- Functions called/Symbols referenced:
  - [each_worker](../e/each_worker.md) (called with as_text=true)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL interface)

## Notes and Other Information
- Located at src/backend/utils/adt/jsonfuncs.c:1960-1965
- This is a simple delegation function that provides the text-mode variant of JSON object expansion
- The actual implementation logic is contained in the each_worker function
- Returns a set of (key, value) tuples where both elements are text type
- Part of PostgreSQL's JSON manipulation function suite