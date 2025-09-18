# jsonb_each_text

## Location
src/backend/utils/adt/jsonfuncs.c: 1966 - 1971

## Overview
The jsonb_each_text function is a PostgreSQL SQL function that expands a JSONB object into a set of key-value pairs, where both keys and values are returned as text.

## Definition
```c
Datum jsonb_each_text(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a thin wrapper around the each_worker_jsonb function, specifically designed to handle JSONB (binary JSON) objects. It calls each_worker_jsonb with the as_text parameter set to true and provides the function name "jsonb_each_text" for error reporting. The function converts JSONB objects into tabular form where both keys and values are represented as text strings, making it useful for applications that need string representations of JSON data.

## Parameters / Member Variables
- This function uses the standard PostgreSQL function call interface (PG_FUNCTION_ARGS) which provides access to:
  - Function arguments through the fcinfo structure
  - Return value handling mechanisms
  - Error reporting context

## Dependencies
- Functions called/Symbols referenced:
  - each_worker_jsonb (called with funcname="jsonb_each_text" and as_text=true)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL interface)

## Notes and Other Information
- Located at src/backend/utils/adt/jsonfuncs.c:1966-1971
- This is a simple delegation function that provides the text-mode variant of JSONB object expansion
- The actual implementation logic is contained in the each_worker_jsonb function
- Returns a set of (key, value) tuples where both elements are text type
- Part of PostgreSQL's JSONB manipulation function suite
- More efficient than JSON processing due to JSONB's binary format