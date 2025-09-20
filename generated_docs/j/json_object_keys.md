# json_object_keys

## Location
[src/backend/utils/adt/jsonfuncs.c:730-783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L730-L783)

## Overview
Extracts all the keys from the top-level JSON object and returns them as a set of rows in a table function format.

## Definition

```c
Datum
json_object_keys(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is a PostgreSQL table function (set-returning function) that parses a JSON object and extracts all keys from the top level. It uses PostgreSQL's SRF (Set-Returning Function) framework to return multiple rows, one for each key found in the JSON object. The function employs a JSON parser with semantic actions to identify object field names during parsing and stores them in an array for later retrieval.

The function operates in two phases:
1. **First call**: Parses the entire JSON input, extracts all top-level object keys, and stores them in the function context
2. **Subsequent calls**: Returns one key per call until all keys have been returned

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to the input JSON text

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL
  - SRF_FIRSTCALL_INIT
  - SRF_PERCALL_SETUP
  - SRF_RETURN_NEXT
  - SRF_RETURN_DONE
  - [makeJsonLexContext](../m/makeJsonLexContext.md)
  - pg_parse_json_or_ereport
  - freeJsonLexContext
  - [okeys_array_start](../o/okeys_array_start.md)
  - [okeys_scalar](../o/okeys_scalar.md)
  - [okeys_object_field_start](../o/okeys_object_field_start.md)
  - [OkeysState](../O/OkeysState.md)
  - [JsonSemAction](../J/JsonSemAction.md)
  - [FuncCallContext](../F/FuncCallContext.md)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function is typically exposed as a SQL function for JSON processing
- Uses the OkeysState structure to maintain state across multiple function calls
- Only processes top-level object keys; nested object keys are ignored
- The semantic action callbacks (okeys_*) handle different JSON elements during parsing
- Memory allocation occurs in the multi-call memory context to persist across function calls
- Returns keys as PostgreSQL text datums