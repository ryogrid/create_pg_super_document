# json_to_tsvector

## Location
[src/backend/tsearch/to_tsany.c:423-442](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L423-L442)

## Overview
A PostgreSQL function that converts JSON text data to a text search vector (TSVector) using JSON flags and the current default text search configuration.

## Definition

```c
Datum
json_to_tsvector(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL SQL-callable wrapper that converts JSON text to a TSVector for full-text search operations. It extracts the JSON text input and JSONB flags from function arguments, retrieves the current text search configuration, and delegates the actual conversion work to the  function. The function handles proper memory management by freeing copied arguments before returning the result.

## Parameters / Member Variables
-  (text *): JSON text input to be converted to TSVector
-  (Jsonb *): JSONB flags that control parsing behavior and specify which JSON elements to include

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_P
  - PG_GETARG_JSONB_P
  - [parse_jsonb_index_flags](../p/parse_jsonb_index_flags.md)
  - [getTSCurrentConfig](../g/getTSCurrentConfig.md)
  - [json_to_tsvector_worker](json_to_tsvector_worker.md)
  - PG_FREE_IF_COPY
  - PG_RETURN_TSVECTOR
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's full-text search functionality for JSON data
- Uses the default text search configuration obtained via getTSCurrentConfig(true)
- Implements proper PostgreSQL function calling conventions with PG_FUNCTION_ARGS
- Memory management follows PostgreSQL patterns with PG_FREE_IF_COPY for varlena types
- The actual JSON parsing and TSVector construction is delegated to json_to_tsvector_worker

## Simplified Source

```c
Datum json_to_tsvector(PG_FUNCTION_ARGS)
{
    // Extract JSON text and flags from arguments
    text *json = PG_GETARG_TEXT_P(0);
    Jsonb *jbFlags = PG_GETARG_JSONB_P(1);

    // Parse flags and get current text search configuration
    uint32 flags = parse_jsonb_index_flags(jbFlags);
    Oid cfgId = getTSCurrentConfig(true);

    // Convert JSON to TSVector using worker function
    TSVector result = json_to_tsvector_worker(cfgId, json, flags);

    // Clean up memory and return result
    PG_FREE_IF_COPY(json, 0);
    PG_FREE_IF_COPY(jbFlags, 1);
    PG_RETURN_TSVECTOR(result);
}
```