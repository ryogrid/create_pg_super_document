# iterate_json_values

## Location
[src/backend/utils/adt/jsonfuncs.c:5708-5732](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5708-L5732)

## Overview
Iterates over JSON values and elements according to specified flags and passes them to a callback function for processing using JSON parsing infrastructure.

## Definition
```c
void iterate_json_values(text *json, uint32 flags, void *action_state,
                        JsonIterateStringValuesAction action)
```

## Detailed Description
This function provides a mechanism for traversing a text-based JSON structure and applying a callback function to selected types of values. It sets up a JSON lexical context and semantic action structure to parse the JSON text. The function configures specific callback handlers for scalar values and object field starts, which are used to filter and process the JSON elements according to the provided flags. Unlike iterate_jsonb_values which works with binary JSONB format, this function processes text-based JSON using the JSON parser infrastructure.

## Parameters / Member Variables
- `json`: The text representation of the JSON data to iterate over
- `flags`: Bitfield flags controlling which types of values to process (jtiKey, jtiString, jtiNumeric, jtiBool)
- `action_state`: User-defined state object passed through to the callback function
- `action`: Callback function of type JsonIterateStringValuesAction that processes each selected value

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - [makeJsonLexContext](../m/makeJsonLexContext.md)
  - [iterate_values_scalar](iterate_values_scalar.md)
  - [iterate_values_object_field_start](iterate_values_object_field_start.md)
  - pg_parse_json_or_ereport
  - [freeJsonLexContext](../f/freeJsonLexContext.md)
- Called from (representative examples):
  - [json_to_tsvector_worker](../j/json_to_tsvector_worker.md)
  - pg_parse_json_or_ereport

## Notes and Other Information
The function creates an IterateJsonStringValuesState structure to maintain parsing state and passes it through the JSON parser's semantic action mechanism. The actual value filtering and callback invocation is delegated to iterate_values_scalar and iterate_values_object_field_start functions. This approach allows for streaming processing of JSON text without requiring conversion to JSONB format first.