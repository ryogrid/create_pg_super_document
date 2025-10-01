# jsonb_in_object_end

## Location
[src/backend/utils/adt/jsonb.c:301-310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L301-L310)

## Overview
A callback function used during JSONB parsing to handle the end of JSON objects, finalizing the parsing state for completed object processing.

## Definition

```c
static JsonParseErrorType
jsonb_in_object_end(void *pstate)
```
## Detailed Description
This function serves as a callback handler in the JSONB input parsing pipeline. When the JSON parser encounters the end of an object ('}' character), this function is invoked to finalize the object parsing process. It pushes a WJB_END_OBJECT token onto the JSONB parse state stack, which signals the completion of object parsing and triggers any necessary cleanup or finalization operations.

The function works as the counterpart to jsonb_in_object_start, completing the bracketing of object parsing within PostgreSQL's streaming JSON parser framework. Together, these callbacks ensure proper nesting and state management during complex JSON object parsing.

## Parameters / Member Variables
- : A void pointer that is cast to JsonbInState*, containing the current parsing state and accumulated results

## Dependencies
- Functions called/Symbols referenced:
  - [pushJsonbValue](../p/pushJsonbValue.md) (pushes parsing tokens onto the state stack)
  - WJB_END_OBJECT (token constant indicating object end)
  - JSON_SUCCESS (return value indicating successful parsing)
  - [JsonbInState](../J/JsonbInState.md) (parsing state structure)
- Called from (representative examples):
  - [jsonb_from_cstring](jsonb_from_cstring.md) (main JSONB input function)
  - [datum_to_jsonb_internal](../d/datum_to_jsonb_internal.md) (internal conversion function)

## Notes and Other Information
- This is a static function internal to the jsonb.c module
- Part of the callback-based JSON parsing architecture in PostgreSQL
- Works in conjunction with jsonb_in_object_start to bracket object parsing
- Returns JSON_SUCCESS to indicate successful processing of the object end token
- The WJB_END_OBJECT token triggers finalization of the parsed object in the JSONB value construction process

## Simplified Source

```c
static JsonParseErrorType jsonb_in_object_end(void *pstate) {
    JsonbInState *_state = (JsonbInState *) pstate;

    // Signal the end of object parsing by pushing END_OBJECT token
    _state->res = pushJsonbValue(&_state->parseState, WJB_END_OBJECT, NULL);

    return JSON_SUCCESS;
}
```