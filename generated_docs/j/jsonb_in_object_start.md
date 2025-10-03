# jsonb_in_object_start

## Location
[src/backend/utils/adt/jsonb.c:290-300](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L290-L300)

## Overview
A callback function used during JSONB parsing to handle the start of JSON objects, initializing the parsing state for object processing.

## Definition

```c
static JsonParseErrorType
jsonb_in_object_start(void *pstate)
```
## Detailed Description
This function serves as a callback handler in the JSONB input parsing pipeline. When the JSON parser encounters the beginning of an object ('{' character), this function is invoked to set up the internal parsing state. It pushes a WJB_BEGIN_OBJECT token onto the JSONB parse state stack and configures unique key validation settings if enabled.

The function operates within PostgreSQL's streaming JSON parser framework, where different callback functions handle various JSON structural elements. This particular callback ensures that object parsing begins with the correct internal state initialization.

## Parameters / Member Variables
- `*pstate`: A void pointer that is cast to JsonbInState*, containing the current parsing state and configuration
## Dependencies
- Functions called/Symbols referenced:
  - [pushJsonbValue](../p/pushJsonbValue.md) (pushes parsing tokens onto the state stack)
  - WJB_BEGIN_OBJECT (token constant indicating object start)
  - JSON_SUCCESS (return value indicating successful parsing)
  - [JsonbInState](../J/JsonbInState.md) (parsing state structure)
- Called from (representative examples):
  - [jsonb_from_cstring](jsonb_from_cstring.md) (main JSONB input function)
  - [datum_to_jsonb_internal](../d/datum_to_jsonb_internal.md) (internal conversion function)

## Notes and Other Information
- This is a static function internal to the jsonb.c module
- Part of the callback-based JSON parsing architecture in PostgreSQL
- Sets up unique_keys validation when configured in the parsing state
- Returns JSON_SUCCESS to indicate successful processing of the object start token
- Works in conjunction with jsonb_in_object_end to bracket object parsing

## Simplified Source

```c
static JsonParseErrorType
jsonb_in_object_start(void *pstate)
{
    JsonbInState *state = (JsonbInState *) pstate;

    // Push object start token onto parse stack
    state->res = pushJsonbValue(&state->parseState, WJB_BEGIN_OBJECT, NULL);

    // Configure unique key validation
    state->parseState->unique_keys = state->unique_keys;

    return JSON_SUCCESS;
}
```