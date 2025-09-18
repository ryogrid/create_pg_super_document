# populate_recordset_scalar

## Location
[src/backend/utils/adt/jsonfuncs.c:4288-4304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4288-L4304)

## Overview
A static function that handles scalar values during JSON recordset population, validating input structure and storing scalar values at the appropriate nesting level.

## Definition
```c
static JsonParseErrorType populate_recordset_scalar(void *state, char *token, JsonTokenType tokentype)
```

## Detailed Description
This function is a callback handler for JSON parsing that is invoked when a scalar value (string, number, boolean, or null) is encountered during recordset population. The function enforces structural constraints and manages scalar value storage based on the lexical nesting level.

The function implements different behaviors based on the lexical nesting level:
- Level 0: Rejects scalar values at the top level, as recordset functions require an array structure
- Level 2: Stores the scalar value in the saved_scalar field for later processing (typically as object field values)
- Other levels: No special processing required

The validation at level 0 ensures that recordset functions receive properly structured JSON input (arrays, not scalar values). At level 2, scalar values represent field values within record objects and are saved for subsequent processing by other callback functions.

## Parameters / Member Variables
- `state`: A void pointer that is cast to PopulateRecordsetState, containing the parsing state including the lexer and function name for error reporting
- `token`: A string containing the scalar value as parsed from the JSON input
- `tokentype`: The specific type of the JSON token (JsonTokenType), indicating whether it's a string, number, boolean, or null

## Dependencies
- Functions called/Symbols referenced:
  - [JsonTokenType](../J/JsonTokenType.md) (enum type for token classification)
  - [PopulateRecordsetState](../P/PopulateRecordsetState.md) (struct type)
  - JSON_SUCCESS (return value constant)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - [populate_recordset_worker](populate_recordset_worker.md)
  - JsObjectFree

## Notes and Other Information
- This function is part of the JSON recordset population infrastructure in PostgreSQL
- The saved_scalar field in the state is used to temporarily store scalar values for processing by other callbacks
- The tokentype parameter provides additional context about the scalar value but is not currently used in the function logic
- Error reporting includes the function name from the state for context-appropriate error messages
- Level 2 typically represents scalar values within JSON objects that represent individual record fields