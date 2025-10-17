# populate_recordset_array_element_start

## Location
[src/backend/utils/adt/jsonfuncs.c:4266-4280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L4266-L4280)

## Overview
A static function that validates array elements during JSON recordset population, ensuring that top-level array elements are JSON objects representing individual records.

## Definition
```c
static JsonParseErrorType populate_recordset_array_element_start(void *state, bool isnull)
```

## Detailed Description
This function is a callback handler for JSON parsing that is invoked when an array element starts during JSON recordset population. Its primary purpose is to enforce the constraint that recordset functions require an array of objects, where each object represents a single record. The function performs validation at lexical level 1 (top-level array elements) to ensure that each element is a JSON object.

The validation specifically checks that:
- At lexical level 1 (top-level array elements), the token type must be JSON_TOKEN_OBJECT_START
- If this constraint is violated, an error is reported with an appropriate message

For elements at other nesting levels, no validation is performed as nested structures within records are allowed.

## Parameters / Member Variables
- `state`: A void pointer that is cast to PopulateRecordsetState, containing the parsing state including the lexer and function name for error reporting
- `isnull`: A boolean parameter indicating if the element is null (not used in the current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [PopulateRecordsetState](../P/PopulateRecordsetState.md) (struct type)
  - JSON_TOKEN_OBJECT_START (token type constant for object start)
  - JSON_SUCCESS (return value constant)
  - JsonParseErrorType (return type)
- Called from (representative examples):
  - [populate_recordset_worker](populate_recordset_worker.md)
  - JsObjectFree

## Notes and Other Information
- This function is part of the JSON recordset population infrastructure in PostgreSQL
- The validation ensures that recordset functions receive properly structured input (array of objects)
- The isnull parameter is not currently used but provides extensibility for null element handling
- Error messages include the function name from the state for better user feedback
- This validation is crucial for the proper functioning of downstream record processing functions

## Simplified Source

```c
static JsonParseErrorType populate_recordset_array_element_start(void *state, bool isnull) {
    PopulateRecordsetState *_state = (PopulateRecordsetState *) state;

    // Validate that top-level array elements are objects
    if (_state->lex->lex_level == 1 &&
        _state->lex->token_type != JSON_TOKEN_OBJECT_START) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                       errmsg("argument of %s must be an array of objects",
                              _state->function_name)));
    }

    return JSON_SUCCESS;
}
```