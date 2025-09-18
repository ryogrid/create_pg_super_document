# JsonSemAction

## Location
src/include/common/jsonapi.h: 132 - 144

## Overview
JsonSemAction is a structure that defines semantic action callbacks for JSON parsing, providing a callback-based interface for handling different JSON constructs during parsing.

## Definition
```c
typedef struct JsonSemAction
{
    void           *semstate;
    json_struct_action object_start;
    json_struct_action object_end;
    json_struct_action array_start;
    json_struct_action array_end;
    json_ofield_action object_field_start;
    json_ofield_action object_field_end;
    json_aelem_action array_element_start;
    json_aelem_action array_element_end;
    json_scalar_action scalar;
} JsonSemAction;
```

## Detailed Description
JsonSemAction implements a callback-driven parser pattern for JSON processing. It allows clients to define custom behavior for each type of JSON construct encountered during parsing. The structure contains function pointers for handling the start and end of objects and arrays, object field processing, array element processing, and scalar value handling. All action functions can be NULL, in which case no action is taken for that construct. This design enables flexible JSON processing ranging from pure validation (all NULL actions) to complex transformation and extraction operations.

## Parameters / Member Variables
- `semstate`: User-defined state pointer passed to all action functions
- `object_start`: Callback invoked when starting to parse a JSON object
- `object_end`: Callback invoked when finishing parsing a JSON object
- `array_start`: Callback invoked when starting to parse a JSON array
- `array_end`: Callback invoked when finishing parsing a JSON array
- `object_field_start`: Callback invoked when starting to parse an object field
- `object_field_end`: Callback invoked when finishing parsing an object field
- `array_element_start`: Callback invoked when starting to parse an array element
- `array_element_end`: Callback invoked when finishing parsing an array element
- `scalar`: Callback invoked when encountering a scalar value (string, number, boolean, null)

## Dependencies
- Functions called/Symbols referenced:
  - json_struct_action (function pointer type)
  - json_ofield_action (function pointer type)
  - json_aelem_action (function pointer type)
  - json_scalar_action (function pointer type)
- Called from (representative examples):
  - [pg_parse_json](../p/pg_parse_json.md)
  - [pg_parse_json_incremental](../p/pg_parse_json_incremental.md)
  - [json_validate](../j/json_validate.md)
  - [jsonb_from_cstring](../j/jsonb_from_cstring.md)
  - [json_object_keys](../j/json_object_keys.md)
  - [each_worker](../e/each_worker.md)
  - [elements_worker](../e/elements_worker.md)
  - [populate_recordset_worker](../p/populate_recordset_worker.md)

## Notes and Other Information
All action function callbacks return JsonParseErrorType to indicate success or failure. If any action returns a non-success value, parsing is immediately abandoned and that error code is propagated. The special return value JSON_SEM_ACTION_FAILED indicates that the action function has already reported the error appropriately. String parameters (fname, token) passed to action functions are palloc'd and ownership is transferred to the action function. The structure enables PostgreSQL's JSON infrastructure to support diverse use cases including validation, extraction, transformation, and conversion to different internal representations like JSONB.