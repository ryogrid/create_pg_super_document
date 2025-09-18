# TransformJsonStringValuesState

## Location
[src/backend/utils/adt/jsonfuncs.c:75-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L75-L82)

## Overview
TransformJsonStringValuesState is a structure that maintains state information for the transform_json_string_values function, which applies transformations to JSON string values and builds the resulting JSON output.

## Definition
```c
typedef struct TransformJsonStringValuesState
{
    JsonLexContext *lex;
    StringInfo  strval;                         /* resulting json */
    JsonTransformStringValuesAction action;     /* an action that will be applied
                                                 * to each json value */
    void       *action_state;                   /* any necessary context for transformation */
} TransformJsonStringValuesState;
```

## Detailed Description
The TransformJsonStringValuesState structure serves as the central state management component for JSON transformation operations. Unlike iteration-only operations, this structure is designed to rebuild JSON while applying transformations to string values. It maintains the parsing context, accumulates the transformed JSON output in a StringInfo buffer, and provides a framework for applying custom transformation actions to JSON values. The structure supports the complete reconstruction of JSON documents with modified string values.

## Parameters / Member Variables
- `lex`: Pointer to JsonLexContext structure that provides the lexical parsing context for JSON processing
- `strval`: StringInfo buffer that accumulates the resulting transformed JSON document as it is being built
- `action`: Function pointer of type JsonTransformStringValuesAction that defines the transformation to be applied to each JSON string value
- `action_state`: Generic void pointer to store any context or state information required by the transformation action function

## Dependencies
- Functions called/Symbols referenced:
  - [JsonLexContext](../J/JsonLexContext.md)
  - StringInfo
  - JsonTransformStringValuesAction
- Called from (representative examples):
  - [transform_json_string_values](../t/transform_json_string_values.md)
  - [transform_string_values_object_start](../t/transform_string_values_object_start.md)
  - [transform_string_values_object_end](../t/transform_string_values_object_end.md)
  - [transform_string_values_array_start](../t/transform_string_values_array_start.md)
  - [transform_string_values_array_end](../t/transform_string_values_array_end.md)
  - [transform_string_values_object_field_start](../t/transform_string_values_object_field_start.md)
  - [transform_string_values_array_element_start](../t/transform_string_values_array_element_start.md)
  - [transform_string_values_scalar](../t/transform_string_values_scalar.md)

## Notes and Other Information
This structure is specifically designed for JSON transformation operations where the original JSON structure is preserved but string values are modified according to the provided action function. The StringInfo buffer efficiently handles the dynamic construction of the resulting JSON document. The structure is used extensively in PostgreSQL JSON transformation functions and provides a clean separation between parsing logic, transformation logic, and output generation.