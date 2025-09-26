# transform_json_string_values

## Location
[src/backend/utils/adt/jsonfuncs.c:5829-5861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L5829-L5861)

## Overview
This function iterates over a JSON document and applies a specified transformation action to every string value or element, returning a new text with the transformed values.

## Definition
text *transform_json_string_values(text *json, void *action_state, JsonTransformStringValuesAction transform_action)

## Detailed Description
The function serves as the main entry point for JSON string value transformation in PostgreSQL. It parses a JSON document using the JSON lexer and semantic action framework, allowing for custom transformations of string values while preserving the overall JSON structure. The function sets up a complete JSON parsing context with callback functions for different JSON elements (objects, arrays, scalars) and uses the provided transformation action to modify string values according to custom logic.

The transformation process maintains the original JSON structure while allowing selective modification of string content through the provided action function. This is particularly useful for operations like text search highlighting, string escaping, or content filtering within JSON documents.

## Parameters / Member Variables
- : The input JSON text to be processed and transformed
- : Context data passed to the transformation action function (can be any custom state needed by the transform action)
- : Function pointer defining how string values should be transformed (JsonTransformStringValuesAction type)

## Dependencies
- Functions called/Symbols referenced:
  - [makeJsonLexContext](../m/makeJsonLexContext.md)
  - [makeStringInfo](../m/makeStringInfo.md)
  - [transform_string_values_object_start](transform_string_values_object_start.md)
  - [transform_string_values_object_end](transform_string_values_object_end.md)
  - [transform_string_values_array_start](transform_string_values_array_start.md)
  - [transform_string_values_array_end](transform_string_values_array_end.md)
  - [transform_string_values_scalar](transform_string_values_scalar.md)
  - [transform_string_values_array_element_start](transform_string_values_array_element_start.md)
  - [transform_string_values_object_field_start](transform_string_values_object_field_start.md)
  - pg_parse_json_or_ereport
  - [freeJsonLexContext](../f/freeJsonLexContext.md)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md)
- Called from (representative examples):
  - [ts_headline_json_byid_opt](ts_headline_json_byid_opt.md)

## Notes and Other Information
- The function allocates memory for JsonSemAction and TransformJsonStringValuesState structures that need to be properly managed
- Uses the PostgreSQL JSON parser infrastructure for robust JSON handling
- Returns a new text object containing the transformed JSON, leaving the original unchanged
- Part of the PostgreSQL JSON processing utilities in src/backend/utils/adt/jsonfuncs.c:5829-5861