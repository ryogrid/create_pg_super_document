# JsValue

## Location
src/backend/utils/adt/jsonfuncs.c: 291 - 305

## Overview
JsValue is a generalized structure for passing JSON/JSONB values within PostgreSQL's JSON processing functions, providing a unified interface for both JSON text and binary JSONB representations.

## Definition


## Detailed Description
JsValue serves as a polymorphic wrapper that can hold either JSON text data or binary JSONB data. This abstraction allows PostgreSQL's JSON processing functions to work uniformly with both representations without needing separate code paths. The structure uses a discriminated union pattern where the  flag determines which variant of the data is currently stored.

For JSON text representation, it stores the raw string data along with its length and token type information. For JSONB representation, it holds a pointer to a JsonbValue structure that contains the binary representation.

## Parameters / Member Variables
- : Boolean flag indicating whether this contains JSON text (true) or JSONB binary data (false)
- : Pointer to the JSON string data when holding text representation
- : Length of the JSON string, or -1 if the string is null-terminated
- : The JsonTokenType indicating the JSON value type (object, array, string, number, etc.)
- : Pointer to JsonbValue structure when holding binary JSONB representation

## Dependencies
- Functions called/Symbols referenced:
  - [JsonTokenType](JsonTokenType.md) (enum for JSON token types)
  - [JsonbValue](JsonbValue.md) (structure for JSONB binary data)
- Called from (representative examples):
  - JsObjectFree (memory management)
  - [populate_array_element](../p/populate_array_element.md) (array processing)
  - [populate_composite](../p/populate_composite.md) (composite type handling)
  - [JsValueToJsObject](JsValueToJsObject.md) (conversion to JsObject)
  - [populate_scalar](../p/populate_scalar.md) (scalar value processing)
  - [json_populate_type](../j/json_populate_type.md) (type population functions)

## Notes and Other Information
- This structure is central to PostgreSQL's JSON/JSONB interoperability
- The discriminated union design allows efficient handling of both text and binary JSON formats
- Memory management must consider both variants when freeing JsValue instances
- The structure is used extensively in JSON parsing, type conversion, and data population functions
- Part of the internal JSON processing API in src/backend/utils/adt/jsonfuncs.c