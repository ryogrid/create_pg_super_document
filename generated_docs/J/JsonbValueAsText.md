# JsonbValueAsText

## Location
[src/backend/utils/adt/jsonfuncs.c:1803-1849](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1803-L1849)

## Overview
Converts a JsonbValue to its text representation, handling different JSONB value types appropriately.

## Definition
```c
static text *JsonbValueAsText(JsonbValue *v)
```

## Detailed Description
The `JsonbValueAsText` function converts a JsonbValue structure to a PostgreSQL text data type. It handles all the main JSONB value types through a switch statement, converting each type to its appropriate text representation. For null values it returns NULL, boolean values become "true" or "false", strings are converted directly, numeric values use the numeric output function, and binary JSONB data is converted to JSON text format using JsonbToCString.

## Parameters / Member Variables
- `v`: Pointer to the JsonbValue to be converted to text

## Dependencies
- Functions called/Symbols referenced:
  - cstring_to_text_with_len
  - cstring_to_text
  - DirectFunctionCall1
  - [numeric_out](../n/numeric_out.md)
  - [DatumGetCString](../D/DatumGetCString.md)
  - initStringInfo
  - [JsonbToCString](JsonbToCString.md)
  - elog
- Types used:
  - [JsonbValue](JsonbValue.md)
  - jbvNull, jbvBool, jbvString, jbvNumeric, jbvBinary
  - [StringInfoData](../S/StringInfoData.md)
  - Datum
- Called from:
  - JsObjectFree
  - [jsonb_object_field_text](../j/jsonb_object_field_text.md)
  - [jsonb_array_element_text](../j/jsonb_array_element_text.md)
  - [jsonb_get_element](../j/jsonb_get_element.md)
  - [each_worker_jsonb](../e/each_worker_jsonb.md)
  - [elements_worker_jsonb](../e/elements_worker_jsonb.md)

## Notes and Other Information
- This is a static function within jsonfuncs.c, not exposed externally
- Returns NULL for jbvNull type values
- Boolean values are converted to literal "true" or "false" strings
- Numeric values are converted using PostgreSQL's numeric output function
- Binary JSONB values are converted to their JSON string representation
- The function will throw an ERROR for unrecognized JSONB types
- Memory management for the returned text value is handled by PostgreSQL's memory context system