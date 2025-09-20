# jsonb_get_element

## Location
[src/backend/utils/adt/jsonfuncs.c:1529-1676](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L1529-L1676)

## Overview
A core function that extracts elements from JSONB data structures using a path array, supporting both object key lookup and array indexing with optional text conversion.

## Definition

```c
Datum
jsonb_get_element(Jsonb *jb, Datum *path, int npath, bool *isnull, bool as_text)
```
## Detailed Description
The  function implements the fundamental JSONB path traversal logic in PostgreSQL. It navigates through nested JSONB structures (objects, arrays, and scalars) using a sequence of path elements. The function handles object key lookups using string keys, array indexing with both positive and negative indices, and supports extraction from scalar values. It provides comprehensive error handling and null-safety checks, returning appropriate null values when paths don't exist or are invalid.

## Parameters / Member Variables
- : Input JSONB value to extract from
- : Array of Datum values representing the path elements (keys for objects, indices for arrays)
- : Number of elements in the path array
- : Output parameter set to true if the result should be NULL
- : Boolean flag determining output format
  - : Convert result to text representation
  - : Return result as JSONB

## Dependencies
- Functions called/Symbols referenced:
  - JB_ROOT_IS_OBJECT, JB_ROOT_IS_ARRAY, JB_ROOT_IS_SCALAR (JSONB type checking)
  - [getIthJsonbValueFromContainer](../g/getIthJsonbValueFromContainer.md) (array element access)
  - [getKeyJsonValueFromContainer](../g/getKeyJsonValueFromContainer.md) (object key access)
  - [JsonbToCString](../J/JsonbToCString.md), cstring_to_text (text conversion)
  - [JsonbValueAsText](../J/JsonbValueAsText.md) (value-to-text conversion)
  - [JsonbValueToJsonb](../J/JsonbValueToJsonb.md) (value-to-JSONB conversion)
  - JsonContainerIsArray, JsonContainerIsObject (container type checking)
  - strtoint (string-to-integer conversion)
- Called from (representative examples):
  - [get_jsonb_path_all](../g/get_jsonb_path_all.md)
  - [jsonb_subscript_fetch](jsonb_subscript_fetch.md)
  - [jsonb_subscript_fetch_old](jsonb_subscript_fetch_old.md)

## Notes and Other Information
- Located in src/backend/utils/adt/jsonfuncs.c:1529-1676
- Handles negative array indices by converting them to positive indices from the end
- Supports extraction from scalar values when path length is 0 (returns the scalar itself)
- Implements comprehensive error handling for invalid array indices and non-existent object keys
- Returns NULL for attempts to extract from scalars with non-empty paths
- Core building block for JSONB path-based operations in PostgreSQL
- Efficiently handles nested container traversal through iterative processing