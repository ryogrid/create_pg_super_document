# jsonb_exists

## Location
[src/backend/utils/adt/jsonb_op.c:21-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_op.c#L21-L45)

## Overview
Tests whether a top-level key exists in a JSONB object or a string value exists as an array element.

## Definition

```c
struct_array_builtin(keys, TEXTOID, &key_datums, &key_nulls, &elem_count);
```
## Detailed Description
The jsonb_exists function implements the PostgreSQL '?' operator for JSONB values. It checks for the existence of a specified key at the top level of a JSONB object, or for the existence of a string element in a JSONB array. The function only performs top-level matching and does not recurse into nested structures.

For JSONB objects, it searches for object keys that match the provided string. For JSONB arrays, it searches for string elements that match exactly. Non-string scalar elements in arrays are not matched by this function.

## Parameters / Member Variables
-  (Jsonb *): The JSONB value to search in
-  (text *): The key or string value to search for

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P
  - PG_GETARG_TEXT_PP
  - [findJsonbValueFromContainer](../f/findJsonbValueFromContainer.md)
  - VARDATA_ANY
  - VARSIZE_ANY_EXHDR
  - PG_RETURN_BOOL
- Types used:
  - Jsonb
  - [JsonbValue](../J/JsonbValue.md)
  - jbvString
- Constants used:
  - JB_FOBJECT
  - JB_FARRAY

## Notes and Other Information
- Only matches at the top level - no recursive search is performed
- For objects: matches against key names (which are always strings)
- For arrays: only matches string elements, not other data types
- Returns true if the key/element exists, false otherwise
- Corresponds to the '?' operator in PostgreSQL JSONB operations