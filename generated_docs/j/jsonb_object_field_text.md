# jsonb_object_field_text

## Location
[src/backend/utils/adt/jsonfuncs.c:898-919](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L898-L919)

## Overview
Extracts a field from a JSONB object by key name and returns the field value as text, or NULL if the key is not found, the input is not an object, or the value is JSON null.

## Definition
```c
Datum jsonb_object_field_text(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the JSONB object field access operator (->>) for text output. It takes a JSONB value and a text key as input parameters, searches for the specified key within the JSONB object, and returns the corresponding value as a text datum. Unlike the -> operator which returns JSONB format, this operator returns the raw text representation of the value. The function performs type checking to ensure the input JSONB is an object and also checks that the found value is not a JSON null before conversion to text.

The function uses the same key lookup mechanism as jsonb_object_field but converts the result to text using JsonbValueAsText instead of returning it as JSONB.

## Parameters / Member Variables
- `jb`: The input JSONB value from which to extract a field
- `key`: The text key name to search for in the JSONB object

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P: Retrieves JSONB argument from function call
  - PG_GETARG_TEXT_PP: Retrieves text argument from function call
  - JB_ROOT_IS_OBJECT: Macro to check if JSONB root is an object type
  - [getKeyJsonValueFromContainer](../g/getKeyJsonValueFromContainer.md): Searches for a key in JSONB container and returns the value
  - jbvNull: JSONB value type constant for null values
  - [JsonbValueAsText](../J/JsonbValueAsText.md): Converts JsonbValue to text representation
  - PG_RETURN_TEXT_P: Returns text result from function
  - PG_RETURN_NULL: Returns NULL result from function
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- This function is the backend implementation for the JSONB ->> operator in SQL
- Returns NULL if the input is not a JSONB object, if the specified key is not found, or if the value is JSON null
- Unlike jsonb_object_field which returns JSONB format, this returns plain text representation
- Handles JSON null values explicitly by checking v->type != jbvNull before conversion
- The function handles both regular and short-header text values through VARDATA_ANY and VARSIZE_ANY_EXHDR macros
- Located in src/backend/utils/adt/jsonfuncs.c:898-919