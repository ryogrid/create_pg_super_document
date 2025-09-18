# jsonb_object_field

## Location
src/backend/utils/adt/jsonfuncs.c: 860 - 881

## Overview
Extracts a field from a JSONB object by key name and returns the field value as a JSONB value, or NULL if the key is not found or the input is not an object.

## Definition
```c
Datum jsonb_object_field(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the JSONB object field access operator (->). It takes a JSONB value and a text key as input parameters, searches for the specified key within the JSONB object, and returns the corresponding value as a JSONB datum. The function performs type checking to ensure the input JSONB is an object before attempting field extraction. If the input is not an object or the key doesn't exist, the function returns NULL.

The function uses PostgreSQL's function call interface, accessing arguments through PG_GETARG_* macros and returning results through PG_RETURN_* macros.

## Parameters / Member Variables
- `jb`: The input JSONB value from which to extract a field
- `key`: The text key name to search for in the JSONB object

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_JSONB_P: Retrieves JSONB argument from function call
  - PG_GETARG_TEXT_PP: Retrieves text argument from function call  
  - JB_ROOT_IS_OBJECT: Macro to check if JSONB root is an object type
  - getKeyJsonValueFromContainer: Searches for a key in JSONB container and returns the value
  - JsonbValueToJsonb: Converts JsonbValue to Jsonb format
  - PG_RETURN_JSONB_P: Returns JSONB result from function
  - PG_RETURN_NULL: Returns NULL result from function
- Called from (representative examples):
  - No direct references found (likely called through SQL function interface)

## Notes and Other Information
- This function is the backend implementation for the JSONB -> operator in SQL
- Returns NULL if the input is not a JSONB object or if the specified key is not found
- The function handles both regular and short-header text values through VARDATA_ANY and VARSIZE_ANY_EXHDR macros
- Located in src/backend/utils/adt/jsonfuncs.c:860-881