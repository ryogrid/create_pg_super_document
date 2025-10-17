# getJsonPathVariableFromJsonb

## Location
[src/backend/utils/adt/jsonpath_exec.c:3173-3202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3173-L3202)

## Overview
A callback function implementation that retrieves JSON path variables from a JSONB object by treating the JSONB as a variable store.

## Definition
static JsonbValue *getJsonPathVariableFromJsonb(void *varsJsonb, char *varName, int varNameLength, JsonbValue *baseObject, int *baseObjectId)

## Detailed Description
This function serves as a JsonPathGetVarCallback implementation specifically designed for cases where variables are stored in a JSONB object. It searches for a variable by name within the JSONB container, treating the JSONB as a key-value store where variable names are keys. If the variable is found, it sets up the base object context and returns the variable value. This enables JSON path expressions to reference variables stored in JSONB format, which is useful for dynamic parameterized queries where variables are passed as JSONB objects.

## Parameters / Member Variables
- varsJsonb: Void pointer to the JSONB object containing the variables (cast to Jsonb internally)
- varName: Null-terminated string containing the variable name to look up
- varNameLength: Length of the variable name string
- baseObject: Pointer to JsonbValue where the base object will be stored if variable is found
- baseObjectId: Pointer to integer where the base object ID will be stored (-1 if not found, 1 if found)

## Dependencies
- Functions called/Symbols referenced:
  - [findJsonbValueFromContainer](../f/findJsonbValueFromContainer.md) (searches for value in JSONB container)
  - [JsonbInitBinary](../J/JsonbInitBinary.md) (initializes JsonbValue from binary JSONB)
  - jbvString (JsonbValue type constant for string values)
  - JB_FOBJECT (JSONB container type flag for objects)
- Called from (representative examples):
  - [jsonb_path_exists_internal](../j/jsonb_path_exists_internal.md)
  - [jsonb_path_match_internal](../j/jsonb_path_match_internal.md)
  - [jsonb_path_query_internal](../j/jsonb_path_query_internal.md)
  - [jsonb_path_query_array_internal](../j/jsonb_path_query_array_internal.md)
  - [jsonb_path_query_first_internal](../j/jsonb_path_query_first_internal.md)

## Notes and Other Information
- This is a static callback function, only accessible within the jsonpath_exec.c module
- Implements the JsonPathGetVarCallback interface for JSONB-based variable storage
- Creates a temporary JsonbValue structure to perform the variable name lookup
- Returns NULL if the variable is not found in the JSONB object
- Sets baseObjectId to 1 when variable is found, -1 when not found
- Part of PostgreSQL's JSON path variable resolution system for JSONB-stored variables

## Simplified Source

```c
static JsonbValue *
getJsonPathVariableFromJsonb(void *varsJsonb, char *varName, int varNameLength,
                            JsonbValue *baseObject, int *baseObjectId)
{
    Jsonb *vars = varsJsonb;
    JsonbValue tmp;
    JsonbValue *result;

    // Create a string value for the variable name lookup
    tmp.type = jbvString;
    tmp.val.string.val = varName;
    tmp.val.string.len = varNameLength;

    // Search for the variable in the JSONB object
    result = findJsonbValueFromContainer(&vars->root, JB_FOBJECT, &tmp);

    if (result == NULL) {
        // Variable not found
        *baseObjectId = -1;
        return NULL;
    }

    // Variable found - set up base object context
    *baseObjectId = 1;
    JsonbInitBinary(baseObject, vars);

    return result;
}
```