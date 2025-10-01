# JsonbInitBinary

## Location
[src/backend/utils/adt/jsonpath_exec.c:3601-3613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3601-L3613)

## Overview
JsonbInitBinary is a static utility function that initializes a JsonbValue structure to represent binary JSONB data by setting up references to an existing Jsonb container.

## Definition
static JsonbValue *JsonbInitBinary(JsonbValue *jbv, Jsonb *jb)

## Detailed Description
This function initializes a JsonbValue structure to wrap an existing Jsonb container in binary format. It sets the JsonbValue type to jbvBinary and establishes pointers to the binary data within the Jsonb structure. The function provides a lightweight wrapper that allows existing JSONB binary data to be used within the JsonbValue framework without copying the actual data. This is particularly useful when working with stored JSONB values that need to be processed through the JSON path execution engine.

## Parameters / Member Variables
- `jbv`: Pointer to the JsonbValue structure to be initialized
- `jb`: Pointer to the existing Jsonb container that contains the binary data to be referenced

## Dependencies
- Functions called/Symbols referenced:
  - Jsonb (PostgreSQL JSONB container type)
  - jbvBinary (JsonbValue type enum value for binary data)
  - VARSIZE_ANY_EXHDR (PostgreSQL macro to get size excluding header)
- Called from (representative examples):
  - [executeJsonPath](../e/executeJsonPath.md)
  - [executeKeyValueMethod](../e/executeKeyValueMethod.md)
  - [JsonItemFromDatum](JsonItemFromDatum.md)
  - [getJsonPathVariableFromJsonb](../g/getJsonPathVariableFromJsonb.md)

## Notes and Other Information
- This is a static function internal to jsonpath_exec.c, not exposed in the public API
- The function does not allocate new memory or copy data; it only sets up pointers to existing data
- Sets jbv->type to jbvBinary to indicate this JsonbValue contains binary JSONB data
- Uses VARSIZE_ANY_EXHDR to calculate the length of the binary data excluding the varlena header
- The function returns the same JsonbValue pointer that was passed in, allowing for convenient chaining
- Critical for interfacing between PostgreSQL's stored JSONB format and the JSON path execution engine

## Simplified Source

```c
static JsonbValue *JsonbInitBinary(JsonbValue *jbv, Jsonb *jb) {
    // Initialize JsonbValue to wrap existing JSONB binary data
    jbv->type = jbvBinary;
    jbv->val.binary.data = &jb->root;
    jbv->val.binary.len = VARSIZE_ANY_EXHDR(jb);

    return jbv;
}
```