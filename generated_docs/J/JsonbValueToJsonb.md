# JsonbValueToJsonb

## Location
[src/backend/utils/adt/jsonb_util.c:92-133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L92-L133)

## Overview
Converts an in-memory JsonbValue into a Jsonb structure suitable for on-disk storage, handling different value types including scalars, objects, arrays, and binary data.

## Definition
```c
Jsonb *JsonbValueToJsonb(JsonbValue *val)
```

## Detailed Description
This function performs the inverse operation of JsonbToJsonbValue by converting a JsonbValue (in-memory representation) back to a Jsonb structure (on-disk/wire format). The function handles different types of JsonbValues appropriately:

- **Scalar values**: Wrapped in a special "raw scalar" array structure to maintain the JSONB format requirements
- **Objects and Arrays**: Converted directly using the convertToJsonb function
- **Binary values**: The binary data is copied into a new Jsonb structure with proper variable-length headers

The function is essential for preparing JSONB data for storage or transmission after in-memory processing and manipulation.

## Parameters / Member Variables
- `val`: Input JsonbValue structure to be converted to Jsonb format

## Dependencies
- Functions called/Symbols referenced:
  - IsAJsonbScalar (macro to check if value is scalar)
  - [pushJsonbValue](../p/pushJsonbValue.md) (builds JSONB structures)
  - [convertToJsonb](../c/convertToJsonb.md) (converts JsonbValue to Jsonb)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - SET_VARSIZE (sets variable-length data size)
  - VARDATA (gets variable-length data pointer)
  - memcpy (memory copy function)
- Called from (representative examples):
  - [jsonb_build_object_worker](../j/jsonb_build_object_worker.md)
  - [jsonb_build_array_worker](../j/jsonb_build_array_worker.md)  
  - [jsonb_set](../j/jsonb_set.md)
  - [jsonb_insert](../j/jsonb_insert.md)
  - [jsonb_path_query_internal](../j/jsonb_path_query_internal.md)
  - PG_RETURN_JSONB_P

## Notes and Other Information
- Scalar values require special handling by wrapping them in a "raw scalar" array structure
- The function allocates new memory for the result, so callers are responsible for memory management
- Binary JsonbValues are assumed to already contain valid JSONB binary data
- This is a core conversion function used extensively throughout the JSONB subsystem
- Located in src/backend/utils/adt/jsonb_util.c:92-133

## Simplified Source

```c
Jsonb *JsonbValueToJsonb(JsonbValue *val) {
    Jsonb *out;

    if (IsAJsonbScalar(val)) {
        // Wrap scalar in a raw scalar array structure
        JsonbParseState *pstate = NULL;
        JsonbValue scalarArray;

        scalarArray.type = jbvArray;
        scalarArray.val.array.rawScalar = true;
        scalarArray.val.array.nElems = 1;

        pushJsonbValue(&pstate, WJB_BEGIN_ARRAY, &scalarArray);
        pushJsonbValue(&pstate, WJB_ELEM, val);
        JsonbValue *res = pushJsonbValue(&pstate, WJB_END_ARRAY, NULL);

        out = convertToJsonb(res);
    }
    else if (val->type == jbvObject || val->type == jbvArray) {
        // Convert objects/arrays directly
        out = convertToJsonb(val);
    }
    else {
        // Handle binary data - copy to new Jsonb structure
        Assert(val->type == jbvBinary);
        out = palloc(VARHDRSZ + val->val.binary.len);
        SET_VARSIZE(out, VARHDRSZ + val->val.binary.len);
        memcpy(VARDATA(out), val->val.binary.data, val->val.binary.len);
    }

    return out;
}
```