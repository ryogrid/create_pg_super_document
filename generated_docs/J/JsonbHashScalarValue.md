# JsonbHashScalarValue

## Location
[src/backend/utils/adt/jsonb_util.c:1322-1364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1322-L1364)

## Overview
JsonbHashScalarValue computes a hash value for a PostgreSQL JSONB scalar value and mixes it into an existing hash, used primarily in JSONB indexing operations.

## Definition

```c
void
JsonbHashScalarValue(const JsonbValue *scalarVal, uint32 *hash)
```
## Detailed Description
This function generates hash values for JSONB scalar values (null, string, numeric, boolean) and combines them with an existing hash value using left rotation and XOR operations. The function is critical for JSONB GIN indexing and hash-based operations, ensuring that equivalent scalar values produce consistent hash codes. Each JSONB scalar type has a specific hash computation strategy: null values get a constant hash (0x01), strings use hash_any, numerics use hash_numeric to ensure mathematically equivalent values hash equally, and booleans get distinct constants (0x02 for true, 0x04 for false).

## Parameters / Member Variables
- : Pointer to the JsonbValue scalar to be hashed (must be a scalar type)
- hash: hash table empty: Pointer to existing hash value that will be modified by combining with the scalar's hash

## Dependencies
- Functions called/Symbols referenced:
  - [hash_any](../h/hash_any.md) (for string values)
  - [DatumGetUInt32](../D/DatumGetUInt32.md) (for datum conversion)
  - DirectFunctionCall1 (for calling hash_numeric)
  - [hash_numeric](../h/hash_numeric.md) (for numeric values)
  - [NumericGetDatum](../N/NumericGetDatum.md) (for numeric datum conversion)
  - [pg_rotate_left32](../p/pg_rotate_left32.md) (for hash combination)
- Called from (representative examples):
  - [jsonb_hash](../j/jsonb_hash.md)
  - [gin_extract_jsonb_path](../g/gin_extract_jsonb_path.md)
  - [jsonb_path_ops__add_path_item](../j/jsonb_path_ops__add_path_item.md)
  - [jsonb_path_ops__extract_nodes](../j/jsonb_path_ops__extract_nodes.md)

## Notes and Other Information
The function uses a left-rotate-then-XOR strategy for combining hash values, which provides good hash distribution properties. Callers may independently XOR in JB_FOBJECT and JB_FARRAY flags as needed. The function will throw an ERROR for invalid JSONB scalar types, making it safe to use with validated JsonbValue structures.

## Simplified Source

```c
void JsonbHashScalarValue(const JsonbValue *scalarVal, uint32 *hash) {
    uint32 tmp;

    // Compute hash based on scalar type
    switch (scalarVal->type) {
        case jbvNull:
            tmp = 0x01;
            break;
        case jbvString:
            tmp = DatumGetUInt32(hash_any(
                (const unsigned char *) scalarVal->val.string.val,
                scalarVal->val.string.len));
            break;
        case jbvNumeric:
            // Ensure equal numerics hash to same value
            tmp = DatumGetUInt32(DirectFunctionCall1(hash_numeric,
                NumericGetDatum(scalarVal->val.numeric)));
            break;
        case jbvBool:
            tmp = scalarVal->val.boolean ? 0x02 : 0x04;
            break;
        default:
            elog(ERROR, "invalid jsonb scalar type");
            tmp = 0;
            break;
    }

    // Combine hash using left-rotate and XOR
    *hash = pg_rotate_left32(*hash, 1);
    *hash ^= tmp;
}
```