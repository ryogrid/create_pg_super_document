# JsonbHashScalarValue

## Location
src/backend/utils/adt/jsonb_util.c: 1322 - 1364

## Overview
JsonbHashScalarValue computes a hash value for a PostgreSQL JSONB scalar value and mixes it into an existing hash, used primarily in JSONB indexing operations.

## Definition


## Detailed Description
This function generates hash values for JSONB scalar values (null, string, numeric, boolean) and combines them with an existing hash value using left rotation and XOR operations. The function is critical for JSONB GIN indexing and hash-based operations, ensuring that equivalent scalar values produce consistent hash codes. Each JSONB scalar type has a specific hash computation strategy: null values get a constant hash (0x01), strings use hash_any, numerics use hash_numeric to ensure mathematically equivalent values hash equally, and booleans get distinct constants (0x02 for true, 0x04 for false).

## Parameters / Member Variables
- : Pointer to the JsonbValue scalar to be hashed (must be a scalar type)
- hash: hash table empty: Pointer to existing hash value that will be modified by combining with the scalar's hash

## Dependencies
- Functions called/Symbols referenced:
  - hash_any (for string values)
  - DatumGetUInt32 (for datum conversion)
  - DirectFunctionCall1 (for calling hash_numeric)
  - hash_numeric (for numeric values)
  - NumericGetDatum (for numeric datum conversion)
  - pg_rotate_left32 (for hash combination)
- Called from (representative examples):
  - jsonb_hash
  - gin_extract_jsonb_path
  - jsonb_path_ops__add_path_item
  - jsonb_path_ops__extract_nodes

## Notes and Other Information
The function uses a left-rotate-then-XOR strategy for combining hash values, which provides good hash distribution properties. Callers may independently XOR in JB_FOBJECT and JB_FARRAY flags as needed. The function will throw an ERROR for invalid JSONB scalar types, making it safe to use with validated JsonbValue structures.