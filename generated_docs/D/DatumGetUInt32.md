# DatumGetUInt32

## Location
[src/include/postgres.h:222-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postgres.h#L222-L231)

## Overview
Extracts a 32-bit unsigned integer value from PostgreSQL's internal Datum representation, providing type-safe conversion from Datum to uint32.

## Definition

```c
static inline uint32
DatumGetUInt32(Datum X)
```
## Detailed Description
DatumGetUInt32 is a static inline function that converts a PostgreSQL Datum value back to a 32-bit unsigned integer (uint32). This function serves as the counterpart to UInt32GetDatum, providing the reverse conversion from PostgreSQL's universal Datum type to a native C unsigned integer type. The function performs a simple cast operation, which is safe because 32-bit unsigned integers are stored directly within the Datum value without additional encoding or indirection.

## Parameters / Member Variables
- : The Datum value to be converted to a 32-bit unsigned integer

## Dependencies
- Functions called/Symbols referenced:
  - (None - simple cast operation)
- Called from (representative examples):
  - [_hash_datum2hashkey](../h/_hash_datum2hashkey.md) (src/backend/access/hash/hashutil.c:91)
  - [notification_hash](../n/notification_hash.md) (src/backend/commands/async.c:2363)
  - [ExecHashGetHashValue](../E/ExecHashGetHashValue.md) (src/backend/executor/nodeHash.c:1898)
  - [hash_array](../h/hash_array.md) (src/backend/utils/adt/arrayfuncs.c:4249)
  - [JsonbHashScalarValue](../J/JsonbHashScalarValue.md) (src/backend/utils/adt/jsonb_util.c:1333, 1338)
  - PG_GETARG_UINT32 macro (src/include/fmgr.h:270)

## Notes and Other Information
- This is a static inline function defined in src/include/postgres.h, making it widely available throughout PostgreSQL
- Extensively used in hash functions and operations that require 32-bit unsigned integer values
- Critical for hash table operations, indexing, and data structure implementations that rely on unsigned integer keys
- Often used in conjunction with PG_GETARG_UINT32 macro for extracting function arguments
- The function assumes the Datum contains a valid 32-bit unsigned integer value - no validation is performed
- Particularly important in hash-based algorithms, bloom filters, and various data type hash functions throughout PostgreSQL
- Performance-critical as it's used in hot code paths like hashing and comparison operations

## Simplified Source

```c
static inline uint32 DatumGetUInt32(Datum X) {
    // Simple cast: Extract 32-bit unsigned integer from Datum
    return (uint32) X;
}
```