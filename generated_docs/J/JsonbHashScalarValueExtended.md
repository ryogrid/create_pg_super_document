# JsonbHashScalarValueExtended

## Location
[src/backend/utils/adt/jsonb_util.c:1365-1406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb_util.c#L1365-L1406)

## Overview
JsonbHashScalarValueExtended computes a 64-bit hash value for a PostgreSQL JSONB scalar value with a seed, providing extended hash functionality for better hash distribution.

## Definition

```c
void
JsonbHashScalarValueExtended(const JsonbValue *scalarVal, uint64 *hash,
							 uint64 seed)
```
## Detailed Description
This function is an extended version of JsonbHashScalarValue that produces 64-bit hash values instead of 32-bit ones and incorporates a seed value for better hash distribution. It handles the same JSONB scalar types (null, string, numeric, boolean) but uses extended hash functions that support seeding. For null values, it adds the seed to a constant base value (0x01). String and numeric values use their respective extended hash functions with the provided seed. Boolean values have conditional behavior: when a seed is provided, they use hashcharextended; otherwise, they fall back to simple constants like the non-extended version.

## Parameters / Member Variables
- : Pointer to the JsonbValue scalar to be hashed (must be a scalar type)
- hash: hash table empty: Pointer to existing 64-bit hash value that will be modified
- : 64-bit seed value for hash computation to improve distribution

## Dependencies
- Functions called/Symbols referenced:
  - [hash_any_extended](../h/hash_any_extended.md) (for string values with seed)
  - [DatumGetUInt64](../D/DatumGetUInt64.md) (for 64-bit datum conversion)
  - DirectFunctionCall2 (for calling extended hash functions)
  - [hash_numeric_extended](../h/hash_numeric_extended.md) (for numeric values with seed)
  - [NumericGetDatum](../N/NumericGetDatum.md) (for numeric datum conversion)
  - [UInt64GetDatum](../U/UInt64GetDatum.md) (for 64-bit datum conversion)
  - [hashcharextended](../h/hashcharextended.md) (for boolean values with seed)
  - [BoolGetDatum](../B/BoolGetDatum.md) (for boolean datum conversion)
  - ROTATE_HIGH_AND_LOW_32BITS (for hash combination)
- Called from (representative examples):
  - [jsonb_hash_extended](../j/jsonb_hash_extended.md)

## Notes and Other Information
The function uses ROTATE_HIGH_AND_LOW_32BITS macro for hash combination instead of the simple left rotation used in the 32-bit version. This provides better mixing properties for 64-bit hash values. The seed parameter allows for creating different hash families, which is useful for hash table resizing and reducing hash collisions. Boolean handling is more sophisticated than the basic version, using the extended hash infrastructure when a seed is present.