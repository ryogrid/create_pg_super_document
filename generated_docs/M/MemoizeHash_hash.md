# MemoizeHash_hash

## Location
[src/backend/executor/nodeMemoize.c:158-220](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L158-L220)

## Overview
Hash function for the simplehash hashtable used in the Memoize executor node, which computes hash values for cache key lookups based on the probeslot contents.

## Definition

```c
static uint32
MemoizeHash_hash(struct memoize_hash *tb, const MemoizeKey *key)
```
## Detailed Description
This function computes a hash value for memoize cache lookups. It operates on the MemoizeState's probeslot rather than the provided key parameter (which is unused). The function supports two modes:

1. **Binary mode**: Uses  for fast binary hashing of attribute values
2. **Standard mode**: Uses type-specific hash functions with collation support

The function combines hash values from multiple key attributes by rotating the accumulated hash left by 1 bit and XORing with each attribute's hash. NULL values are treated as having a hash value of 0. The final result is processed through  for better distribution.

## Parameters / Member Variables
- : Pointer to the memoize hash table structure containing private data
- : MemoizeKey pointer (unused - function uses probeslot instead)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoizeKey](MemoizeKey.md)
  - [MemoizeState](MemoizeState.md)  
  - [pg_rotate_left32](../p/pg_rotate_left32.md)
  - [datum_image_hash](../d/datum_image_hash.md)
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md)
  - [DatumGetUInt32](../D/DatumGetUInt32.md)
  - [murmurhash32](../m/murmurhash32.md)
- Called from (representative examples):
  - SH_DECLARE (hash table declaration)
  - SH_HASH_KEY (hash table key hashing macro)

## Notes and Other Information
- The key parameter is intentionally unused; all lookups must first populate the MemoizeState's probeslot
- Uses memory context switching to ecxt_per_tuple_memory for temporary allocations
- Binary mode provides better performance for types that support binary hashing
- The rotating XOR combination helps distribute hash values across multiple key attributes
- Final murmurhash32 step improves hash distribution quality