# timestamp_hash_extended

## Location
[src/backend/utils/adt/timestamp.c:2315-2324](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/timestamp.c#L2315-L2324)

## Overview
The timestamp_hash_extended function computes an extended hash value for timestamp data types by delegating to the hashint8extended function.

## Definition

```c
Datum
timestamp_hash_extended(PG_FUNCTION_ARGS)
```
## Detailed Description
This function provides extended hash support for timestamp values in PostgreSQL's advanced hashing operations. Similar to timestamp_hash, it leverages the internal 64-bit integer representation of timestamps but uses the extended hashing algorithm provided by hashint8extended. Extended hash functions are used in scenarios requiring higher hash quality or when additional entropy is needed, such as in advanced hash-based algorithms or when hash collision resistance is critical.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Function call information structure containing the timestamp value to be hashed and the seed value for extended hashing
## Dependencies
- Functions called/Symbols referenced:
  - [hashint8extended](../h/hashint8extended.md) (delegates extended hash computation to this function)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is the extended version of timestamp_hash, providing enhanced hash quality through additional randomization
- Uses the same delegation pattern as timestamp_hash but calls hashint8extended instead of hashint8
- Extended hash functions typically accept a seed parameter to provide additional entropy and reduce hash collisions
- Part of PostgreSQL's extended hash function infrastructure for improved hash-based operation performance
- Maintains consistency with the underlying integer representation while providing enhanced hash distribution

## Simplified Source

```c
Datum
timestamp_hash_extended(PG_FUNCTION_ARGS)
{
    // Delegate to 64-bit extended hash function for consistent timestamp hashing
    return hashint8extended(fcinfo);
}
```