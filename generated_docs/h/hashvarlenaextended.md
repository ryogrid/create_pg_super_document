# hashvarlenaextended

## Location
[src/backend/access/hash/hashfunc.c:398-410](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashfunc.c#L398-L410)

## Overview
An extended version of hashvarlena that supports seed-based hashing for variable-length data types.

## Definition

```c
struct varlena *key = PG_GETARG_VARLENA_PP(0);
```
## Detailed Description
The hashvarlenaextended function is the extended variant of hashvarlena that accepts an additional seed parameter for hash computation. It provides the same direct hashing capability for variable-length data types where all bits are significant, but allows for seed-based hash value generation. This enables the creation of multiple independent hash functions from the same input data, which is valuable for advanced hash table implementations and algorithms requiring hash function families.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  -  (arg 0): The varlena value to be hashed (struct varlena*)
  -  (arg 1): 64-bit seed value for hash computation (int64)

## Dependencies
- Functions called/Symbols referenced:
  - [varlena](../v/varlena.md)
  - PG_GETARG_VARLENA_PP
  - [hash_any_extended](hash_any_extended.md)
  - PG_GETARG_INT64

- Called from (representative examples):
  - No direct references found

## Notes and Other Information
- Extends hashvarlena with seed-based hashing capability
- Maintains the same direct hashing approach without transformations
- Uses hash_any_extended to incorporate the seed parameter into hash computation
- Suitable for hash table resizing and algorithms requiring multiple hash functions
- Located at src/backend/access/hash/hashfunc.c:398-410

## Simplified Source

```c
Datum hashvarlenaextended(PG_FUNCTION_ARGS) {
    struct varlena *key = PG_GETARG_VARLENA_PP(0);

    // Hash the variable-length data with seed parameter
    Datum result = hash_any_extended((unsigned char *) VARDATA_ANY(key),
                                    VARSIZE_ANY_EXHDR(key),
                                    PG_GETARG_INT64(1));

    // Clean up memory for toasted inputs
    PG_FREE_IF_COPY(key, 0);

    return result;
}
```