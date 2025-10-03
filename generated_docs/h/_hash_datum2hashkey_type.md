# _hash_datum2hashkey_type

## Location
[src/backend/access/hash/hashutil.c:102-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L102-L124)

## Overview
Function that converts a Datum value of a specified type to a hash key, designed for cross-type situations where the data type differs from the index column type.

## Definition
```c
uint32 _hash_datum2hashkey_type(Relation rel, Datum key, Oid keytype)
```

## Detailed Description
This function provides a more flexible alternative to `_hash_datum2hashkey` by allowing the caller to specify the data type of the input Datum. It dynamically looks up the appropriate hash function for the given type within the index's operator family, rather than using the pre-cached hash function. This makes it suitable for cross-type comparisons where the search key type may differ from the indexed column type.

The function is more expensive than `_hash_datum2hashkey` because it must perform a dynamic lookup of the hash function using `get_opfamily_proc`. It includes error handling to ensure that a valid hash function exists for the specified type within the index's operator family. Like its simpler counterpart, it assumes single-attribute indexes.

## Parameters / Member Variables
- `rel`: Relation pointer to the hash index relation containing operator family information
- `key`: Datum value to be hashed
- `keytype`: Oid specifying the data type of the key parameter

## Dependencies
- Functions called/Symbols referenced:
  - [get_opfamily_proc](../g/get_opfamily_proc.md) (looks up hash function for the specified type in operator family)
  - HASHSTANDARD_PROC (constant identifying the primary hash function)
  - RegProcedureIsValid (validates that a hash function was found)
  - [OidFunctionCall1Coll](../O/OidFunctionCall1Coll.md) (calls the hash function by OID with collation support)
  - [DatumGetUInt32](../D/DatumGetUInt32.md) (extracts uint32 result from function call)
  - RelationGetRelationName (gets relation name for error messages)
- Called from (representative examples):
  - [_hash_first](_hash_first.md) (in hashsearch.c at line 342)

## Notes and Other Information
- More expensive than `_hash_datum2hashkey` due to dynamic function lookup
- Designed specifically for cross-type situations in hash indexes
- Includes comprehensive error handling for missing hash functions
- Currently assumes single-attribute indexes (XXX comment indicates this limitation)
- Supports collation-aware hashing through OidFunctionCall1Coll
- Part of PostgreSQL's operator family system for supporting cross-type operations

## Simplified Source

```c
uint32 _hash_datum2hashkey_type(Relation rel, Datum key, Oid keytype)
{
    RegProcedure hash_proc;
    Oid collation;

    // Look up hash function for the specified type in operator family
    hash_proc = get_opfamily_proc(rel->rd_opfamily[0], keytype, keytype, HASHSTANDARD_PROC);

    // Ensure hash function exists for this type
    if (!RegProcedureIsValid(hash_proc))
        elog(ERROR, "missing support function %d(%u,%u) for index \"%s\"",
             HASHSTANDARD_PROC, keytype, keytype, RelationGetRelationName(rel));

    collation = rel->rd_indcollation[0];

    // Call the hash function and return the 32-bit hash value
    return DatumGetUInt32(OidFunctionCall1Coll(hash_proc, collation, key));
}
```