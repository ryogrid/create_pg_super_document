# _hash_datum2hashkey

## Location
[src/backend/access/hash/hashutil.c:82-101](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashutil.c#L82-L101)

## Overview
Function that converts a Datum value to a hash key by calling the index's hash function for the given data type.

## Definition
```c
uint32 _hash_datum2hashkey(Relation rel, Datum key)
```

## Detailed Description
This function serves as a wrapper that applies the appropriate hash function to a given Datum value to produce a 32-bit hash key. It retrieves the hash function associated with the index relation and applies it to the input data. The function assumes the index has only one attribute (single-column index) and uses the "primary" hash function tracked by PostgreSQL's generic index code.

The function extracts the hash function information from the index relation's metadata and applies it with the proper collation settings. It's a key component in the hash index implementation that bridges between high-level Datum values and low-level hash keys used for bucket selection.

## Parameters / Member Variables
- `rel`: Relation pointer to the hash index relation containing hash function metadata
- `key`: Datum value to be hashed, assumed to be of the index's column type

## Dependencies
- Functions called/Symbols referenced:
  - [index_getprocinfo](../i/index_getprocinfo.md) (retrieves hash function information from index metadata)
  - HASHSTANDARD_PROC (constant identifying the primary hash function)
  - [FunctionCall1Coll](../F/FunctionCall1Coll.md) (calls the hash function with collation support)
  - [DatumGetUInt32](../D/DatumGetUInt32.md) (extracts uint32 result from function call)
- Called from (representative examples):
  - [_hash_first](_hash_first.md) (in hashsearch.c at line 340)
  - _hash_convert_tuple (in hashutil.c at line 331)

## Notes and Other Information
- Currently assumes single-attribute indexes (XXX comment indicates this limitation)
- Uses the "primary" hash function registered with the index
- Supports collation-aware hashing through FunctionCall1Coll
- Returns a 32-bit unsigned integer hash value
- Part of the hash index access method's core functionality for converting data values to hash keys