# RecordCacheEntry

## Location
src/backend/utils/cache/typcache.c: 157 - 160

## Overview
RecordCacheEntry is a simple structure that stores cached tuple descriptors for non-anonymous record types in PostgreSQL.

## Definition
```c
typedef struct RecordCacheEntry
{
    TupleDesc   tupdesc;
} RecordCacheEntry;
```

## Detailed Description
RecordCacheEntry is used as part of PostgreSQL's record type caching system to store definitions of non-anonymous record types. The cache ensures that once a record type is defined, it will be remembered for the life of the backend process, allowing subsequent uses of the same record type (determined by equalRowTypes) to reference the existing cached entry rather than recreating it.

The caching system uses a linear array of TupleDescs that can be quickly indexed using assigned typmod values, along with a hash table to speed up searches for matching TupleDescs. This dual-indexing approach provides both fast direct access via typmod and efficient searching for equivalent record types.

## Parameters / Member Variables
- `tupdesc`: A TupleDesc structure that contains the complete definition and metadata for the record type, including column names, types, and other attributes

## Dependencies
- Functions called/Symbols referenced:
  - [TupleDesc](../T/TupleDesc.md) (PostgreSQL tuple descriptor structure)
- Called from (representative examples):
  - [record_type_typmod_hash](../r/record_type_typmod_hash.md)
  - [record_type_typmod_compare](../r/record_type_typmod_compare.md)  
  - [assign_record_type_typmod](../a/assign_record_type_typmod.md)

## Notes and Other Information
- Part of PostgreSQL's type cache system located in src/backend/utils/cache/typcache.c
- Used exclusively for non-anonymous record types - anonymous records are handled differently
- The cache persists for the entire lifetime of the backend process
- Enables efficient reuse of record type definitions by avoiding redundant parsing and validation
- Works in conjunction with a hash table and linear array indexing system for optimal performance
- The typmod assignment system allows for O(1) access to cached record types once the typmod is known