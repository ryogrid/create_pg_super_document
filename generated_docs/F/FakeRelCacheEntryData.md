# FakeRelCacheEntryData

## Location
[src/backend/access/transam/xlogutils.c:563-564](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogutils.c#L563-L564)

## Overview
FakeRelCacheEntryData is a struct used to create minimal fake relation cache entries during XLOG replay and WAL-skipped file synchronization when a full relation cache is not available.

## Definition


## Detailed Description
FakeRelCacheEntryData is the actual struct returned by CreateFakeRelcacheEntry(), though the declared return type is Relation. This struct provides a lightweight alternative to full relation cache entries during recovery operations and WAL-skipped file processing.

The struct is designed to support low-level operations like ReadBuffer() by providing only the essential fields related to physical storage. It contains a complete RelationData structure followed by a FormData_pg_class structure that holds the pg_class catalog information.

The design ensures that the RelationData member comes first, allowing the struct to be cast to a Relation pointer safely. This enables the fake entry to be used in contexts that expect a standard relation cache entry while providing minimal overhead.

## Parameters / Member Variables
- : Complete RelationData structure containing all relation metadata including physical identifiers, buffer management info, and various relation properties. Must be the first member to allow safe casting to Relation.
- : FormData_pg_class structure containing the pg_class catalog tuple data, including relation name, owner, access method, storage characteristics, and various boolean flags indicating relation properties.

## Dependencies
- Functions called/Symbols referenced:
  - RelationData (embedded struct)
  - FormData_pg_class (embedded struct)
- Called from (representative examples):
  - [CreateFakeRelcacheEntry](../C/CreateFakeRelcacheEntry.md) (returns pointer to this struct)
  - FakeRelCacheEntry (typedef for pointer to this struct)

## Notes and Other Information
- The struct is allocated as a single memory block in CreateFakeRelcacheEntry() using palloc0()
- Only fields related to physical storage are initialized, making it suitable only for low-level operations
- Used specifically during XLOG replay when the normal relation cache is not available
- Also used for syncing WAL-skipped files where minimal relation information is sufficient
- The fake entry must be freed using FreeFakeRelcacheEntry() to prevent memory leaks
- The RelationData member must be first to ensure proper memory layout for casting operations