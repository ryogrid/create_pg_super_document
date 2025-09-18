# SharedRecordTableEntry

## Location
src/backend/utils/cache/typcache.c: 196 - 199

## Overview
SharedRecordTableEntry is a structure used in PostgreSQL's type cache system to represent shared record table entries that enable lookup of typmod values using TupleDesc structures that may reside in either local or shared memory.

## Definition


## Detailed Description
SharedRecordTableEntry serves as the shared version of RecordCacheEntry in PostgreSQL's type cache implementation. This structure is specifically designed to facilitate typmod lookups using TupleDesc structures, regardless of whether those TupleDesc structures are stored in local backend memory or shared memory across multiple backends. The structure acts as a hash table entry that enables efficient sharing of record type information between different database processes.

The primary purpose of this structure is to support the shared record typmod registry, which allows multiple PostgreSQL backends to share record type metadata efficiently without duplicating information in each backend's local memory.

## Parameters / Member Variables
- : A SharedRecordTableKey structure that can hold either a local TupleDesc or a pointer to a shared TupleDesc, along with a flag indicating which type it contains

## Dependencies
- Functions called/Symbols referenced:
  - SharedRecordTableKey
- Called from (representative examples):
  - shared_record_table_hash
  - SharedRecordTypmodRegistryInit
  - find_or_make_matching_shared_tupledesc

## Notes and Other Information
- Located in src/backend/utils/cache/typcache.c:196-199
- This structure is part of PostgreSQL's shared memory optimization for type cache management
- The design allows for efficient hash table operations on record types regardless of memory location
- Used extensively in the shared record typmod registry system for cross-backend type information sharing