# index_register

## Location
[src/backend/bootstrap/bootstrap.c:901-950](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L901-L950)

## Overview
A function that records index definitions during PostgreSQL bootstrap for deferred construction, implementing a two-phase index creation strategy.

## Definition

```c
void
index_register(Oid heap,
			   Oid ind,
			   const IndexInfo *indexInfo)
```
## Detailed Description
This function implements a deferred index construction mechanism during PostgreSQL bootstrap. Rather than building indexes immediately when they are defined, it records the index specifications in a linked list (ILHead) for later construction. This two-phase approach is necessary because:

1. Indexes themselves have catalog entries that must be included in system catalog indexes
2. All catalog entries must exist before indexes can be properly constructed  
3. Building indexes immediately would create circular dependencies

The function creates a deep copy of the IndexInfo structure and stores it in a special no-garbage-collection memory context to ensure the index definitions persist until the actual index construction phase. The deferred indexes are built just before bootstrap completion when all catalog entries are finalized.

## Parameters / Member Variables
- : The OID of the heap relation that the index will be built on
- : The OID of the index relation being registered
- : Pointer to IndexInfo structure containing index specification details

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (creates memory context for persistent storage)
  - [palloc](../p/palloc.md) (allocates memory for IndexList and IndexInfo copies)
  - memcpy (copies IndexInfo structure)
  - copyObject (deep copies expressions and predicates)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (switches memory contexts)
  - ALLOCSET_DEFAULT_SIZES (default memory context configuration)
  - IndexList (linked list structure for storing index registrations)
  - IndexInfo (structure containing index specification)

- Called from:
  - index_create (during index creation in catalog layer)

## Notes and Other Information
- Uses a special 'BootstrapNoGC' memory context to prevent premature garbage collection
- Performs deep copying of expressions and predicates to ensure data persistence
- Asserts that exclusion constraints are not present during bootstrap (not supported)
- Maintains a global linked list (ILHead) of deferred index constructions
- Critical for resolving circular dependencies in system catalog index creation
- The two-phase approach ensures all catalog entries exist before index construction
- Index expressions and predicates are copied but expression states are reset to NIL/NULL