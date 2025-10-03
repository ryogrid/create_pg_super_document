# table_relation_set_new_filelocator

## Location
[src/include/access/tableam.h:1622-1639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/tableam.h#L1622-L1639)

## Overview
A table access method (tableam) wrapper function that creates new storage for a relation with a new filelocator, used during relation creation and DDL operations that need to establish fresh storage.

## Definition

```c
static inline void
table_relation_set_new_filelocator(Relation rel,
								   const RelFileLocator *newrlocator,
								   char persistence,
								   TransactionId *freezeXid,
								   MultiXactId *minmulti)
```
## Detailed Description
This function is a high-level interface to the table access method layer for creating new storage for an existing relation. It delegates to the table access method's  implementation to handle the storage-specific details of creating new files and setting up the storage structure.

The function is designed to be called before updating the relcache entry when creating new storage for an existing relation. This ensures proper ordering of operations during DDL commands that require new storage allocation. The function also handles the establishment of transaction visibility horizons for the new storage.

## Parameters / Member Variables
- `rel`: The relation for which new storage is being created
- `*newrlocator`: Pointer to the new RelFileLocator that specifies the new storage location and properties
- `persistence`: Character indicating the persistence level ('p' for permanent, 't' for temporary, 'u' for unlogged)
- `*freezeXid`: Output parameter set to the transaction ID horizon that should be recorded in pg_class.relfrozenxid
- `*minmulti`: Output parameter set to the MultiXactId horizon that should be recorded in pg_class.relminmxid
## Dependencies
- Functions called/Symbols referenced:
  - rel->rd_tableam->relation_set_new_filelocator (table access method implementation)
  - MultiXactId (transaction management type)
- Called from (representative examples):
  - [heap_create](../h/heap_create.md) (during table creation)
  - [RelationSetNewRelfilenumber](../R/RelationSetNewRelfilenumber.md) (during relation file replacement)

## Notes and Other Information
- This is an inline wrapper function that provides a consistent interface across different table access methods
- The function must be called before updating relcache entries to maintain proper ordering
- The freezeXid and minmulti output parameters are critical for maintaining MVCC consistency in the new storage
- Part of the DDL-related functionality in the table access method framework

## Simplified Source

```c
static inline void
table_relation_set_new_filelocator(Relation rel,
                                   const RelFileLocator *newrlocator,
                                   char persistence,
                                   TransactionId *freezeXid,
                                   MultiXactId *minmulti)
{
    // Delegate to table access method implementation
    rel->rd_tableam->relation_set_new_filelocator(rel, newrlocator,
                                                   persistence, freezeXid,
                                                   minmulti);
}
```