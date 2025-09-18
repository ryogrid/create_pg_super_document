# StoreSingleInheritance

## Location
[src/backend/catalog/pg_inherits.c:508-551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_inherits.c#L508-L551)

## Overview
Creates a single entry in the pg_inherits system catalog to record an inheritance relationship between two relations.

## Definition
```c
void StoreSingleInheritance(Oid relationId, Oid parentOid, int32 seqNumber)
```

## Detailed Description
This function creates a single row in the pg_inherits catalog table to establish an inheritance relationship between a child relation and its parent. The pg_inherits catalog tracks all inheritance relationships in PostgreSQL, including table inheritance and partitioning relationships.

The function performs these steps:
1. Opens the pg_inherits catalog relation with RowExclusiveLock
2. Constructs a tuple with the inheritance relationship data
3. Inserts the tuple into the catalog using CatalogTupleInsert
4. Cleans up allocated memory and closes the relation

The function sets inhdetachpending to false, indicating this is not a pending detach operation for partition management.

## Parameters / Member Variables
- `relationId`: OID of the child relation that inherits from the parent
- `parentOid`: OID of the parent relation being inherited from  
- `seqNumber`: Sequence number indicating the order of inheritance (used for multiple inheritance)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates a heap tuple from the provided values
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md): Inserts the tuple into the system catalog
  - [heap_freetuple](../h/heap_freetuple.md): Frees memory allocated for the heap tuple
- Called from (representative examples):
  - index_create: When creating inheritance relationships for indexes
  - index_concurrently_swap: During concurrent index operations
  - [IndexSetParentIndex](../I/IndexSetParentIndex.md): When setting up index inheritance
  - [StoreCatalogInheritance1](StoreCatalogInheritance1.md): Higher-level inheritance storage function

## Notes and Other Information
- This is a low-level function for creating individual inheritance entries
- Always sets inhdetachpending to false - for partition detach operations, other functions handle setting this flag
- Uses RowExclusiveLock on pg_inherits to ensure consistency during concurrent operations
- The seqNumber parameter is important for multiple inheritance scenarios where order matters
- Part of the internal catalog management system, not typically called directly by user code
- Location: src/backend/catalog/pg_inherits.c:508-551