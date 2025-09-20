# relationHasPrimaryKey

## Location
[src/backend/catalog/index.c:147-200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L147-L200)

## Overview
Determines whether an existing relation has a primary key index by searching through all indexes associated with the relation.

## Definition

```c
static bool
relationHasPrimaryKey(Relation rel)
```
## Detailed Description
This function checks if a relation (table) has a primary key by examining all indexes associated with the relation. It retrieves the list of index OIDs from the relation cache and searches through each index in the pg_index system catalog to find one marked as a primary key. The function intentionally does not check the indisvalid flag, allowing it to detect primary key constraints even if the associated index is currently invalid. This behavior is important for enforcing the rule that only one primary key index can exist per table.

## Parameters / Member Variables
- : A Relation pointer to the table being examined. The caller must hold a suitable lock on this relation.

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexList](../R/RelationGetIndexList.md): Retrieves the list of index OIDs for the relation
  - Form_pg_index: PostgreSQL system catalog structure for index information
  - [SearchSysCache1](../S/SearchSysCache1.md): Searches system cache for index tuple by OID
  - HeapTupleIsValid: Validates heap tuple
  - GETSTRUCT: Macro to extract structure from heap tuple
  - [ReleaseSysCache](../R/ReleaseSysCache.md): Releases system cache entry
  - [list_free](../l/list_free.md): Frees memory allocated for the index OID list
- Called from (representative examples):
  - SerializedReindexState: Used during reindex operations
  - [index_check_primary_key](../i/index_check_primary_key.md): Called when validating primary key constraints

## Notes and Other Information
- The function is marked static, indicating it's only used within the same source file
- Returns false if no primary key is found, true if one exists
- Does not validate the index (ignores indisvalid flag) to maintain constraint that only one primary key can exist
- Uses system cache lookups for efficiency when examining index properties
- Properly handles memory cleanup by freeing the index OID list