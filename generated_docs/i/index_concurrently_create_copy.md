# index_concurrently_create_copy

## Location
[src/backend/catalog/index.c:1298-1481](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L1298-L1481)

## Overview
index_concurrently_create_copy creates a concurrent copy of an existing index based on its definition, used primarily during concurrent reindex operations.

## Definition

```c
Oid
index_concurrently_create_copy(Relation heapRelation, Oid oldIndexId,
							   Oid tablespaceOid, const char *newName)
```
## Detailed Description
This function creates a new index that is a copy of an existing index, intended for use during concurrent reindex operations. It extracts all necessary metadata from the original index including column definitions, operator classes, collations, expressions, predicates, and options. The new index is created with INDEX_CREATE_SKIP_BUILD and INDEX_CREATE_CONCURRENT flags, meaning the catalog entries are created but the actual index data building is deferred to a later phase.

The function performs thorough metadata extraction from system catalogs (pg_index, pg_class, pg_attribute) rather than relying solely on the IndexInfo structure, as some information like expressions and predicates may have been flattened for planner use. It explicitly prevents creation of indexes with exclusion constraints during concurrent operations.

## Parameters / Member Variables
- : The table relation that the index belongs to
- : Object identifier of the existing index to copy
- : Tablespace where the new index should be created
- : Name for the new index copy

## Dependencies
- Functions called/Symbols referenced:
  - index_open (to access the original index)
  - BuildIndexInfo (to extract index metadata)
  - SearchSysCache1/SearchSysCache2 (for catalog lookups)
  - SysCacheGetAttrNotNull/SysCacheGetAttr (for attribute retrieval)
  - TextDatumGetCString (for text field conversion)
  - stringToNode (for parsing stored expressions and predicates)
  - makeIndexInfo (to construct new IndexInfo structure)
  - get_attoptions (for attribute-specific options)
  - make_ands_implicit (for predicate format conversion)
  - index_create (to create the new index)
  - index_close (for cleanup)
- Called from (representative examples):
  - Concurrent reindex operations

## Notes and Other Information
- Returns the OID of the newly created index copy
- The new index is created but not built (INDEX_CREATE_SKIP_BUILD flag)
- Explicitly rejects indexes with exclusion constraints for concurrent creation
- Extracts complete metadata including expressions, predicates, and per-attribute options
- Creates the index with CONCURRENT flag for proper concurrent reindex handling
- Performs extensive catalog lookups to ensure complete metadata transfer
- The function is specifically designed for the concurrent reindex workflow
- Located at src/backend/catalog/index.c:1298-1481