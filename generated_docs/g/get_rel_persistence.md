# get_rel_persistence

## Location
[src/backend/utils/cache/lsyscache.c:2078-2099](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L2078-L2099)

## Overview
Returns the persistence attribute (relpersistence) of a given relation, indicating whether the relation is permanent, temporary, or unlogged.

## Definition


## Detailed Description
This function retrieves the persistence attribute of a relation from the PostgreSQL system catalog (pg_class). The persistence attribute determines the storage characteristics and durability properties of the relation:
- 'p' for permanent relations (normal tables)
- 't' for temporary relations 
- 'u' for unlogged relations

The function performs a system cache lookup to efficiently retrieve this information without directly accessing the catalog table.

## Parameters / Member Variables
- : The OID (Object Identifier) of the relation whose persistence attribute is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - elog (error logging)
  - GETSTRUCT (macro to extract structure from tuple)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_class (pg_class tuple structure)
- Called from (representative examples):
  - [index_drop](../i/index_drop.md)
  - [DefineIndex](../D/DefineIndex.md)
  - [ReindexIndex](../R/ReindexIndex.md)
  - [ReindexTable](../R/ReindexTable.md)
  - [ReindexMultipleInternal](../R/ReindexMultipleInternal.md)
  - [RangeVarCallbackForLockTable](../R/RangeVarCallbackForLockTable.md)
  - [set_rel_consider_parallel](../s/set_rel_consider_parallel.md)

## Notes and Other Information
- The function will throw an ERROR if the relation OID is not found in the system catalog
- Uses PostgreSQL's system cache for efficient lookup
- The persistence attribute is crucial for determining relation behavior during crash recovery and logging
- Part of the low-level system cache API (lsyscache.c) that provides convenient access to catalog information