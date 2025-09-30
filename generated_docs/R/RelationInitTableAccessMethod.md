# RelationInitTableAccessMethod

## Location
[src/backend/utils/cache/relcache.c:1810-1874](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L1810-L1874)

## Overview
Initializes table access method support for a table-like relation by setting up the appropriate access method handler based on the relation's type and characteristics.

## Definition

```c
struct
	 */
	InitTableAmRoutine(relation);
```
## Detailed Description
This function initializes the table access method for a given relation by determining and setting the appropriate access method handler. It handles three distinct cases:

1. **Sequences**: Even though sequences are stored in pg_class with relam = InvalidOid, they are accessed like heap tables, so the function assigns the heap table access method handler directly.

2. **Catalog Relations**: To avoid expensive syscache lookups during bootstrap and for performance, catalog relations are assumed to use the heap table access method and are assigned the heap handler directly.

3. **Regular Relations**: For all other relations, the function performs a syscache lookup to find the access method information from pg_am and retrieves the handler function OID.

After determining the handler, the function calls InitTableAmRoutine() to fetch and initialize the table access method's API struct.

## Parameters / Member Variables
- : Pointer to the Relation structure that needs table access method initialization

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_am
  - [IsCatalogRelation](../I/IsCatalogRelation.md)
  - [InitTableAmRoutine](../I/InitTableAmRoutine.md)
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](ReleaseSysCache.md)
- Called from (representative examples):
  - [RelationBuildDesc](RelationBuildDesc.md)
  - [RelationBuildLocalRelation](RelationBuildLocalRelation.md)
  - [load_relcache_init_file](../l/load_relcache_init_file.md)

## Notes and Other Information
- The function uses different strategies to avoid performance overhead: sequences get heap AM directly, catalog relations skip syscache lookup
- Sequences are treated specially because they're accessed like heap tables despite having InvalidOid in pg_class.relam
- The function assumes catalog relations always use HEAP_TABLE_AM_OID for efficiency
- Error handling is provided for cases where the access method lookup fails
- This is part of the relation cache (relcache) infrastructure in PostgreSQL

## Simplified Source

```c
void RelationInitTableAccessMethod(Relation relation) {
    if (relation->rd_rel->relkind == RELKIND_SEQUENCE) {
        // Sequences use heap table access method directly
        Assert(relation->rd_rel->relam == InvalidOid);
        relation->rd_amhandler = F_HEAP_TABLEAM_HANDLER;
    }
    else if (IsCatalogRelation(relation)) {
        // Catalog relations use heap AM, avoid syscache lookup for performance
        Assert(relation->rd_rel->relam == HEAP_TABLE_AM_OID);
        relation->rd_amhandler = F_HEAP_TABLEAM_HANDLER;
    }
    else {
        // Regular relations: lookup access method in pg_am catalog
        Assert(relation->rd_rel->relam != InvalidOid);

        HeapTuple tuple = SearchSysCache1(AMOID,
                                         ObjectIdGetDatum(relation->rd_rel->relam));
        if (!HeapTupleIsValid(tuple)) {
            elog(ERROR, "cache lookup failed for access method %u",
                 relation->rd_rel->relam);
        }

        Form_pg_am aform = (Form_pg_am) GETSTRUCT(tuple);
        relation->rd_amhandler = aform->amhandler;
        ReleaseSysCache(tuple);
    }

    // Initialize the table access method's API struct
    InitTableAmRoutine(relation);
}
```