# RelationInitTableAccessMethod

## Location
src/backend/utils/cache/relcache.c: 1810 - 1874

## Overview
Initializes table access method support for a table-like relation by setting up the appropriate access method handler based on the relation's type and characteristics.

## Definition


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
  - IsCatalogRelation
  - InitTableAmRoutine
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - ReleaseSysCache
- Called from (representative examples):
  - RelationBuildDesc
  - RelationBuildLocalRelation
  - load_relcache_init_file

## Notes and Other Information
- The function uses different strategies to avoid performance overhead: sequences get heap AM directly, catalog relations skip syscache lookup
- Sequences are treated specially because they're accessed like heap tables despite having InvalidOid in pg_class.relam
- The function assumes catalog relations always use HEAP_TABLE_AM_OID for efficiency
- Error handling is provided for cases where the access method lookup fails
- This is part of the relation cache (relcache) infrastructure in PostgreSQL