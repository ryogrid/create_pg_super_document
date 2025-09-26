# RelidByRelfilenumber

## Location
[src/backend/utils/cache/relfilenumbermap.c:141-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relfilenumbermap.c#L141-L250)

## Overview
RelidByRelfilenumber maps a relation's (tablespace, relfilenumber) to a relation's OID and caches the result for performance optimization.

## Definition
```c
Oid RelidByRelfilenumber(Oid reltablespace, RelFileNumber relfilenumber)
```

## Detailed Description
This function performs reverse lookups from file system identifiers (tablespace OID + relfilenumber) to relation OIDs. It implements a multi-layered approach to resolve these mappings:

1. **Cache Lookup**: First checks the RelfilenumberMapHash for cached mappings
2. **Shared Tables**: For GLOBALTABLESPACE_OID, uses RelationMapFilenumberToOid with shared=true
3. **Regular Tables**: Scans pg_class using ClassTblspcRelfilenodeIndexId index to find matching entries
4. **Non-shared Mapped Tables**: Falls back to RelationMapFilenumberToOid with shared=false for system relations
5. **Cache Storage**: Stores both positive and negative results in the hash cache

The function handles special cases like MyDatabaseTableSpace normalization and explicitly ignores temporary relations due to potential relfilenumber conflicts across backends. It ensures cache consistency by deferring cache entry creation until after all catalog access is complete.

## Parameters / Member Variables
- `reltablespace`: The tablespace OID where the relation file resides
- `relfilenumber`: The file number identifier of the relation within the tablespace

## Dependencies
- Functions called/Symbols referenced:
  - InitializeRelfilenumberMap
  - MemSet
  - hash_search (HASH_FIND, HASH_ENTER)
  - RelationMapFilenumberToOid
  - table_open, table_close
  - systable_beginscan, systable_getnext, systable_endscan
  - RelfilenumberMapKey, RelfilenumberMapEntry
  - Form_pg_class, SysScanDesc
- Called from (representative examples):
  - ReorderBufferProcessTXN
  - pg_filenode_relation

## Notes and Other Information
- Returns InvalidOid if no matching relation is found
- Implements negative caching to avoid repeated failed lookups
- Normalizes MyDatabaseTableSpace to 0 to match pg_class representation
- Skips temporary relations (RELPERSISTENCE_TEMP) to avoid ambiguous matches
- Uses ClassTblspcRelfilenodeIndexId index for efficient pg_class lookups
- Handles both shared system tables and regular user tables through different code paths  
- Cache entry creation is deferred until after catalog operations to prevent invalidation race conditions
- Critical for logical replication and administrative functions that need to resolve file identifiers back to logical relations