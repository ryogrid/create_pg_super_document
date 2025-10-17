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
  - [InitializeRelfilenumberMap](../I/InitializeRelfilenumberMap.md)
  - MemSet
  - [hash_search](../h/hash_search.md) (HASH_FIND, HASH_ENTER)
  - [RelationMapFilenumberToOid](RelationMapFilenumberToOid.md)
  - [table_open](../t/table_open.md), table_close
  - [systable_beginscan](../s/systable_beginscan.md), systable_getnext, systable_endscan
  - RelfilenumberMapKey, RelfilenumberMapEntry
  - Form_pg_class, SysScanDesc
- Called from (representative examples):
  - [ReorderBufferProcessTXN](ReorderBufferProcessTXN.md)
  - [pg_filenode_relation](../p/pg_filenode_relation.md)

## Notes and Other Information
- Returns InvalidOid if no matching relation is found
- Implements negative caching to avoid repeated failed lookups
- Normalizes MyDatabaseTableSpace to 0 to match pg_class representation
- Skips temporary relations (RELPERSISTENCE_TEMP) to avoid ambiguous matches
- Uses ClassTblspcRelfilenodeIndexId index for efficient pg_class lookups
- Handles both shared system tables and regular user tables through different code paths  
- Cache entry creation is deferred until after catalog operations to prevent invalidation race conditions
- Critical for logical replication and administrative functions that need to resolve file identifiers back to logical relations

## Simplified Source

```c
Oid
RelidByRelfilenumber(Oid reltablespace, RelFileNumber relfilenumber)
{
    RelfilenumberMapKey key;
    RelfilenumberMapEntry *entry;
    bool found;
    Oid relid;

    // Initialize cache if needed
    if (RelfilenumberMapHash == NULL)
        InitializeRelfilenumberMap();

    // Normalize tablespace (pg_class shows 0 for MyDatabaseTableSpace)
    if (reltablespace == MyDatabaseTableSpace)
        reltablespace = 0;

    // Set up cache key
    MemSet(&key, 0, sizeof(key));
    key.reltablespace = reltablespace;
    key.relfilenumber = relfilenumber;

    // Check cache first
    entry = hash_search(RelfilenumberMapHash, &key, HASH_FIND, &found);
    if (found)
        return entry->relid;

    // Cache miss - do the actual lookup
    relid = InvalidOid;

    if (reltablespace == GLOBALTABLESPACE_OID) {
        // Shared table - use relation mapper
        relid = RelationMapFilenumberToOid(relfilenumber, true);
    } else {
        // Regular table - scan pg_class
        Relation relation = table_open(RelationRelationId, AccessShareLock);
        ScanKeyData skey[2];

        // Copy and set up scan keys
        memcpy(skey, relfilenumber_skey, sizeof(skey));
        skey[0].sk_argument = ObjectIdGetDatum(reltablespace);
        skey[1].sk_argument = ObjectIdGetDatum(relfilenumber);

        SysScanDesc scandesc = systable_beginscan(relation,
                                                  ClassTblspcRelfilenodeIndexId,
                                                  true, NULL, 2, skey);

        HeapTuple ntp;
        found = false;
        while (HeapTupleIsValid(ntp = systable_getnext(scandesc))) {
            Form_pg_class classform = (Form_pg_class) GETSTRUCT(ntp);

            // Skip temporary relations to avoid conflicts
            if (classform->relpersistence == RELPERSISTENCE_TEMP)
                continue;

            if (found)
                elog(ERROR, "unexpected duplicate for tablespace %u, relfilenumber %u",
                     reltablespace, relfilenumber);

            found = true;
            relid = classform->oid;
        }

        systable_endscan(scandesc);
        table_close(relation, AccessShareLock);

        // If not found in pg_class, try non-shared mapped relations
        if (!found)
            relid = RelationMapFilenumberToOid(relfilenumber, false);
    }

    // Cache the result (positive or negative)
    entry = hash_search(RelfilenumberMapHash, &key, HASH_ENTER, &found);
    if (found)
        elog(ERROR, "corrupted hashtable");
    entry->relid = relid;

    return relid;
}
```