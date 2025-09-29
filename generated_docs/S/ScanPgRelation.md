# ScanPgRelation

## Location
[src/backend/utils/cache/relcache.c:339-408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L339-L408)

## Overview
ScanPgRelation scans the pg_class system catalog to retrieve a specific relation tuple by OID, handling snapshot selection and tuple copying for relcache operations.

## Definition
static HeapTuple ScanPgRelation(Oid targetRelId, bool indexOK, bool force_non_historic)

## Detailed Description
ScanPgRelation is a core function used by RelationBuildDesc to locate and retrieve pg_class tuples during relation cache construction. The function performs a system catalog scan of pg_class using the target relation OID, with careful handling of snapshot selection to ensure consistency during concurrent operations. It includes safety checks for database selection and supports both index and heap scans depending on system state and caller requirements.

The function is designed to handle the complexities of concurrent updates by requiring the caller to hold at least AccessShareLock on the target relation. It returns a copied heap tuple that must be freed by the caller, ensuring the tuple remains valid after the scan completes.

## Parameters / Member Variables
- : The OID of the relation to search for in pg_class
- : Boolean flag indicating whether index scans are allowed (false forces heap scan)
- : Boolean flag to force use of a non-historic catalog snapshot for newer tuple versions

## Dependencies
- Functions called/Symbols referenced:
  - [SysScanDesc](SysScanDesc.md) (system scan descriptor type)
  - [GetNonHistoricCatalogSnapshot](../G/GetNonHistoricCatalogSnapshot.md) (for non-historic snapshot acquisition)
  - [systable_beginscan](../s/systable_beginscan.md) (to initiate system table scan)
  - [systable_getnext](../s/systable_getnext.md) (to retrieve next tuple from scan)
  - [heap_copytuple](../h/heap_copytuple.md) (to create a copy of the retrieved tuple)
- Called from (representative examples):
  - [RelationBuildDesc](../R/RelationBuildDesc.md) (main relcache building function)
  - [RelationInitPhysicalAddr](../R/RelationInitPhysicalAddr.md) (for physical address initialization)
  - [RelationReloadIndexInfo](../R/RelationReloadIndexInfo.md) (for index information reloading)
  - [RelationReloadNailed](../R/RelationReloadNailed.md) (for reloading nailed relations)

## Notes and Other Information
- The function includes a critical safety check preventing pg_class access before database selection
- Uses AccessShareLock on pg_class during the scan operation
- Supports both index and heap scans based on criticalRelcachesBuilt state and indexOK parameter
- The returned tuple is a palloc'd copy that must be freed with heap_freetuple
- [Snapshot](Snapshot.md) selection logic accommodates both normal and non-historic catalog access patterns
- Essential for maintaining relcache consistency during concurrent database operations

## Simplified Source

```c
static HeapTuple
ScanPgRelation(Oid targetRelId, bool indexOK, bool force_non_historic)
{
    HeapTuple pg_class_tuple;
    Relation pg_class_desc;
    SysScanDesc pg_class_scan;
    ScanKeyData key[1];
    Snapshot snapshot = NULL;

    // Safety check - ensure database is selected before accessing pg_class
    if (!OidIsValid(MyDatabaseId))
        elog(FATAL, "cannot read pg_class without having selected a database");

    // Setup scan key to find relation by OID
    ScanKeyInit(&key[0],
                Anum_pg_class_oid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(targetRelId));

    // Open pg_class relation
    pg_class_desc = table_open(RelationRelationId, AccessShareLock);

    // Use non-historic snapshot if requested (for logical decoding)
    if (force_non_historic)
        snapshot = GetNonHistoricCatalogSnapshot(RelationRelationId);

    // Begin system table scan (index or heap scan based on conditions)
    pg_class_scan = systable_beginscan(pg_class_desc, ClassOidIndexId,
                                      indexOK && criticalRelcachesBuilt,
                                      snapshot,
                                      1, key);

    // Get the tuple
    pg_class_tuple = systable_getnext(pg_class_scan);

    // Copy tuple before releasing buffer (caller must free)
    if (HeapTupleIsValid(pg_class_tuple))
        pg_class_tuple = heap_copytuple(pg_class_tuple);

    // Cleanup scan and close relation
    systable_endscan(pg_class_scan);
    table_close(pg_class_desc, AccessShareLock);

    return pg_class_tuple;
}
```