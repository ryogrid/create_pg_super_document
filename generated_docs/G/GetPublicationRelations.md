# GetPublicationRelations

## Location
[src/backend/catalog/pg_publication.c:716-758](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_publication.c#L716-L758)

## Overview
Retrieves a list of relation OIDs for a specific FOR TABLE publication, handling partition options and returning a sorted, deduplicated list of relations.

## Definition
```c
List *GetPublicationRelations(Oid pubid, PublicationPartOpt pub_partopt)
```

## Detailed Description
This function retrieves all relations (tables) that belong to a specific publication by scanning the pg_publication_rel system catalog. It is specifically designed for FOR TABLE publications and should not be used for FOR ALL TABLES publications (which should use GetAllTablesPublicationRelations instead).

The function performs a systematic scan of the publication relations catalog, filtering by the publication OID. For each relation found, it calls GetPubPartitionOptionRelations to handle partition-related logic based on the provided partition options. The final result is sorted and deduplicated to ensure consistency.

The function opens the PublicationRelRelationId catalog table with AccessShareLock, performs an indexed scan using the PublicationRelPrpubidIndexId index for efficient lookup, and properly cleans up resources after the scan.

## Parameters / Member Variables
- `pubid`: The OID of the publication for which to retrieve relations
- `pub_partopt`: Publication partition options that control how partitioned tables are handled

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [GetPubPartitionOptionRelations](GetPubPartitionOptionRelations.md)
  - [list_sort](../l/list_sort.md)
  - [list_oid_cmp](../l/list_oid_cmp.md)
  - [list_deduplicate_oid](../l/list_deduplicate_oid.md)
  - Form_pg_publication_rel
  - [SysScanDesc](../S/SysScanDesc.md)
  - [PublicationPartOpt](../P/PublicationPartOpt.md)
- Called from (representative examples):
  - NUM_PUBLICATION_TABLES_ELEM
  - [AlterPublicationOptions](../A/AlterPublicationOptions.md)
  - [AlterPublicationTables](../A/AlterPublicationTables.md)
  - [AlterPublicationSchemas](../A/AlterPublicationSchemas.md)

## Notes and Other Information
- Specifically for FOR TABLE publications only - do not use for FOR ALL TABLES publications
- Uses indexed scan with PublicationRelPrpubidIndexId for efficient catalog lookup
- Handles partition options through GetPubPartitionOptionRelations function
- Returns a sorted and deduplicated list to ensure consistent ordering
- Properly manages locks and resource cleanup with AccessShareLock
- The function ensures memory management by properly closing catalog relations and ending scans

## Simplified Source

```c
List *GetPublicationRelations(Oid pubid, PublicationPartOpt pub_partopt) {
    List *result;
    Relation pubrelsrel;
    ScanKeyData scankey;
    SysScanDesc scan;
    HeapTuple tup;

    // Open publication relations catalog
    pubrelsrel = table_open(PublicationRelRelationId, AccessShareLock);

    // Set up scan key to find relations for this publication
    ScanKeyInit(&scankey,
                Anum_pg_publication_rel_prpubid,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(pubid));

    // Begin indexed scan
    scan = systable_beginscan(pubrelsrel, PublicationRelPrpubidIndexId,
                              true, NULL, 1, &scankey);

    // Collect relations, handling partition options
    result = NIL;
    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_publication_rel pubrel;

        pubrel = (Form_pg_publication_rel) GETSTRUCT(tup);
        result = GetPubPartitionOptionRelations(result, pub_partopt,
                                               pubrel->prrelid);
    }

    // Clean up scan and close relation
    systable_endscan(scan);
    table_close(pubrelsrel, AccessShareLock);

    // Sort and deduplicate the result list
    list_sort(result, list_oid_cmp);
    list_deduplicate_oid(result);

    return result;
}
```