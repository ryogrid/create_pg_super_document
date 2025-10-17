# AttrDefaultFetch

## Location
[src/backend/utils/cache/relcache.c:4490-4569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L4490-L4569)

## Overview
Loads attribute default value definitions from the pg_attrdef catalog for a relation, building an array of AttrDefault entries sorted by attribute number.

## Definition
```c
static void AttrDefaultFetch(Relation relation, int ndef)
```

## Detailed Description
AttrDefaultFetch retrieves and processes default value expressions for table columns that have been marked with atthasdef. The function performs a systematic scan of the pg_attrdef system catalog to find default expressions corresponding to the specified relation.

The function allocates memory in CacheMemoryContext for an array of AttrDefault structures, then scans pg_attrdef using the relation's OID as the search key. For each matching record, it extracts the default expression (adbin field), converts it from internal format to a C string, and stores it along with the attribute number in the AttrDefault array.

The implementation includes robust error handling for missing or unexpected records, issuing warnings rather than hard errors to allow PostgreSQL to continue operating even with incomplete default value information. After loading all records, the function sorts the array by attribute number for efficient lookup and installs it in the relation's tuple descriptor constraint structure.

## Parameters
- `relation`: The Relation structure for which to fetch default values
- `ndef`: Expected number of attributes that have default values (those marked with atthasdef)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [fastgetattr](../f/fastgetattr.md)
  - TextDatumGetCString
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - qsort
  - [AttrDefaultCmp](AttrDefaultCmp.md)
- Called from:
  - [RelationBuildTupleDesc](../R/RelationBuildTupleDesc.md)

## Notes and Other Information
- Memory allocation occurs in CacheMemoryContext to ensure persistence
- Handles gracefully missing pg_attrdef records by issuing warnings rather than errors
- Sorts AttrDefault entries by attribute number for efficient access by equalTupleDescs()
- Detoasts and converts default expressions from Datum to C string format
- Updates relation's constraint structure with the loaded default values
- Part of the relation cache building process during tuple descriptor construction

## Simplified Source

```c
static void AttrDefaultFetch(Relation relation, int ndef) {
    AttrDefault *attrdef;
    Relation adrel;
    SysScanDesc adscan;
    ScanKeyData skey;
    HeapTuple htup;
    int found = 0;

    // Allocate array for expected number of defaults
    attrdef = (AttrDefault *) MemoryContextAllocZero(CacheMemoryContext,
                                                     ndef * sizeof(AttrDefault));

    // Search pg_attrdef for this relation's defaults
    ScanKeyInit(&skey, Anum_pg_attrdef_adrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(relation)));

    adrel = table_open(AttrDefaultRelationId, AccessShareLock);
    adscan = systable_beginscan(adrel, AttrDefaultIndexId, true, NULL, 1, &skey);

    // Process each default value record
    while (HeapTupleIsValid(htup = systable_getnext(adscan))) {
        Form_pg_attrdef adform = (Form_pg_attrdef) GETSTRUCT(htup);
        Datum val;
        bool isnull;

        // Check array bounds
        if (found >= ndef) {
            elog(WARNING, "unexpected pg_attrdef record found for attribute %d",
                 adform->adnum);
            break;
        }

        // Extract default expression (adbin)
        val = fastgetattr(htup, Anum_pg_attrdef_adbin, adrel->rd_att, &isnull);
        if (isnull) {
            elog(WARNING, "null adbin for attribute %d", adform->adnum);
        } else {
            // Convert to string and store in cache memory
            char *s = TextDatumGetCString(val);
            attrdef[found].adnum = adform->adnum;
            attrdef[found].adbin = MemoryContextStrdup(CacheMemoryContext, s);
            pfree(s);
            found++;
        }
    }

    systable_endscan(adscan);
    table_close(adrel, AccessShareLock);

    // Warn if we didn't find expected number
    if (found != ndef)
        elog(WARNING, "%d pg_attrdef record(s) missing for relation",
             ndef - found);

    // Sort by attribute number for efficient lookup
    if (found > 1)
        qsort(attrdef, found, sizeof(AttrDefault), AttrDefaultCmp);

    // Install in relation's constraint structure
    relation->rd_att->constr->defval = attrdef;
    relation->rd_att->constr->num_defval = found;
}
```