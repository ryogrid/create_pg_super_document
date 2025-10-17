# CheckConstraintFetch

## Location
[src/backend/utils/cache/relcache.c:4585-4673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L4585-L4673)

## Overview
CheckConstraintFetch loads check constraints for a relation from the pg_constraint system catalog and stores them in the relation's cache structure.

## Definition

```c
static void
CheckConstraintFetch(Relation relation)
```
## Detailed Description
CheckConstraintFetch is a static function within the relation cache subsystem that retrieves and processes check constraints for a given relation. The function performs a systematic scan of the pg_constraint catalog to find all check constraints associated with the relation, validates and processes the constraint data, and stores it in the relation's cached tuple descriptor.

The function allocates memory in CacheMemoryContext for storing constraint information, ensuring the data persists for the lifetime of the relation cache entry. It performs validation by checking that the expected number of constraints are found and warns if discrepancies exist. The constraint binary expressions (conbin) are detoasted and converted to C strings for storage.

After loading all constraints, the function sorts them by name to ensure deterministic ordering, which is important for both consistent constraint application and efficient comparison operations in equalTupleDescs().

## Parameters / Member Variables
- `relation`: The Relation structure for which check constraints should be loaded
## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md)
  - [ScanKeyInit](../S/ScanKeyInit.md)  
  - [table_open](../t/table_open.md)
  - [systable_beginscan](../s/systable_beginscan.md)
  - [systable_getnext](../s/systable_getnext.md)
  - [systable_endscan](../s/systable_endscan.md)
  - [table_close](../t/table_close.md)
  - [fastgetattr](../f/fastgetattr.md)
  - TextDatumGetCString
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - [CheckConstraintCmp](CheckConstraintCmp.md)
  - qsort
- Called from (representative examples):
  - [RelationBuildTupleDesc](../R/RelationBuildTupleDesc.md)

## Notes and Other Information
- Uses CacheMemoryContext for memory allocation to ensure constraint data persists with the relation cache
- Handles missing or extra constraint records gracefully by issuing warnings rather than errors
- Sorts constraints by name for deterministic ordering and performance optimization
- Only processes constraints of type CONSTRAINT_CHECK, ignoring other constraint types
- Validates that conbin (constraint binary expression) is not null before processing

## Simplified Source

```c
static void CheckConstraintFetch(Relation relation) {
    ConstrCheck *check;
    int ncheck = relation->rd_rel->relchecks;
    Relation conrel;
    SysScanDesc conscan;
    ScanKeyData skey[1];
    HeapTuple htup;
    int found = 0;

    // Allocate array for expected check constraints
    check = (ConstrCheck *) MemoryContextAllocZero(CacheMemoryContext,
                                                   ncheck * sizeof(ConstrCheck));

    // Search pg_constraint for this relation's check constraints
    ScanKeyInit(&skey[0], Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(RelationGetRelid(relation)));

    conrel = table_open(ConstraintRelationId, AccessShareLock);
    conscan = systable_beginscan(conrel, ConstraintRelidTypidNameIndexId, true,
                                 NULL, 1, skey);

    // Process each constraint record
    while (HeapTupleIsValid(htup = systable_getnext(conscan))) {
        Form_pg_constraint conform = (Form_pg_constraint) GETSTRUCT(htup);
        Datum val;
        bool isnull;

        // Only want check constraints
        if (conform->contype != CONSTRAINT_CHECK)
            continue;

        // Check array bounds
        if (found >= ncheck) {
            elog(WARNING, "unexpected pg_constraint record found");
            break;
        }

        // Store constraint metadata
        check[found].ccvalid = conform->convalidated;
        check[found].ccnoinherit = conform->connoinherit;
        check[found].ccname = MemoryContextStrdup(CacheMemoryContext,
                                                  NameStr(conform->conname));

        // Extract constraint expression (conbin)
        val = fastgetattr(htup, Anum_pg_constraint_conbin, conrel->rd_att, &isnull);
        if (isnull) {
            elog(WARNING, "null conbin for relation");
        } else {
            // Convert to string and store in cache memory
            char *s = TextDatumGetCString(val);
            check[found].ccbin = MemoryContextStrdup(CacheMemoryContext, s);
            pfree(s);
            found++;
        }
    }

    systable_endscan(conscan);
    table_close(conrel, AccessShareLock);

    // Warn if we didn't find expected number
    if (found != ncheck)
        elog(WARNING, "%d pg_constraint record(s) missing", ncheck - found);

    // Sort by constraint name for deterministic ordering
    if (found > 1)
        qsort(check, found, sizeof(ConstrCheck), CheckConstraintCmp);

    // Install in relation's constraint structure
    relation->rd_att->constr->check = check;
    relation->rd_att->constr->num_check = found;
}
```