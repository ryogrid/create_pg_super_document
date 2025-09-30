# get_all_vacuum_rels

## Location
[src/backend/commands/vacuum.c:1021-1082](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L1021-L1082)

## Overview
Constructs a list of VacuumRelations for all vacuumable relations in the current database by scanning the system catalog.

## Definition
```c
static List *
get_all_vacuum_rels(MemoryContext vac_context, int options)
```

## Detailed Description
The get_all_vacuum_rels function performs a full catalog scan of pg_class to identify all relations that can be vacuumed or analyzed in the current database. It filters relations based on their kind (regular tables, materialized views, and partitioned tables) and checks permissions for each relation before adding it to the result list.

The function creates VacuumRelation entries with OIDs but no RangeVar, since these are discovered relations rather than user-specified ones. This approach avoids inappropriate error messages if a relation becomes unavailable later. All memory allocation is performed in the provided vac_context.

## Parameters / Member Variables
- `vac_context`: Memory context in which to allocate new VacuumRelation structures
- `options`: Vacuum options flags used for permission checking

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - [vacuum_is_permitted_for_relation](../v/vacuum_is_permitted_for_relation.md)
  - [makeVacuumRelation](../m/makeVacuumRelation.md)
  - [table_endscan](../t/table_endscan.md)
  - [table_close](../t/table_close.md)
- Called from (representative examples):
  - [vacuum](../v/vacuum.md) (src/backend/commands/vacuum.c:547)

## Notes and Other Information
- Includes partitioned tables in the scan; caller decides whether to process them
- Filters by relation kind: regular tables, materialized views, and partitioned tables only
- Performs permission checking for each relation before inclusion
- Uses catalog scan with AccessShareLock for safe concurrent access
- Creates VacuumRelation entries with OIDs only (no RangeVar or column lists)
- Location: src/backend/commands/vacuum.c:1021-1082

## Simplified Source

```c
static List *get_all_vacuum_rels(MemoryContext vac_context, int options) {
    List *vacrels = NIL;
    Relation pgclass;
    TableScanDesc scan;
    HeapTuple tuple;

    // Open pg_class catalog for scanning
    pgclass = table_open(RelationRelationId, AccessShareLock);
    scan = table_beginscan_catalog(pgclass, 0, NULL);

    // Scan all relations in the catalog
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
        MemoryContext oldcontext;
        Oid relid = classForm->oid;

        // Filter by relation kind: tables, matviews, and partitioned tables
        if (classForm->relkind != RELKIND_RELATION &&
            classForm->relkind != RELKIND_MATVIEW &&
            classForm->relkind != RELKIND_PARTITIONED_TABLE) {
            continue;
        }

        // Check if user has permission to vacuum/analyze this relation
        if (!vacuum_is_permitted_for_relation(relid, classForm, options))
            continue;

        // Add relation to vacuum list (OID only, no RangeVar or columns)
        oldcontext = MemoryContextSwitchTo(vac_context);
        vacrels = lappend(vacrels, makeVacuumRelation(NULL, relid, NIL));
        MemoryContextSwitchTo(oldcontext);
    }

    // Clean up scan and close catalog
    table_endscan(scan);
    table_close(pgclass, AccessShareLock);

    return vacrels;
}
```