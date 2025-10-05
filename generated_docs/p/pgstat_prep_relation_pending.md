# pgstat_prep_relation_pending

## Location
[src/backend/utils/activity/pgstat_relation.c:898-916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/activity/pgstat_relation.c#L898-L916)

## Overview
Creates or retrieves a pending statistics entry for a relation, initializing it with basic identification information for statistics collection.

## Definition
```c
static PgStat_TableStatus *pgstat_prep_relation_pending(Oid rel_id, bool isshared)
```

## Detailed Description
This internal function serves as a foundation for relation statistics management by ensuring that a proper pending statistics entry exists for a given relation. It handles both shared (system catalog) and regular database relations by determining the appropriate database context and creating the entry if it doesn't already exist.

The function acts as a wrapper around the more general pgstat_prep_pending_entry function, specializing it for relation statistics (PGSTAT_KIND_RELATION). It properly initializes the returned entry with the relation ID and shared status, making it ready for statistics accumulation.

This function is crucial for the statistics collection infrastructure as it ensures that all relation statistics operations have a valid target entry to work with, whether for regular transaction processing or two-phase commit scenarios.

## Parameters / Member Variables
- `rel_id`: Object identifier (OID) of the relation for which to prepare the statistics entry
- `isshared`: Boolean flag indicating whether the relation is a shared system catalog (true) or a regular database relation (false)

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_prep_pending_entry](pgstat_prep_pending_entry.md)
  - PGSTAT_KIND_RELATION (statistics kind constant)
  - [PgStat_EntryRef](../P/PgStat_EntryRef.md) (data structure)
  - [PgStat_TableStatus](../P/PgStat_TableStatus.md) (data structure)
- Called from (representative examples):
  - [pgstat_assoc_relation](pgstat_assoc_relation.md)
  - [pgstat_twophase_postcommit](pgstat_twophase_postcommit.md)
  - [pgstat_twophase_postabort](pgstat_twophase_postabort.md)

## Notes and Other Information
- This is a static function, internal to the pgstat_relation.c module
- Handles both shared system catalogs and regular database relations through the isshared parameter
- For shared relations, uses InvalidOid as the database ID; otherwise uses MyDatabaseId
- Essential building block for relation statistics infrastructure, used by both regular and two-phase commit statistics handling
- Ensures that statistics entries are properly initialized and ready for accumulation of statistics data

## Simplified Source

```c
static PgStat_TableStatus *
pgstat_prep_relation_pending(Oid rel_id, bool isshared)
{
    PgStat_EntryRef *entry_ref;
    PgStat_TableStatus *pending;

    // Create or find pending statistics entry for the relation
    entry_ref = pgstat_prep_pending_entry(PGSTAT_KIND_RELATION,
                                          isshared ? InvalidOid : MyDatabaseId,
                                          rel_id, NULL);

    // Initialize the pending entry with basic information
    pending = entry_ref->pending;
    pending->id = rel_id;
    pending->shared = isshared;

    return pending;
}
```