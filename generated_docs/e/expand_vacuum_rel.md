# expand_vacuum_rel

## Location
[src/backend/commands/vacuum.c:881-1020](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L881-L1020)

## Overview
Expands a VacuumRelation by filling in the table OID if not specified and optionally adding VacuumRelations for all partitions of a partitioned table.

## Definition

```c
static List *
expand_vacuum_rel(VacuumRelation *vrel, MemoryContext vac_context,
				  int options)
```
## Detailed Description
The expand_vacuum_rel function processes a VacuumRelation to create a complete list of relations to vacuum. If the VacuumRelation contains an OID, it simply returns a list containing that relation. If no OID is provided, it resolves the relation name to an OID and checks if it's a partitioned table. For partitioned tables, it creates additional VacuumRelation entries for each partition.

The function is designed to handle permission checks and locking carefully. It takes a transient AccessShareLock for syscache lookups and uses find_all_inheritors to discover partitions, but releases locks to avoid deadlock risks in multi-transaction scenarios. Autovacuum workers are not expected to reach this code since they supply OIDs directly.

## Parameters / Member Variables
- : The input VacuumRelation containing either an OID or a relation name to be expanded
- : Memory context in which to allocate new VacuumRelation structures
- : Vacuum options flags that control behavior (e.g., VACOPT_SKIP_LOCKED, VACOPT_VACUUM)

## Dependencies
- Functions called/Symbols referenced:
  - AmAutoVacuumWorkerProcess
  - [RangeVarGetRelidExtended](../R/RangeVarGetRelidExtended.md)
  - [vacuum_is_permitted_for_relation](../v/vacuum_is_permitted_for_relation.md)
  - [makeVacuumRelation](../m/makeVacuumRelation.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [UnlockRelationOid](../U/UnlockRelationOid.md)
- Called from (representative examples):
  - [vacuum](../v/vacuum.md) (src/backend/commands/vacuum.c:539)

## Notes and Other Information
- Only processes relations when OID is not already provided (autovacuum supplies OIDs)
- Uses transient locking strategy to minimize deadlock risk
- Handles both regular relations and partitioned tables with automatic partition discovery
- Includes permission checking via vacuum_is_permitted_for_relation
- Memory allocation is performed in the provided vac_context
- Location: src/backend/commands/vacuum.c:881-1020

## Simplified Source

```c
static List *expand_vacuum_rel(VacuumRelation *vrel, MemoryContext vac_context,
                              int options) {
    List *vacrels = NIL;
    MemoryContext oldcontext;

    // If OID already provided, just return this relation
    if (OidIsValid(vrel->oid)) {
        oldcontext = MemoryContextSwitchTo(vac_context);
        vacrels = lappend(vacrels, vrel);
        MemoryContextSwitchTo(oldcontext);
        return vacrels;
    }

    // Resolve relation name to OID and expand partitions if needed
    Oid relid;
    HeapTuple tuple;
    Form_pg_class classForm;
    bool include_parts;
    int rvr_opts;

    // Autovacuum workers should not reach this code
    Assert(!AmAutoVacuumWorkerProcess());

    // Get relation OID with optional skip-locked behavior
    rvr_opts = (options & VACOPT_SKIP_LOCKED) ? RVR_SKIP_LOCKED : 0;
    relid = RangeVarGetRelidExtended(vrel->relation, AccessShareLock,
                                    rvr_opts, NULL, NULL);

    // Handle lock unavailable case
    if (!OidIsValid(relid)) {
        if (options & VACOPT_VACUUM)
            ereport(WARNING, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                             errmsg("skipping vacuum of \"%s\" --- lock not available",
                                   vrel->relation->relname)));
        else
            ereport(WARNING, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                             errmsg("skipping analyze of \"%s\" --- lock not available",
                                   vrel->relation->relname)));
        return vacrels;
    }

    // Fetch relation info from syscache
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "cache lookup failed for relation %u", relid);
    classForm = (Form_pg_class) GETSTRUCT(tuple);

    // Add main relation if user has required privileges
    if (vacuum_is_permitted_for_relation(relid, classForm, options)) {
        oldcontext = MemoryContextSwitchTo(vac_context);
        vacrels = lappend(vacrels, makeVacuumRelation(vrel->relation,
                                                     relid, vrel->va_cols));
        MemoryContextSwitchTo(oldcontext);
    }

    // Check if we need to include partitions
    include_parts = (classForm->relkind == RELKIND_PARTITIONED_TABLE);
    ReleaseSysCache(tuple);

    // Add partitions if this is a partitioned table
    if (include_parts) {
        List *part_oids = find_all_inheritors(relid, NoLock, NULL);
        ListCell *part_lc;

        foreach(part_lc, part_oids) {
            Oid part_oid = lfirst_oid(part_lc);

            if (part_oid == relid)
                continue;  // Skip the main table

            // Add partition to vacuum list (no RangeVar for partitions)
            oldcontext = MemoryContextSwitchTo(vac_context);
            vacrels = lappend(vacrels, makeVacuumRelation(NULL, part_oid,
                                                         vrel->va_cols));
            MemoryContextSwitchTo(oldcontext);
        }
    }

    // Release the lock to avoid deadlock risks
    UnlockRelationOid(relid, AccessShareLock);
    return vacrels;
}
```