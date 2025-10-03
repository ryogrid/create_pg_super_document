# vac_update_relstats

## Location
[src/backend/commands/vacuum.c:1409-1584](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/vacuum.c#L1409-L1584)

## Overview
Updates the whole-relation statistics stored in the pg_class system catalog row for a given relation, including both heap and index relations, using in-place tuple updates to avoid transaction semantics issues during vacuum operations.

## Definition

```c
void
vac_update_relstats(Relation relation,
					BlockNumber num_pages, double num_tuples,
					BlockNumber num_all_visible_pages,
					bool hasindex, TransactionId frozenxid,
					MultiXactId minmulti,
					bool *frozenxid_updated, bool *minmulti_updated,
					bool in_outer_xact)
```
## Detailed Description
This function updates the statistical information for a relation in its pg_class tuple. It deliberately violates transaction semantics by using in-place updates to overwrite the existing tuple data directly. This approach is necessary because:

1. **Vacuum Efficiency**: If regular tuple updates were used, vacuuming pg_class itself would be inefficient since most tuples would become obsoleted during a vacuum cycle.

2. **PROC_IN_VACUUM Safety**: When in lazy VACUUM mode with PROC_IN_VACUUM set, regular updates could cause issues where concurrent vacuum operations might incorrectly delete tuples.

The function updates three types of information:
- **Statistical Data**: Always updated (relpages, reltuples, relallvisible)
- **DDL Flags**: Updated only when not in an outer transaction (relhasindex, relhasrules, relhastriggers)
- **Transaction IDs**: Updated with proper validation (relfrozenxid, relminmxid)

Special handling is provided for "future" transaction IDs that appear corrupt, which are overwritten with new valid values.

## Parameters / Member Variables
- `relation`: The relation whose statistics are being updated
- `num_pages`: New number of pages in the relation
- `num_tuples`: New count of live tuples in the relation
- `num_all_visible_pages`: New count of all-visible pages for visibility map
- `hasindex`: Whether the relation currently has any indexes
- `frozenxid`: New frozen transaction ID, or InvalidTransactionId if no update needed
- `minmulti`: New minimum MultiXactId, or InvalidMultiXactId if no update needed
- `*frozenxid_updated`: Output parameter indicating if relfrozenxid was actually updated
- `*minmulti_updated`: Output parameter indicating if relminmxid was actually updated
- `in_outer_xact`: Whether this operation is within an outer transaction (affects DDL flag updates)
## Dependencies
- Functions called/Symbols referenced:
  - [systable_inplace_update_begin](../s/systable_inplace_update_begin.md)
  - [systable_inplace_update_finish](../s/systable_inplace_update_finish.md)
  - [systable_inplace_update_cancel](../s/systable_inplace_update_cancel.md)
  - TransactionIdIsNormal
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [ReadNextTransactionId](../R/ReadNextTransactionId.md)
  - MultiXactIdIsValid
  - [MultiXactIdPrecedes](../M/MultiXactIdPrecedes.md)
  - [ReadNextMultiXactId](../R/ReadNextMultiXactId.md)
- Called from (representative examples):
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [update_relstats_all_indexes](../u/update_relstats_all_indexes.md)
  - [do_analyze_rel](../d/do_analyze_rel.md)

## Notes and Other Information
- This function is shared by both VACUUM and ANALYZE operations
- Only updates DDL flags (hasindex, hasrules, hastriggers) when not in an outer transaction to avoid incorrect flag clearing during rollbacks
- Provides warnings when overwriting seemingly corrupt "future" transaction IDs
- The in-place update mechanism requires the statistics being updated to be fixed-size, not-null columns
- Transaction ID validation prevents relfrozenxid from going backwards unless the stored value appears to be corrupt ("in the future")

## Simplified Source

```c
void
vac_update_relstats(Relation relation,
                    BlockNumber num_pages, double num_tuples,
                    BlockNumber num_all_visible_pages,
                    bool hasindex, TransactionId frozenxid,
                    MultiXactId minmulti,
                    bool *frozenxid_updated, bool *minmulti_updated,
                    bool in_outer_xact)
{
    Oid relid = RelationGetRelid(relation);
    Relation pg_class_rel;
    ScanKeyData key[1];
    HeapTuple tuple;
    void *inplace_state;
    Form_pg_class pgc_form;
    bool dirty = false;

    // Open pg_class catalog for in-place updates
    pg_class_rel = table_open(RelationRelationId, RowExclusiveLock);

    // Find the pg_class tuple for this relation
    ScanKeyInit(&key[0], Anum_pg_class_oid, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(relid));
    systable_inplace_update_begin(pg_class_rel, ClassOidIndexId, true,
                                  NULL, 1, key, &tuple, &inplace_state);

    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "pg_class entry for relid %u vanished during vacuuming", relid);

    pgc_form = (Form_pg_class) GETSTRUCT(tuple);

    // Update basic statistics (always safe to update)
    if (pgc_form->relpages != (int32) num_pages) {
        pgc_form->relpages = (int32) num_pages;
        dirty = true;
    }
    if (pgc_form->reltuples != (float4) num_tuples) {
        pgc_form->reltuples = (float4) num_tuples;
        dirty = true;
    }
    if (pgc_form->relallvisible != (int32) num_all_visible_pages) {
        pgc_form->relallvisible = (int32) num_all_visible_pages;
        dirty = true;
    }

    // Update DDL flags, but only outside of outer transactions
    if (!in_outer_xact) {
        // Clear relhasindex if no indexes found
        if (pgc_form->relhasindex && !hasindex) {
            pgc_form->relhasindex = false;
            dirty = true;
        }

        // Clear relhasrules if no rules present
        if (pgc_form->relhasrules && relation->rd_rules == NULL) {
            pgc_form->relhasrules = false;
            dirty = true;
        }

        // Clear relhastriggers if no triggers present
        if (pgc_form->relhastriggers && relation->trigdesc == NULL) {
            pgc_form->relhastriggers = false;
            dirty = true;
        }
    }

    // Update relfrozenxid with validation
    TransactionId old_frozenxid = pgc_form->relfrozenxid;
    bool future_xid = false;
    if (frozenxid_updated)
        *frozenxid_updated = false;

    if (TransactionIdIsNormal(frozenxid) && old_frozenxid != frozenxid) {
        bool update = false;

        // Allow forward movement or correction of "future" XIDs
        if (TransactionIdPrecedes(old_frozenxid, frozenxid))
            update = true;
        else if (TransactionIdPrecedes(ReadNextTransactionId(), old_frozenxid))
            future_xid = update = true;  // Corrupt "future" XID

        if (update) {
            pgc_form->relfrozenxid = frozenxid;
            dirty = true;
            if (frozenxid_updated)
                *frozenxid_updated = true;
        }
    }

    // Update relminmxid with similar validation
    MultiXactId old_minmulti = pgc_form->relminmxid;
    bool future_mxid = false;
    if (minmulti_updated)
        *minmulti_updated = false;

    if (MultiXactIdIsValid(minmulti) && old_minmulti != minmulti) {
        bool update = false;

        if (MultiXactIdPrecedes(old_minmulti, minmulti))
            update = true;
        else if (MultiXactIdPrecedes(ReadNextMultiXactId(), old_minmulti))
            future_mxid = update = true;  // Corrupt "future" MXID

        if (update) {
            pgc_form->relminmxid = minmulti;
            dirty = true;
            if (minmulti_updated)
                *minmulti_updated = true;
        }
    }

    // Commit or cancel the in-place update
    if (dirty)
        systable_inplace_update_finish(inplace_state, tuple);
    else
        systable_inplace_update_cancel(inplace_state);

    table_close(pg_class_rel, RowExclusiveLock);

    // Warn about corrupt transaction IDs that were corrected
    if (future_xid)
        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg_internal("overwrote invalid relfrozenxid value %u with new value %u for table \"%s\"",
                               old_frozenxid, frozenxid, RelationGetRelationName(relation))));
    if (future_mxid)
        ereport(WARNING, (errcode(ERRCODE_DATA_CORRUPTED),
                errmsg_internal("overwrote invalid relminmxid value %u with new value %u for table \"%s\"",
                               old_minmulti, minmulti, RelationGetRelationName(relation))));
}
```