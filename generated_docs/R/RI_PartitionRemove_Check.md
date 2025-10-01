# RI_PartitionRemove_Check

## Location
[src/backend/utils/adt/ri_triggers.c:1654-1872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1654-L1872)

## Overview
Verifies that no foreign key references exist when a partition is detached from the referenced side of a foreign key constraint.

## Definition
```c
void RI_PartitionRemove_Check(Trigger *trigger, Relation fk_rel, Relation pk_rel)
```

## Detailed Description
This function performs referential integrity validation specifically for partition detachment operations on the referenced (primary key) side of a foreign key constraint. When a partition containing primary key values is about to be detached, this function ensures no foreign key rows in other tables would be left referencing non-existent primary key values.

The function constructs and executes a specialized INNER JOIN query that:
1. **Constraint Discovery**: Uses the partition constraint to identify rows that would be removed
2. **Reference Detection**: Finds foreign key rows that reference values in the partition being detached
3. **Query Construction**: Builds a query with partition constraint filtering
4. **Match Type Logic**: Handles different NULL behaviors (MATCH SIMPLE vs MATCH FULL)
5. **Performance Optimization**: Temporarily increases work_mem for efficient execution
6. **Violation Reporting**: Reports detailed constraint violation if any references exist

The generated query structure is:
```sql
SELECT fk.keycols FROM [ONLY] fk_table fk
JOIN pk_partition pk ON (pk.key = fk.key)
WHERE (<partition constraint>) AND (fk.key IS NOT NULL [AND/OR ...])
```

## Parameters / Member Variables
- `trigger`: The foreign key trigger containing constraint information
- `fk_rel`: The foreign key table relation that references the partition
- `pk_rel`: The partition being detached from the primary key table

## Dependencies
- Functions called/Symbols referenced:
  - [ri_FetchConstraintInfo](../r/ri_FetchConstraintInfo.md)
  - [quoteOneName](../q/quoteOneName.md), quoteRelationName
  - RIAttName, RIAttType, RIAttCollation
  - [ri_GenerateQual](../r/ri_GenerateQual.md), ri_GenerateQualCollation
  - [pg_get_partconstrdef_string](../p/pg_get_partconstrdef_string.md)
  - [SPI_connect](../S/SPI_connect.md), SPI_prepare, SPI_execute_snapshot, SPI_finish
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [heap_deform_tuple](../h/heap_deform_tuple.md), ExecStoreVirtualTuple
  - [ri_ReportViolation](../r/ri_ReportViolation.md)
  - [NewGUCNestLevel](../N/NewGUCNestLevel.md), set_config_option, AtEOXact_GUC
- Called from (representative examples):
  - [ATDetachCheckNoForeignKeyRefs](../A/ATDetachCheckNoForeignKeyRefs.md)

## Notes and Other Information
- This is a specialized function for partition management operations, not general constraint checking
- Does not perform permission checks, assuming the user detaching has sufficient privileges
- Uses INNER JOIN (not LEFT OUTER JOIN) to find existing references rather than missing ones
- Incorporates partition constraint logic to identify which rows would be affected by detachment
- Temporarily adjusts work_mem and hash_mem_multiplier for performance optimization
- Located in src/backend/utils/adt/ri_triggers.c:1654-1872
- Returns void but throws an error if any referencing rows are found
- Part of PostgreSQL's partition management and referential integrity system
- Handles both partitioned and regular foreign key tables appropriately

## Simplified Source

```c
void RI_PartitionRemove_Check(Trigger *trigger, Relation fk_rel, Relation pk_rel) {
    // Get constraint information from trigger
    const RI_ConstraintInfo *riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false);

    // Build query to find references to partition being detached
    StringInfoData querybuf;
    initStringInfo(&querybuf);

    // SELECT fk.keycols FROM [ONLY] fk_table fk JOIN pk_partition pk ON...
    appendStringInfoString(&querybuf, "SELECT ");
    for (int i = 0; i < riinfo->nkeys; i++) {
        char fkattname[MAX_QUOTED_NAME_LEN + 3];
        quoteOneName(fkattname, RIAttName(fk_rel, riinfo->fk_attnums[i]));
        appendStringInfo(&querybuf, "%sfk.%s", (i == 0) ? "" : ", ", fkattname);
    }

    // Add FROM and JOIN clauses
    char pkrelname[MAX_QUOTED_REL_NAME_LEN], fkrelname[MAX_QUOTED_REL_NAME_LEN];
    quoteRelationName(pkrelname, pk_rel);
    quoteRelationName(fkrelname, fk_rel);
    const char *fk_only = (fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) ? "" : "ONLY ";
    appendStringInfo(&querybuf, " FROM %s%s fk JOIN %s pk ON", fk_only, fkrelname, pkrelname);

    // Add join conditions for all key columns
    for (int i = 0; i < riinfo->nkeys; i++) {
        char pkattname[MAX_QUOTED_NAME_LEN + 3], fkattname[MAX_QUOTED_NAME_LEN + 3];
        strcpy(pkattname, "pk."); strcpy(fkattname, "fk.");
        quoteOneName(pkattname + 3, RIAttName(pk_rel, riinfo->pk_attnums[i]));
        quoteOneName(fkattname + 3, RIAttName(fk_rel, riinfo->fk_attnums[i]));

        ri_GenerateQual(&querybuf, (i == 0) ? "(" : "AND",
                        pkattname, RIAttType(pk_rel, riinfo->pk_attnums[i]),
                        riinfo->pf_eq_oprs[i],
                        fkattname, RIAttType(fk_rel, riinfo->fk_attnums[i]));

        // Handle collation differences
        Oid pk_coll = RIAttCollation(pk_rel, riinfo->pk_attnums[i]);
        Oid fk_coll = RIAttCollation(fk_rel, riinfo->fk_attnums[i]);
        if (pk_coll != fk_coll)
            ri_GenerateQualCollation(&querybuf, pk_coll);
    }

    // Add WHERE clause with partition constraint
    char *constraintDef = pg_get_partconstrdef_string(RelationGetRelid(pk_rel), "pk");
    if (constraintDef && constraintDef[0] != '\0')
        appendStringInfo(&querybuf, ") WHERE %s AND (", constraintDef);
    else
        appendStringInfoString(&querybuf, ") WHERE (");

    // Add NULL checks for foreign key columns (MATCH SIMPLE vs FULL)
    for (int i = 0; i < riinfo->nkeys; i++) {
        char fkattname[MAX_QUOTED_NAME_LEN + 3];
        quoteOneName(fkattname, RIAttName(fk_rel, riinfo->fk_attnums[i]));
        appendStringInfo(&querybuf, "%sfk.%s IS NOT NULL",
                        (i == 0) ? "" : (riinfo->confmatchtype == FKCONSTR_MATCH_SIMPLE) ? " AND " : " OR ",
                        fkattname);
    }
    appendStringInfoChar(&querybuf, ')');

    // Optimize work_mem for the check query
    int save_nestlevel = NewGUCNestLevel();
    char workmembuf[32];
    snprintf(workmembuf, sizeof(workmembuf), "%d", maintenance_work_mem);
    set_config_option("work_mem", workmembuf, PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);
    set_config_option("hash_mem_multiplier", "1", PGC_USERSET, PGC_S_SESSION, GUC_ACTION_SAVE, true, 0, false);

    // Execute the query to check for violations
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    SPIPlanPtr qplan = SPI_prepare(querybuf.data, 0, NULL);
    if (qplan == NULL)
        elog(ERROR, "SPI_prepare failed");

    int spi_result = SPI_execute_snapshot(qplan, NULL, NULL, GetLatestSnapshot(),
                                         InvalidSnapshot, true, false, 1);

    if (spi_result != SPI_OK_SELECT)
        elog(ERROR, "Query execution failed");

    // Report violation if any referencing rows found
    if (SPI_processed > 0) {
        TupleTableSlot *slot = MakeSingleTupleTableSlot(SPI_tuptable->tupdesc, &TTSOpsVirtual);
        HeapTuple tuple = SPI_tuptable->vals[0];

        heap_deform_tuple(tuple, SPI_tuptable->tupdesc, slot->tts_values, slot->tts_isnull);
        ExecStoreVirtualTuple(slot);

        // Create fake constraint info for reporting
        RI_ConstraintInfo fake_riinfo;
        memcpy(&fake_riinfo, riinfo, sizeof(RI_ConstraintInfo));
        for (int i = 0; i < fake_riinfo.nkeys; i++)
            fake_riinfo.pk_attnums[i] = i + 1;

        ri_ReportViolation(&fake_riinfo, pk_rel, fk_rel, slot, SPI_tuptable->tupdesc, 0, true);
    }

    // Clean up
    SPI_finish();
    AtEOXact_GUC(true, save_nestlevel);
}
```