# RI_Initial_Check

## Location
[src/backend/utils/adt/ri_triggers.c:1359-1653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1359-L1653)

## Overview
Validates an entire table for foreign key constraint violations using a single query during ALTER TABLE ADD FOREIGN KEY operations.

## Definition
```c
bool RI_Initial_Check(Trigger *trigger, Relation fk_rel, Relation pk_rel)
```

## Detailed Description
This function performs a comprehensive foreign key constraint validation for an entire table using a single SQL query. Unlike trigger-based validation, it's specifically designed for ALTER TABLE ADD FOREIGN KEY operations to validate existing data before the constraint is established.

The function constructs and executes a complex LEFT OUTER JOIN query that:
1. **Permission Checking**: Verifies the current user has SELECT permissions on both tables
2. **RLS Handling**: Checks row-level security constraints and ownership
3. **Query Construction**: Builds a query to find FK rows that don't match any PK row
4. **Match Type Logic**: Handles different NULL behaviors (MATCH SIMPLE vs MATCH FULL)
5. **Performance Optimization**: Temporarily increases work_mem for efficient execution
6. **Violation Reporting**: Reports detailed constraint violation information if found

The generated query structure is:
```sql
SELECT fk.keycols FROM [ONLY] fk_table fk
LEFT OUTER JOIN [ONLY] pk_table pk ON (pk.key = fk.key)
WHERE pk.key IS NULL AND (fk.key IS NOT NULL [AND/OR ...])
```

## Parameters / Member Variables
- `trigger`: The foreign key trigger containing constraint information
- `fk_rel`: The foreign key table relation being validated
- `pk_rel`: The primary key table relation being referenced

## Dependencies
- Functions called/Symbols referenced:
  - [ri_FetchConstraintInfo](../r/ri_FetchConstraintInfo.md)
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md)
  - [has_bypassrls_privilege](../h/has_bypassrls_privilege.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [quoteOneName](../q/quoteOneName.md)
  - [quoteRelationName](../q/quoteRelationName.md)
  - RIAttName, RIAttType, RIAttCollation
  - [ri_GenerateQual](../r/ri_GenerateQual.md), ri_GenerateQualCollation
  - [SPI_connect](../S/SPI_connect.md), SPI_prepare, SPI_execute_snapshot, SPI_finish
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md), ExecDropSingleTupleTableSlot
  - [ri_ReportViolation](../r/ri_ReportViolation.md)
  - [ri_NullCheck](../r/ri_NullCheck.md)
- Called from (representative examples):
  - [validateForeignKeyConstraint](../v/validateForeignKeyConstraint.md)

## Notes and Other Information
- This is NOT a trigger function but a utility for constraint validation during DDL
- Returns false if permission checks fail, allowing caller to fall back to trigger method
- Temporarily adjusts work_mem and hash_mem_multiplier for performance optimization
- Uses SPI (Server Programming Interface) to execute the validation query
- Handles both partitioned and regular tables appropriately
- Located in src/backend/utils/adt/ri_triggers.c:1359-1653
- Implements sophisticated NULL handling logic based on foreign key match types
- Forces current snapshot usage to ensure data consistency during validation

## Simplified Source

```c
bool RI_Initial_Check(Trigger *trigger, Relation fk_rel, Relation pk_rel) {
    const RI_ConstraintInfo *riinfo;
    StringInfoData querybuf;
    List *rtes = NIL;
    List *perminfos = NIL;
    RTEPermissionInfo *pk_perminfo, *fk_perminfo;
    RangeTblEntry *rte;
    int save_nestlevel;
    char workmembuf[32];
    SPIPlanPtr qplan;
    int spi_result;

    // Get constraint information
    riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false);

    // Check permissions on both tables
    pk_perminfo = makeNode(RTEPermissionInfo);
    pk_perminfo->relid = RelationGetRelid(pk_rel);
    pk_perminfo->requiredPerms = ACL_SELECT;
    // ... build permission structures for both tables ...

    fk_perminfo = makeNode(RTEPermissionInfo);
    fk_perminfo->relid = RelationGetRelid(fk_rel);
    fk_perminfo->requiredPerms = ACL_SELECT;
    // ... add to permission lists ...

    // Add selected columns to permission info
    for (int i = 0; i < riinfo->nkeys; i++) {
        int attno = riinfo->pk_attnums[i] - FirstLowInvalidHeapAttributeNumber;
        pk_perminfo->selectedCols = bms_add_member(pk_perminfo->selectedCols, attno);

        attno = riinfo->fk_attnums[i] - FirstLowInvalidHeapAttributeNumber;
        fk_perminfo->selectedCols = bms_add_member(fk_perminfo->selectedCols, attno);
    }

    if (!ExecCheckPermissions(rtes, perminfos, false))
        return false;

    // Check row-level security constraints
    if (!has_bypassrls_privilege(GetUserId()) &&
        ((pk_rel->rd_rel->relrowsecurity && !object_ownercheck(...)) ||
         (fk_rel->rd_rel->relrowsecurity && !object_ownercheck(...))))
        return false;

    // Build validation query:
    // SELECT fk.keycols FROM fk_table fk
    // LEFT JOIN pk_table pk ON (pk.key = fk.key)
    // WHERE pk.key IS NULL AND (fk.key IS NOT NULL [AND/OR ...])
    initStringInfo(&querybuf);
    appendStringInfoString(&querybuf, "SELECT ");

    // Add foreign key columns to SELECT
    const char *sep = "";
    for (int i = 0; i < riinfo->nkeys; i++) {
        char fkattname[MAX_QUOTED_NAME_LEN + 3];
        quoteOneName(fkattname, RIAttName(fk_rel, riinfo->fk_attnums[i]));
        appendStringInfo(&querybuf, "%sfk.%s", sep, fkattname);
        sep = ", ";
    }

    // Add FROM and JOIN clauses
    char pkrelname[MAX_QUOTED_REL_NAME_LEN];
    char fkrelname[MAX_QUOTED_REL_NAME_LEN];
    quoteRelationName(pkrelname, pk_rel);
    quoteRelationName(fkrelname, fk_rel);

    const char *fk_only = (fk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) ? "" : "ONLY ";
    const char *pk_only = (pk_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE) ? "" : "ONLY ";

    appendStringInfo(&querybuf, " FROM %s%s fk LEFT OUTER JOIN %s%s pk ON",
                     fk_only, fkrelname, pk_only, pkrelname);

    // Add JOIN conditions with proper collations
    sep = "(";
    for (int i = 0; i < riinfo->nkeys; i++) {
        char pkattname[MAX_QUOTED_NAME_LEN + 3];
        char fkattname[MAX_QUOTED_NAME_LEN + 3];

        quoteOneName(pkattname + 3, RIAttName(pk_rel, riinfo->pk_attnums[i]));
        quoteOneName(fkattname + 3, RIAttName(fk_rel, riinfo->fk_attnums[i]));

        ri_GenerateQual(&querybuf, sep, pkattname, RIAttType(pk_rel, riinfo->pk_attnums[i]),
                       riinfo->pf_eq_oprs[i], fkattname, RIAttType(fk_rel, riinfo->fk_attnums[i]));

        // Handle collation differences
        Oid pk_coll = RIAttCollation(pk_rel, riinfo->pk_attnums[i]);
        Oid fk_coll = RIAttCollation(fk_rel, riinfo->fk_attnums[i]);
        if (pk_coll != fk_coll)
            ri_GenerateQualCollation(&querybuf, pk_coll);

        sep = "AND";
    }

    // Add WHERE clause for NULL detection and match type logic
    char pkattname[MAX_QUOTED_NAME_LEN];
    quoteOneName(pkattname, RIAttName(pk_rel, riinfo->pk_attnums[0]));
    appendStringInfo(&querybuf, ") WHERE pk.%s IS NULL AND (", pkattname);

    sep = "";
    for (int i = 0; i < riinfo->nkeys; i++) {
        char fkattname[MAX_QUOTED_NAME_LEN];
        quoteOneName(fkattname, RIAttName(fk_rel, riinfo->fk_attnums[i]));
        appendStringInfo(&querybuf, "%sfk.%s IS NOT NULL", sep, fkattname);

        // Choose AND/OR based on match type
        switch (riinfo->confmatchtype) {
            case FKCONSTR_MATCH_SIMPLE:
                sep = " AND ";
                break;
            case FKCONSTR_MATCH_FULL:
                sep = " OR ";
                break;
        }
    }
    appendStringInfoChar(&querybuf, ')');

    // Optimize memory settings for query execution
    save_nestlevel = NewGUCNestLevel();
    snprintf(workmembuf, sizeof(workmembuf), "%d", maintenance_work_mem);
    set_config_option("work_mem", workmembuf, PGC_USERSET, PGC_S_SESSION,
                     GUC_ACTION_SAVE, true, 0, false);
    set_config_option("hash_mem_multiplier", "1", PGC_USERSET, PGC_S_SESSION,
                     GUC_ACTION_SAVE, true, 0, false);

    // Execute validation query
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    qplan = SPI_prepare(querybuf.data, 0, NULL);
    if (qplan == NULL)
        elog(ERROR, "SPI_prepare returned %s for %s",
             SPI_result_code_string(SPI_result), querybuf.data);

    spi_result = SPI_execute_snapshot(qplan, NULL, NULL, GetLatestSnapshot(),
                                     InvalidSnapshot, true, false, 1);

    if (spi_result != SPI_OK_SELECT)
        elog(ERROR, "SPI_execute_snapshot returned %s", SPI_result_code_string(spi_result));

    // Check for constraint violations
    if (SPI_processed > 0) {
        // Found violating tuple - report detailed error
        TupleTableSlot *slot;
        HeapTuple tuple = SPI_tuptable->vals[0];
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        RI_ConstraintInfo fake_riinfo;

        slot = MakeSingleTupleTableSlot(tupdesc, &TTSOpsVirtual);
        heap_deform_tuple(tuple, tupdesc, slot->tts_values, slot->tts_isnull);
        ExecStoreVirtualTuple(slot);

        // Adjust column numbers for result tuple
        memcpy(&fake_riinfo, riinfo, sizeof(RI_ConstraintInfo));
        for (int i = 0; i < fake_riinfo.nkeys; i++)
            fake_riinfo.fk_attnums[i] = i + 1;

        // Handle MATCH FULL null validation
        if (fake_riinfo.confmatchtype == FKCONSTR_MATCH_FULL &&
            ri_NullCheck(tupdesc, slot, &fake_riinfo, false) != RI_KEYS_NONE_NULL) {
            ereport(ERROR, (errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
                           errmsg("MATCH FULL does not allow mixing of null and nonnull key values")));
        }

        // Report constraint violation
        ri_ReportViolation(&fake_riinfo, pk_rel, fk_rel, slot, tupdesc,
                          RI_PLAN_CHECK_LOOKUPPK, false);

        ExecDropSingleTupleTableSlot(slot);
    }

    SPI_finish();
    AtEOXact_GUC(true, save_nestlevel);  // Restore GUC settings

    return true;  // No violations found
}
```