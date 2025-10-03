# refresh_by_match_merge

## Location
[src/backend/commands/matview.c:597-887](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/matview.c#L597-L887)

## Overview
Refreshes a materialized view with transactional semantics while allowing concurrent reads by performing a diff-based merge using a full outer join between the old and new data versions.

## Definition

```c
static void
refresh_by_match_merge(Oid matviewOid, Oid tempOid, Oid relowner,
					   int save_sec_context)
```
## Detailed Description
This function implements a sophisticated materialized view refresh strategy that allows concurrent reads during the refresh operation. It works by:

1. **Creating a temporary diff table**: Performs a full outer join between the existing materialized view data and the new data (stored in a temporary table) to identify differences
2. **Duplicate detection**: Validates that the new data contains no duplicate rows without NULLs, which is essential for the diff algorithm to work correctly
3. **Unique index requirement**: Requires at least one usable unique index on the materialized view to ensure proper row identification and matching
4. **Set-based operations**: Uses efficient DELETE and INSERT operations based on the diff results rather than row-by-row processing
5. **Transactional safety**: Maintains ACID properties while allowing concurrent SELECT operations

The function leverages the behavior of NULLs in equality tests and UNIQUE indexes to correctly handle rows with NULL values. The entire operation is performed under an ExclusiveLock to prevent concurrent REFRESH operations and incremental maintenance.

## Parameters / Member Variables
- `matviewOid`: Object ID of the materialized view to refresh
- `tempOid`: Object ID of the temporary table containing the new data
- `relowner`: User ID of the relation owner for security context switching
- `save_sec_context`: Saved security context for restoration after temporary privilege changes
## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md), table_close
  - [SPI_connect](../S/SPI_connect.md), SPI_exec, SPI_execute, SPI_finish
  - [RelationGetIndexList](../R/RelationGetIndexList.md), index_open, index_close
  - [is_usable_unique_index](../i/is_usable_unique_index.md)
  - [OpenMatViewIncrementalMaintenance](../O/OpenMatViewIncrementalMaintenance.md), CloseMatViewIncrementalMaintenance
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md), generate_operator_clause
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md)
- Called from (representative examples):
  - [RefreshMatViewByOid](../R/RefreshMatViewByOid.md)

## Notes and Other Information
- Requires at least one usable unique index on the materialized view to function correctly
- Cannot handle duplicate rows without NULLs in the new data set
- Uses the Server Programming Interface (SPI) extensively for SQL operations
- Temporarily switches security context to create temporary tables outside of SECURITY_RESTRICTED_OPERATION mode
- The diff table contains both the TID of matched old records and the complete new row data as a composite type
- Performs deletes before inserts to maintain referential integrity

## Simplified Source

```c
static void
refresh_by_match_merge(Oid matviewOid, Oid tempOid, Oid relowner,
                       int save_sec_context)
{
    StringInfoData querybuf;
    Relation matviewRel, tempRel;
    char *matviewname, *tempname, *diffname;
    bool foundUniqueIndex = false;
    List *indexoidlist;
    int16 relnatts;
    Oid *opUsedForQual;

    // Open relations and prepare names
    initStringInfo(&querybuf);
    matviewRel = table_open(matviewOid, NoLock);
    matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
                                             RelationGetRelationName(matviewRel));
    tempRel = table_open(tempOid, NoLock);
    tempname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(tempRel)),
                                          RelationGetRelationName(tempRel));
    diffname = make_temptable_name_n(tempname, 2);
    relnatts = RelationGetNumberOfAttributes(matviewRel);

    // Start SPI and analyze new data
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    appendStringInfo(&querybuf, "ANALYZE %s", tempname);
    if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);

    // Check for duplicates in new data
    resetStringInfo(&querybuf);
    appendStringInfo(&querybuf,
                     "SELECT newdata.*::%s FROM %s newdata "
                     "WHERE newdata.* IS NOT NULL AND EXISTS "
                     "(SELECT 1 FROM %s newdata2 WHERE newdata2.* IS NOT NULL "
                     "AND newdata2.* OPERATOR(pg_catalog.*=) newdata.* "
                     "AND newdata2.ctid OPERATOR(pg_catalog.<>) newdata.ctid)",
                     tempname, tempname, tempname);
    if (SPI_execute(querybuf.data, false, 1) != SPI_OK_SELECT)
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);
    if (SPI_processed > 0)
        ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION),
                        errmsg("new data contains duplicate rows without null columns")));

    // Create temporary diff table
    SetUserIdAndSecContext(relowner, save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
    resetStringInfo(&querybuf);
    appendStringInfo(&querybuf, "CREATE TEMP TABLE %s (tid pg_catalog.tid)", diffname);
    if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);
    SetUserIdAndSecContext(relowner, save_sec_context | SECURITY_RESTRICTED_OPERATION);
    resetStringInfo(&querybuf);
    appendStringInfo(&querybuf, "ALTER TABLE %s ADD COLUMN newdata %s", diffname, tempname);
    if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);

    // Start building diff query
    resetStringInfo(&querybuf);
    appendStringInfo(&querybuf,
                     "INSERT INTO %s "
                     "SELECT mv.ctid AS tid, newdata.*::%s AS newdata "
                     "FROM %s mv FULL JOIN %s newdata ON (",
                     diffname, tempname, matviewname, tempname);

    // Build equality conditions from unique indexes
    opUsedForQual = (Oid *) palloc0(sizeof(Oid) * relnatts);
    indexoidlist = RelationGetIndexList(matviewRel);

    foreach(indexoidscan, indexoidlist)
    {
        Oid indexoid = lfirst_oid(indexoidscan);
        Relation indexRel = index_open(indexoid, RowExclusiveLock);

        if (is_usable_unique_index(indexRel))
        {
            Form_pg_index indexStruct = indexRel->rd_index;
            // Add equality conditions for each indexed column
            for (int i = 0; i < indexStruct->indnkeyatts; i++)
            {
                int attnum = indexStruct->indkey.values[i];
                // Get equality operator and build comparison clause
                // ... (detailed operator lookup simplified)
                foundUniqueIndex = true;
            }
        }
        index_close(indexRel, NoLock);
    }

    if (!foundUniqueIndex)
        elog(ERROR, "could not find suitable unique index on materialized view");

    // Complete diff query and execute
    appendStringInfoString(&querybuf,
                           " AND newdata.* OPERATOR(pg_catalog.*=) mv.*) "
                           "WHERE newdata.* IS NULL OR mv.* IS NULL "
                           "ORDER BY tid");
    if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);

    // Analyze diff table
    resetStringInfo(&querybuf);
    appendStringInfo(&querybuf, "ANALYZE %s", diffname);
    if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);

    OpenMatViewIncrementalMaintenance();

    // Delete old rows first
    resetStringInfo(&querybuf);
    appendStringInfo(&querybuf,
                     "DELETE FROM %s mv WHERE ctid OPERATOR(pg_catalog.=) ANY "
                     "(SELECT diff.tid FROM %s diff "
                     "WHERE diff.tid IS NOT NULL AND diff.newdata IS NULL)",
                     matviewname, diffname);
    if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);

    // Insert new rows
    resetStringInfo(&querybuf);
    appendStringInfo(&querybuf,
                     "INSERT INTO %s SELECT (diff.newdata).* "
                     "FROM %s diff WHERE tid IS NULL",
                     matviewname, diffname);
    if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);

    CloseMatViewIncrementalMaintenance();
    table_close(tempRel, NoLock);
    table_close(matviewRel, NoLock);

    // Clean up
    resetStringInfo(&querybuf);
    appendStringInfo(&querybuf, "DROP TABLE %s, %s", diffname, tempname);
    if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
        elog(ERROR, "SPI_exec failed: %s", querybuf.data);

    if (SPI_finish() != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish failed");
}
```