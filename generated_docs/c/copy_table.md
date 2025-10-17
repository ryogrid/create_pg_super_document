# copy_table

## Location
[src/backend/replication/logical/tablesync.c:1141-1292](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/tablesync.c#L1141-L1292)

## Overview
Performs the initial data synchronization for a table in logical replication by copying all existing data from the publisher to the subscriber using PostgreSQL's COPY protocol.

## Definition

```c
static void
copy_table(Relation rel)
```
## Detailed Description
The  function is a central component of PostgreSQL's logical replication initial table synchronization process. It orchestrates the complete process of copying existing data from a table on the publisher to the corresponding table on the subscriber.

The function operates through several coordinated phases:

1. **Remote Table Discovery**: Uses  to gather comprehensive metadata about the publisher table, including column information, data types, and any row filter expressions.

2. **Relation Mapping**: Updates the logical replication relation map and opens the local relation mapping to ensure proper attribute correspondence between publisher and subscriber tables.

3. **COPY Command Construction**: Builds an appropriate COPY command based on table characteristics:
   - For regular tables without row filters: Uses direct 
   - For views, partitioned tables, or tables with row filters: Uses  with proper WHERE clauses
   - Handles column lists to exclude generated columns and include only replicated columns
   - Combines multiple row filter expressions using OR logic

4. **Format Handling**: Supports both text and binary COPY formats, with binary format available for PostgreSQL 16+ publishers when enabled in subscription settings.

5. **Data Transfer Execution**: Initiates the COPY operation on the publisher and sets up the local COPY FROM process using  as the data source callback.

6. **Transaction Management**: Coordinates with PostgreSQL's COPY infrastructure to ensure transactional consistency during the data transfer process.

The function is designed to handle various table types (regular tables, views, partitioned tables) and supports advanced features like selective column replication and row-level filtering introduced in PostgreSQL 15.

## Parameters / Member Variables
- `rel`: Pointer to the local Relation structure representing the subscriber table that will receive the copied data. The caller is responsible for ensuring this relation is properly locked.
## Dependencies
- Functions called/Symbols referenced:
  - [fetch_remote_table_info](../f/fetch_remote_table_info.md) (retrieves publisher table metadata)
  - [logicalrep_relmap_update](../l/logicalrep_relmap_update.md) (updates relation mapping)
  - [logicalrep_rel_open](../l/logicalrep_rel_open.md), logicalrep_rel_close (manages relation mapping lifecycle)
  - [make_copy_attnamelist](../m/make_copy_attnamelist.md) (creates column name list for COPY)
  - [copy_read_data](copy_read_data.md) (data source callback for COPY FROM)
  - [BeginCopyFrom](../B/BeginCopyFrom.md), CopyFrom (PostgreSQL COPY infrastructure)
  - walrcv_exec (executes COPY command on publisher)
  - [make_parsestate](../m/make_parsestate.md) (creates parser state for COPY)
  - [quote_qualified_identifier](../q/quote_qualified_identifier.md), quote_identifier (SQL identifier quoting)
  - Various PostgreSQL utility functions for string manipulation and memory management

- Called from (representative examples):
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md) (initiates table synchronization process)

## Notes and Other Information
- Located in src/backend/replication/logical/tablesync.c:1141-1292
- This is a static helper function used internally within the tablesync module
- Handles both simple and complex COPY scenarios based on table characteristics
- Supports version-specific features by checking publisher PostgreSQL version
- Implements comprehensive error handling for connection failures and COPY operations
- Uses a global  StringInfo structure for efficient data buffering during COPY
- Critical for maintaining data consistency during initial subscription setup
- The function assumes the local relation is already locked by the caller
- Properly cleans up resources including relation mappings and temporary structures
- Supports inheritance hierarchies by using ONLY clause for regular tables to avoid duplicating child table data
- Performance-critical function that must efficiently handle tables with millions of rows during initial sync

## Simplified Source

```c
static void copy_table(Relation rel)
{
    LogicalRepRelMapEntry *relmapentry;
    LogicalRepRelation lrel;
    List *qual = NIL;
    WalRcvExecResult *res;
    StringInfoData cmd;
    CopyFromState cstate;
    List *attnamelist;
    ParseState *pstate;
    List *options = NIL;

    // Step 1: Get publisher table metadata
    fetch_remote_table_info(get_namespace_name(RelationGetNamespace(rel)),
                           RelationGetRelationName(rel), &lrel, &qual);

    // Step 2: Update relation mapping
    logicalrep_relmap_update(&lrel);
    relmapentry = logicalrep_rel_open(lrel.remoteid, NoLock);
    Assert(rel == relmapentry->localrel);

    // Step 3: Build COPY command based on table characteristics
    initStringInfo(&cmd);

    if (lrel.relkind == RELKIND_RELATION && qual == NIL) {
        // Simple case: regular table with no row filters
        appendStringInfo(&cmd, "COPY %s",
                        quote_qualified_identifier(lrel.nspname, lrel.relname));

        // Add column list if needed
        if (lrel.natts) {
            appendStringInfoString(&cmd, " (");
            for (int i = 0; i < lrel.natts; i++) {
                if (i > 0)
                    appendStringInfoString(&cmd, ", ");
                appendStringInfoString(&cmd, quote_identifier(lrel.attnames[i]));
            }
            appendStringInfoChar(&cmd, ')');
        }
        appendStringInfoString(&cmd, " TO STDOUT");
    } else {
        // Complex case: views, partitioned tables, or tables with row filters
        appendStringInfoString(&cmd, "COPY (SELECT ");

        // Build column list (excluding generated columns)
        for (int i = 0; i < lrel.natts; i++) {
            appendStringInfoString(&cmd, quote_identifier(lrel.attnames[i]));
            if (i < lrel.natts - 1)
                appendStringInfoString(&cmd, ", ");
        }

        appendStringInfoString(&cmd, " FROM ");
        if (lrel.relkind == RELKIND_RELATION)
            appendStringInfoString(&cmd, "ONLY ");
        appendStringInfoString(&cmd, quote_qualified_identifier(lrel.nspname, lrel.relname));

        // Add row filters (OR'ed together)
        if (qual != NIL) {
            ListCell *lc;
            char *q = strVal(linitial(qual));
            appendStringInfo(&cmd, " WHERE %s", q);
            for_each_from(lc, qual, 1) {
                q = strVal(lfirst(lc));
                appendStringInfo(&cmd, " OR %s", q);
            }
            list_free_deep(qual);
        }
        appendStringInfoString(&cmd, ") TO STDOUT");
    }

    // Step 4: Add binary format option for PG 16+ if enabled
    if (walrcv_server_version(LogRepWorkerWalRcvConn) >= 160000 &&
        MySubscription->binary) {
        appendStringInfoString(&cmd, " WITH (FORMAT binary)");
        options = list_make1(makeDefElem("format",
                                       (Node *) makeString("binary"), -1));
    }

    // Step 5: Execute COPY command on publisher
    res = walrcv_exec(LogRepWorkerWalRcvConn, cmd.data, 0, NULL);
    pfree(cmd.data);

    if (res->status != WALRCV_OK_COPY_OUT)
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                errmsg("could not start initial contents copy for table \"%s.%s\"",
                       lrel.nspname, lrel.relname)));
    walrcv_clear_result(res);

    // Step 6: Set up local COPY FROM with callback
    copybuf = makeStringInfo();
    pstate = make_parsestate(NULL);
    addRangeTableEntryForRelation(pstate, rel, AccessShareLock,
                                 NULL, false, false);
    attnamelist = make_copy_attnamelist(relmapentry);
    cstate = BeginCopyFrom(pstate, rel, NULL, NULL, false,
                          copy_read_data, attnamelist, options);

    // Step 7: Execute the copy operation
    CopyFrom(cstate);

    // Step 8: Cleanup
    logicalrep_rel_close(relmapentry, NoLock);
}
```