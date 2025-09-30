# fetch_table_list

## Location
[src/backend/commands/subscriptioncmds.c:2140-2247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L2140-L2247)

## Overview
Retrieves the complete list of tables and their attributes from specified publications on the publisher database for logical replication setup.

## Definition

```c
static List *
fetch_table_list(WalReceiverConn *wrconn, List *publications)
```
## Detailed Description
This function queries the publisher database to obtain a comprehensive list of all tables included in the specified publications. It adapts its behavior based on the PostgreSQL server version to leverage newer features and optimize performance. For PostgreSQL 16+, it uses the enhanced pg_get_publication_tables function that can handle multiple publications and automatically filters partition tables whose ancestors are already published. For older versions, it queries pg_publication_tables directly. The function also handles column list information when supported (PostgreSQL 15+), while enforcing the constraint that tables cannot have different column lists across different publications to avoid data inconsistency issues.

## Parameters / Member Variables
- : Active WAL receiver connection to the publisher database
- : List of publication names to query for table information

## Dependencies
- Functions called/Symbols referenced:
  - walrcv_server_version
  - [initStringInfo](../i/initStringInfo.md)
  - [get_publications_str](../g/get_publications_str.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [pfree](../p/pfree.md)
  - walrcv_exec
  - ereport
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md)
  - [slot_getattr](../s/slot_getattr.md)
  - TextDatumGetCString
  - [makeRangeVar](../m/makeRangeVar.md)
  - [list_member](../l/list_member.md)
  - [lappend](../l/lappend.md)
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [walrcv_clear_result](../w/walrcv_clear_result.md)
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md)

## Notes and Other Information
- Uses version-aware SQL queries to optimize performance on newer PostgreSQL versions (16+)
- Enforces column list consistency across publications to prevent data synchronization conflicts
- Returns a list of RangeVar structures representing qualified table names (schema.table)
- Automatically handles partition hierarchy filtering in PostgreSQL 16+ to avoid duplicate table entries
- Column list support is conditional based on server version capabilities (PostgreSQL 15+)
- Validates that identical tables don't have conflicting column specifications across different publications

## Simplified Source

```c
static List *
fetch_table_list(WalReceiverConn *wrconn, List *publications)
{
    WalRcvExecResult *res;
    StringInfoData cmd;
    TupleTableSlot *slot;
    List *tablelist = NIL;
    int server_version = walrcv_server_version(wrconn);
    bool check_columnlist = (server_version >= 150000);

    initStringInfo(&cmd);

    // Build version-specific query for table list
    if (server_version >= 160000) {
        // PostgreSQL 16+: Use enhanced pg_get_publication_tables
        StringInfoData pub_names;
        initStringInfo(&pub_names);
        get_publications_str(publications, &pub_names, true);

        appendStringInfo(&cmd,
            "SELECT DISTINCT n.nspname, c.relname, gpt.attrs "
            "FROM pg_class c "
            "  JOIN pg_namespace n ON n.oid = c.relnamespace "
            "  JOIN ( SELECT (pg_get_publication_tables(VARIADIC array_agg(pubname::text))).* "
            "         FROM pg_publication WHERE pubname IN ( %s )) AS gpt "
            "       ON gpt.relid = c.oid",
            pub_names.data);

        pfree(pub_names.data);
    } else {
        // Older versions: Query pg_publication_tables directly
        appendStringInfoString(&cmd, "SELECT DISTINCT t.schemaname, t.tablename");

        if (check_columnlist)
            appendStringInfoString(&cmd, ", t.attnames");

        appendStringInfoString(&cmd, " FROM pg_catalog.pg_publication_tables t WHERE t.pubname IN (");
        get_publications_str(publications, &cmd, true);
        appendStringInfoChar(&cmd, ')');
    }

    // Execute query
    Oid tableRow[3] = {TEXTOID, TEXTOID, server_version >= 160000 ? INT2VECTOROID : NAMEARRAYOID};
    res = walrcv_exec(wrconn, cmd.data, check_columnlist ? 3 : 2, tableRow);
    pfree(cmd.data);

    if (res->status != WALRCV_OK_TUPLES)
        ereport(ERROR, (errmsg("could not receive list of replicated tables from the publisher")));

    // Process result rows into RangeVar list
    slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
    while (tuplestore_gettupleslot(res->tuplestore, true, false, slot)) {
        char *nspname = TextDatumGetCString(slot_getattr(slot, 1, &isnull));
        char *relname = TextDatumGetCString(slot_getattr(slot, 2, &isnull));
        RangeVar *rv = makeRangeVar(nspname, relname, -1);

        // Check for column list conflicts across publications
        if (check_columnlist && list_member(tablelist, rv))
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("cannot use different column lists for table \"%s.%s\" in different publications",
                        nspname, relname)));
        else
            tablelist = lappend(tablelist, rv);

        ExecClearTuple(slot);
    }

    ExecDropSingleTupleTableSlot(slot);
    walrcv_clear_result(res);
    return tablelist;
}
```