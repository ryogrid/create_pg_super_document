# check_publications_origin

## Location
[src/backend/commands/subscriptioncmds.c:2032-2139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L2032-L2139)

## Overview
Validates and warns when a subscription might copy data with different origins during initial synchronization with copy_data=true and origin=none settings.

## Definition

```c
static void
check_publications_origin(WalReceiverConn *wrconn, List *publications,
						  bool copydata, char *origin, Oid *subrel_local_oids,
						  int subrel_count, char *subname)
```
## Detailed Description
This function performs a critical validation check for logical replication subscriptions to detect potential data origin conflicts. When creating or refreshing a subscription with copy_data=true and origin=none, it queries the publisher to determine if any of the subscribed tables are also being written to by other subscriptions (indicating potential origin conflicts). The function constructs a complex SQL query that examines publication tables, their partition hierarchies, and existing subscription relationships to identify overlapping publications. If conflicts are detected, it logs a warning to alert administrators about potential data origin issues during initial synchronization.

## Parameters / Member Variables
- `*wrconn`: Active WAL receiver connection to the publisher database
- `*publications`: List of publication names being subscribed to
- `copydata`: Boolean indicating whether initial data copy is requested
- `*origin`: Origin setting for the subscription (checked for 'none' value)
- `*subrel_local_oids`: Array of relation OIDs already present on the subscriber
- `subrel_count`: Number of relations in the subrel_local_oids array
- `*subname`: Name of the subscription being created/modified
## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - [initStringInfo](../i/initStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [get_publications_str](../g/get_publications_str.md)
  - [get_namespace_name](../g/get_namespace_name.md)
  - [get_rel_namespace](../g/get_rel_namespace.md)
  - [get_rel_name](../g/get_rel_name.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - walrcv_exec
  - [pfree](../p/pfree.md)
  - ereport
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md)
  - [slot_getattr](../s/slot_getattr.md)
  - TextDatumGetCString
  - [ExecClearTuple](../E/ExecClearTuple.md)
  - [list_append_unique](../l/list_append_unique.md)
  - [makeString](../m/makeString.md)
  - [makeStringInfo](../m/makeStringInfo.md)
  - [errdetail_plural](../e/errdetail_plural.md)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md)
  - [walrcv_clear_result](../w/walrcv_clear_result.md)
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md)

## Notes and Other Information
- This validation only applies when copy_data=true and origin='none' to prevent silent data corruption issues
- The function excludes already synchronized tables (from subrel_local_oids) since they use WAL-based replication with origin tracking
- Uses complex SQL with partition hierarchy functions (pg_partition_ancestors, pg_partition_tree) to check parent and child table relationships
- Warning messages use plural forms to handle single vs. multiple publication scenarios appropriately
- The check is designed to be conservative - it warns even if tables might be empty, prioritizing safety over precision

## Simplified Source

```c
static void
check_publications_origin(WalReceiverConn *wrconn, List *publications,
                          bool copydata, char *origin, Oid *subrel_local_oids,
                          int subrel_count, char *subname)
{
    WalRcvExecResult *res;
    StringInfoData cmd;
    TupleTableSlot *slot;
    List *publist = NIL;

    // Early return if not checking origin conflicts
    if (!copydata || !origin || (pg_strcasecmp(origin, LOGICALREP_ORIGIN_NONE) != 0))
        return;

    // Build SQL query to find overlapping publications
    initStringInfo(&cmd);
    appendStringInfoString(&cmd,
        "SELECT DISTINCT P.pubname "
        "FROM pg_publication P, "
        "     LATERAL pg_get_publication_tables(P.pubname) GPT "
        "     JOIN pg_subscription_rel PS ON (GPT.relid = PS.srrelid OR "
        "     GPT.relid IN (SELECT relid FROM pg_partition_ancestors(PS.srrelid) UNION "
        "                   SELECT relid FROM pg_partition_tree(PS.srrelid))), "
        "     pg_class C JOIN pg_namespace N ON (N.oid = C.relnamespace) "
        "WHERE C.oid = GPT.relid AND P.pubname IN (");

    get_publications_str(publications, &cmd, true);
    appendStringInfoString(&cmd, ")");

    // Exclude already synchronized tables
    for (int i = 0; i < subrel_count; i++) {
        Oid relid = subrel_local_oids[i];
        char *schemaname = get_namespace_name(get_rel_namespace(relid));
        char *tablename = get_rel_name(relid);

        appendStringInfo(&cmd, "AND NOT (N.nspname = '%s' AND C.relname = '%s')",
                         schemaname, tablename);
    }

    // Execute query and process results
    res = walrcv_exec(wrconn, cmd.data, 1, (Oid[]){TEXTOID});
    pfree(cmd.data);

    if (res->status != WALRCV_OK_TUPLES)
        ereport(ERROR, (errmsg("could not receive list of replicated tables")));

    // Collect publication names that have conflicts
    slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
    while (tuplestore_gettupleslot(res->tuplestore, true, false, slot)) {
        char *pubname = TextDatumGetCString(slot_getattr(slot, 1, &isnull));
        publist = list_append_unique(publist, makeString(pubname));
        ExecClearTuple(slot);
    }

    // Issue warning if conflicts detected
    if (publist) {
        StringInfo pubnames = makeStringInfo();
        get_publications_str(publist, pubnames, false);

        ereport(WARNING,
            (errmsg("subscription \"%s\" requested copy_data with origin = NONE "
                    "but might copy data that had a different origin", subname),
             errdetail_plural("Publication (%s) contains tables written by other subscriptions.",
                             "Publications (%s) contain tables written by other subscriptions.",
                             list_length(publist), pubnames->data)));
    }

    ExecDropSingleTupleTableSlot(slot);
    walrcv_clear_result(res);
}
```