# check_publications

## Location
[src/backend/commands/subscriptioncmds.c:486-548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L486-L548)

## Overview
Validates that all specified publications exist on the publisher server by querying the remote pg_publication catalog and reports warnings for any missing publications.

## Definition

```c
struct_array_builtin(datums, list_length(publist), TEXTOID);
```
## Detailed Description
This function verifies the existence of publications on the publisher side during subscription creation or modification. It constructs and executes a SQL query against the publisher's pg_publication catalog to check if all specified publications are present. The function uses the WAL receiver connection to communicate with the publisher.

The validation process involves:
1. Building a SQL query with an IN clause containing all publication names
2. Executing the query on the publisher using the WAL receiver connection
3. Processing the returned tuples to identify which publications exist
4. Comparing the results against the originally requested list
5. Reporting warnings for any publications that don't exist on the publisher

This validation helps prevent subscription creation with non-existent publications, which would lead to replication failures. The function issues warnings rather than errors, allowing administrators to proceed if they understand the implications.

## Parameters / Member Variables
- : Active WAL receiver connection to the publisher server
- : List of publication names to validate on the publisher

## Dependencies
- Functions called/Symbols referenced:
  - [get_publications_str](../g/get_publications_str.md): Formats publication list for SQL query
  - walrcv_exec: Executes SQL query on publisher via WAL receiver
  - [makeStringInfo](../m/makeStringInfo.md)/destroyStringInfo: String buffer management
  - [MakeSingleTupleTableSlot](../M/MakeSingleTupleTableSlot.md)/ExecDropSingleTupleTableSlot: Tuple processing utilities
  - [tuplestore_gettupleslot](../t/tuplestore_gettupleslot.md): Retrieves query result tuples
  - [list_copy](../l/list_copy.md)/list_delete: List manipulation for tracking missing publications
  - TextDatumGetCString: Extracts string data from query results
- Called from (representative examples):
  - [CreateSubscription](../C/CreateSubscription.md): During subscription creation to validate publication list

## Notes and Other Information
- The function issues warnings rather than errors for missing publications, allowing subscription creation to proceed
- Uses errmsg_plural to provide grammatically correct error messages for single vs multiple missing publications
- The SQL query targets pg_catalog.pg_publication to ensure it works across PostgreSQL versions
- Creates a copy of the original publication list to track which publications are found during result processing
- [Query](../Q/Query.md) results are processed using tuple table slots for efficient data access
- WAL receiver connection must be established and valid before calling this function

## Simplified Source

```c
static void
check_publications(WalReceiverConn *wrconn, List *publications)
{
    WalRcvExecResult *res;
    StringInfo cmd;
    TupleTableSlot *slot;
    List *publicationsCopy = NIL;
    Oid tableRow[1] = {TEXTOID};

    // Build query to check publications on publisher
    cmd = makeStringInfo();
    appendStringInfoString(cmd, "SELECT t.pubname FROM\n"
                              " pg_catalog.pg_publication t WHERE\n"
                              " t.pubname IN (");
    get_publications_str(publications, cmd, true);
    appendStringInfoChar(cmd, ')');

    // Execute query on publisher
    res = walrcv_exec(wrconn, cmd->data, 1, tableRow);
    destroyStringInfo(cmd);

    if (res->status != WALRCV_OK_TUPLES)
        ereport(ERROR,
                errmsg("could not receive list of publications from the publisher: %s",
                       res->err));

    // Keep track of which publications we need to find
    publicationsCopy = list_copy(publications);

    // Process query results
    slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
    while (tuplestore_gettupleslot(res->tuplestore, true, false, slot))
    {
        char *pubname;
        bool isnull;

        // Extract publication name from result
        pubname = TextDatumGetCString(slot_getattr(slot, 1, &isnull));
        Assert(!isnull);

        // Remove found publication from our tracking list
        publicationsCopy = list_delete(publicationsCopy, makeString(pubname));
        ExecClearTuple(slot);
    }

    // Clean up
    ExecDropSingleTupleTableSlot(slot);
    walrcv_clear_result(res);

    // Report any missing publications
    if (list_length(publicationsCopy))
    {
        StringInfo pubnames = makeStringInfo();

        get_publications_str(publicationsCopy, pubnames, false);
        ereport(WARNING,
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg_plural("publication %s does not exist on the publisher",
                             "publications %s do not exist on the publisher",
                             list_length(publicationsCopy),
                             pubnames->data));
    }
}
```