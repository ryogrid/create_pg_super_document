# ReplicationSlotDropAtPubNode

## Location
[src/backend/commands/subscriptioncmds.c:1844-1898](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/subscriptioncmds.c#L1844-L1898)

## Overview
ReplicationSlotDropAtPubNode drops a replication slot on the publisher node through an established replication connection, with support for graceful error handling.

## Definition

```c
void
ReplicationSlotDropAtPubNode(WalReceiverConn *wrconn, char *slotname, bool missing_ok)
```
## Detailed Description
ReplicationSlotDropAtPubNode executes a DROP_REPLICATION_SLOT command on the remote publisher node via the established WAL receiver connection. This function is a critical component of subscription cleanup and maintenance operations.

The function constructs and executes a DROP_REPLICATION_SLOT SQL command with the WAIT option, which ensures the operation completes before returning. It handles three possible outcomes:
1. Success (WALRCV_OK_COMMAND): Reports success via NOTICE message
2. Slot not found with missing_ok=true: Logs the issue but doesn't fail 
3. Any other error: Reports ERROR and fails the operation

The function uses PG_TRY/PG_FINALLY blocks to ensure proper memory cleanup of the command string regardless of execution outcome. This is essential for preventing memory leaks during error conditions.

## Parameters / Member Variables
- `*wrconn`: Active WAL receiver connection to the publisher node where the slot exists
- `*slotname`: Name of the replication slot to be dropped on the publisher
- `missing_ok`: If true, treat missing slot as non-fatal and log instead of erroring
## Dependencies
- Functions called/Symbols referenced:
  - walrcv_exec: Executes SQL command on the remote publisher via replication connection
  - [quote_identifier](../q/quote_identifier.md): Safely quotes the slot name to prevent SQL injection
  - [walrcv_clear_result](../w/walrcv_clear_result.md): Cleans up result structure after command execution  
  - [load_file](../l/load_file.md): Loads libpqwalreceiver library for WAL receiver functionality
- Called from (representative examples):
  - [DropSubscription](../D/DropSubscription.md): During subscription cleanup in subscriptioncmds.c:1814, 1825
  - [process_syncing_tables_for_sync](../p/process_syncing_tables_for_sync.md): During tablesync cleanup in tablesync.c:345
  - [LogicalRepSyncTableStart](../L/LogicalRepSyncTableStart.md): During tablesync initialization in tablesync.c:1396

## Notes and Other Information
- Uses the WAIT option to ensure synchronous completion of the slot drop operation
- Provides different error reporting levels based on the missing_ok parameter for flexible error handling
- Essential for proper cleanup during subscription drops and tablesync operations
- Memory management handled via PG_TRY/PG_FINALLY to prevent leaks on errors
- Requires an active replication connection to function - does not establish connections itself
- Part of the logical replication infrastructure for managing publisher-side resources from subscribers

## Simplified Source

```c
void ReplicationSlotDropAtPubNode(WalReceiverConn *wrconn, char *slotname, bool missing_ok)
{
    StringInfoData cmd;

    // Load replication library and build DROP command
    load_file("libpqwalreceiver", false);
    initStringInfo(&cmd);
    appendStringInfo(&cmd, "DROP_REPLICATION_SLOT %s WAIT", quote_identifier(slotname));

    PG_TRY();
    {
        // Execute DROP command on publisher
        WalRcvExecResult *res = walrcv_exec(wrconn, cmd.data, 0, NULL);

        if (res->status == WALRCV_OK_COMMAND) {
            // Success - report slot dropped
            ereport(NOTICE, (errmsg("dropped replication slot \"%s\" on publisher", slotname)));
        }
        else if (res->status == WALRCV_ERROR && missing_ok &&
                 res->sqlstate == ERRCODE_UNDEFINED_OBJECT) {
            // Slot not found but missing_ok=true - just log
            ereport(LOG, (errmsg("could not drop replication slot \"%s\" on publisher: %s",
                                slotname, res->err)));
        }
        else {
            // Other error - fail with ERROR
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                          errmsg("could not drop replication slot \"%s\" on publisher: %s",
                                slotname, res->err)));
        }

        walrcv_clear_result(res);
    }
    PG_FINALLY();
    {
        // Clean up command string
        pfree(cmd.data);
    }
    PG_END_TRY();
}
```