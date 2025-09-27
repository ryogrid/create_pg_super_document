# DropReplicationSlot

## Location
[src/bin/pg_basebackup/streamutil.c:763-811](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/streamutil.c#L763-L811)

## Overview
Removes an existing replication slot by name, with optional waiting behavior for active slots.

## Definition

```c
bool
DropReplicationSlot(PGconn *conn, const char *slot_name)
```
## Detailed Description
This function is a simple wrapper that drops a replication slot by calling the core ReplicationSlotDrop function. It translates the command parameters, particularly the wait flag, to control whether the operation should wait for an active slot to become inactive before dropping it, or fail immediately if the slot is currently in use.

## Parameters / Member Variables
- `cmd`: DropReplicationSlotCmd structure containing drop parameters including:
  - `slotname`: Name of the replication slot to drop
  - `wait`: Boolean flag indicating whether to wait for active slots (if true, waits; if false, fails immediately for active slots)

## Dependencies
- Functions called/Symbols referenced:
  - [ReplicationSlotDrop](../R/ReplicationSlotDrop.md) - Core function that performs the actual slot deletion
  - [DropReplicationSlotCmd](DropReplicationSlotCmd.md) - [Command](../C/Command.md) structure type
- Called from (representative examples):
  - [exec_replication_command](../e/exec_replication_command.md) (walsender.c:2138)
  - [main](../m/main.md) (pg_receivewal.c:880, pg_recvlogical.c:974)

## Notes and Other Information
- This is a thin wrapper function that delegates the actual work to ReplicationSlotDrop
- The wait parameter is inverted when passed to ReplicationSlotDrop (cmd->wait becomes !cmd->wait for the nowait parameter)
- If wait is false, the operation will fail if the slot is currently active/in use
- If wait is true, the operation will block until the slot becomes inactive, then drop it
- Used by both physical and logical replication slot management
- Part of the replication protocol command processing in WAL sender processes

## Simplified Source

```c
// Simplified version of DropReplicationSlot
bool DropReplicationSlot(PGconn *conn, const char *slot_name) {
    PQExpBuffer query;
    PGresult *res;

    // Validate input
    Assert(slot_name != NULL);

    // Build the DROP_REPLICATION_SLOT command
    query = createPQExpBuffer();
    appendPQExpBuffer(query, "DROP_REPLICATION_SLOT \"%s\"", slot_name);

    // Execute the command
    res = PQexec(conn, query->data);

    // Check if command executed successfully
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        // Log error and cleanup
        pg_log_error("could not send replication command \"%s\": %s",
                     query->data, PQerrorMessage(conn));
        cleanup_and_return_false();
    }

    // Verify result format (should be empty)
    if (PQntuples(res) != 0 || PQnfields(res) != 0) {
        // Log unexpected result format and cleanup
        pg_log_error("could not drop replication slot \"%s\": got %d rows and %d fields, expected %d rows and %d fields",
                     slot_name, PQntuples(res), PQnfields(res), 0, 0);
        cleanup_and_return_false();
    }

    // Success - cleanup and return true
    destroyPQExpBuffer(query);
    PQclear(res);
    return true;
}
```

Key simplifications made:
- Consolidated error handling into a conceptual `cleanup_and_return_false()` function
- Added descriptive comments for each major step
- Grouped related operations together logically
- Focused on the main execution path: build query → execute → validate → cleanup
- Preserved all essential error checking and resource management