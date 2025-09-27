# exec_replication_command

## Location
[src/backend/replication/walsender.c:1992-2224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walsender.c#L1992-L2224)

## Overview
exec_replication_command is the central dispatcher for processing incoming replication commands from clients, handling the full lifecycle from parsing to execution of various PostgreSQL replication protocol commands.

## Definition
bool exec_replication_command(const char *cmd_string)

## Detailed Description
exec_replication_command serves as the main entry point for processing replication protocol commands in PostgreSQL's WAL sender process. The function implements a comprehensive command processing pipeline:

1. **State Management**: Checks and updates WAL sender state, particularly handling the transition to stopping mode when shutdown is requested.

2. **Safety Checks**: Prevents execution of commands that could generate WAL during shutdown checkpoint writing and validates transaction state.

3. **Command Recognition**: Uses the replication scanner to determine if the incoming command is a valid replication command versus a regular SQL command.

4. **Memory Management**: Creates a dedicated memory context for command processing to ensure proper cleanup.

5. **Parsing and Validation**: Parses replication commands using the specialized replication parser and validates syntax.

6. **Monitoring Integration**: Reports command activity to PostgreSQL's monitoring systems and handles logging based on configuration.

7. **Command Dispatch**: Implements a comprehensive switch statement that routes parsed commands to their specific handler functions.

The function supports a wide range of replication commands including:
- IDENTIFY_SYSTEM: System identification
- CREATE/DROP/ALTER_REPLICATION_SLOT: Slot management
- START_REPLICATION: Initiating physical/logical replication
- BASE_BACKUP: Full database backup
- TIMELINE_HISTORY: Timeline information
- READ_REPLICATION_SLOT: Slot inspection
- SHOW: Variable display
- UPLOAD_MANIFEST: Manifest upload

## Parameters / Member Variables
- `cmd_string`: The raw replication command string received from the client

## Dependencies
- Functions called/Symbols referenced:
  - [WalSndSetState](../W/WalSndSetState.md), WALSNDSTATE_STOPPING
  - [SnapBuildClearExportedSnapshot](../S/SnapBuildClearExportedSnapshot.md)
  - AllocSetContextCreate, MemoryContextDelete
  - replication_scanner_init, replication_scanner_finish, replication_scanner_is_replication_command
  - replication_yyparse
  - [pgstat_report_activity](../p/pgstat_report_activity.md), STATE_RUNNING
  - [IsAbortedTransactionBlockState](../I/IsAbortedTransactionBlockState.md)
  - [PreventInTransactionBlock](../P/PreventInTransactionBlock.md)
  - [StartTransactionCommand](../S/StartTransactionCommand.md), CommitTransactionCommand
  - [set_ps_display](../s/set_ps_display.md), EndReplicationCommand
  - [Command](../C/Command.md)-specific handlers: IdentifySystem, ReadReplicationSlot, SendBaseBackup, CreateReplicationSlot, DropReplicationSlot, AlterReplicationSlot, StartReplication, StartLogicalReplication, SendTimeLineHistory, GetPGVariable, UploadManifest
- Called from (representative examples):
  - [PostgresMain](../P/PostgresMain.md) (at src/backend/tcop/postgres.c:4763)
  - [CRSSnapshotAction](../C/CRSSnapshotAction.md) (referenced in src/include/replication/walsender.h:39)

## Notes and Other Information
- Returns true if the command was recognized and processed as a replication command, false if it should be handled as a regular SQL command
- The function provides a clear separation between replication protocol commands and standard SQL commands
- Includes comprehensive error handling for various failure modes including parser errors, aborted transactions, and invalid states
- Memory management is carefully handled with dedicated contexts to prevent leaks during command processing
- The logging mechanism respects the log_replication_commands configuration parameter
- Transaction management varies by command type, with some commands requiring transaction blocks while others explicitly prevent them
- The function is critical for PostgreSQL's replication infrastructure, serving as the gateway for all replication protocol interactions

## Simplified Source

```c
// Simplified version of exec_replication_command
bool exec_replication_command(const char *cmd_string) {
    Node *cmd_node;
    const char *cmdtag;
    MemoryContext cmd_context, old_context;

    // State management: Handle shutdown mode
    if (got_STOPPING)
        WalSndSetState(WALSNDSTATE_STOPPING);

    // Safety check: Prevent commands during shutdown
    if (MyWalSnd->state == WALSNDSTATE_STOPPING)
        ereport(ERROR, "cannot execute new commands while WAL sender is in stopping mode");

    // Cleanup previous command artifacts
    SnapBuildClearExportedSnapshot();
    CHECK_FOR_INTERRUPTS();

    // Create memory context for command processing
    cmd_context = AllocSetContextCreate(CurrentMemoryContext, "Replication command context", ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(cmd_context);

    // Initialize scanner and check if this is a replication command
    replication_scanner_init(cmd_string);
    if (!replication_scanner_is_replication_command()) {
        // Not a replication command - cleanup and return false
        replication_scanner_finish();
        MemoryContextSwitchTo(old_context);
        MemoryContextDelete(cmd_context);

        // Error if trying to run SQL in physical replication mode
        if (MyDatabaseId == InvalidOid)
            ereport(ERROR, "cannot execute SQL commands in WAL sender for physical replication");

        return false;
    }

    // Parse the replication command
    if (replication_yyparse() != 0)
        ereport(ERROR, "replication command parser error");

    replication_scanner_finish();
    cmd_node = replication_parse_result;

    // Setup monitoring and logging
    debug_query_string = cmd_string;
    pgstat_report_activity(STATE_RUNNING, cmd_string);
    ereport(log_replication_commands ? LOG : DEBUG1, "received replication command: %s", cmd_string);

    // Safety check: Disallow commands in aborted transactions
    if (IsAbortedTransactionBlockState())
        ereport(ERROR, "current transaction is aborted, commands ignored until end of transaction block");

    // Initialize communication buffers
    initStringInfo(&output_message);
    initStringInfo(&reply_message);
    initStringInfo(&tmpbuf);

    // Dispatch command to appropriate handler
    switch (cmd_node->type) {
        case T_IdentifySystemCmd:
            cmdtag = "IDENTIFY_SYSTEM";
            set_ps_display(cmdtag);
            IdentifySystem();
            EndReplicationCommand(cmdtag);
            break;

        case T_CreateReplicationSlotCmd:
            cmdtag = "CREATE_REPLICATION_SLOT";
            set_ps_display(cmdtag);
            CreateReplicationSlot((CreateReplicationSlotCmd *) cmd_node);
            EndReplicationCommand(cmdtag);
            break;

        case T_DropReplicationSlotCmd:
            cmdtag = "DROP_REPLICATION_SLOT";
            set_ps_display(cmdtag);
            DropReplicationSlot((DropReplicationSlotCmd *) cmd_node);
            EndReplicationCommand(cmdtag);
            break;

        case T_StartReplicationCmd: {
            StartReplicationCmd *cmd = (StartReplicationCmd *) cmd_node;
            cmdtag = "START_REPLICATION";
            set_ps_display(cmdtag);
            PreventInTransactionBlock(true, cmdtag);

            // Choose physical or logical replication
            if (cmd->kind == REPLICATION_KIND_PHYSICAL)
                StartReplication(cmd);
            else
                StartLogicalReplication(cmd);

            EndReplicationCommand(cmdtag);
            break;
        }

        case T_BaseBackupCmd:
            cmdtag = "BASE_BACKUP";
            set_ps_display(cmdtag);
            PreventInTransactionBlock(true, cmdtag);
            SendBaseBackup((BaseBackupCmd *) cmd_node, uploaded_manifest);
            EndReplicationCommand(cmdtag);
            break;

        // Additional cases for other replication commands...
        // (ReadReplicationSlot, AlterReplicationSlot, TimeLineHistory,
        //  VariableShow, UploadManifest)

        default:
            elog(ERROR, "unrecognized replication command node tag: %u", cmd_node->type);
    }

    // Cleanup and return success
    MemoryContextSwitchTo(old_context);
    MemoryContextDelete(cmd_context);
    debug_query_string = NULL;

    return true;
}
```

Key simplifications made:
- Removed detailed error handling structure for clarity
- Consolidated repetitive command handler patterns
- Simplified memory management context switching
- Abstracted complex error reporting to essential messages
- Focused on the main execution flow and command dispatch logic
- Removed some less common command cases for brevity while keeping the most important ones
- Simplified transaction management calls