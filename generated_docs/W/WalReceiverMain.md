# WalReceiverMain

## Location
[src/backend/replication/walreceiver.c:183-664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/walreceiver.c#L183-L664)

## Overview
WalReceiverMain is the main entry point for the WAL receiver process that handles streaming replication from a primary PostgreSQL server to a standby server.

## Definition
void WalReceiverMain(char *startup_data, size_t startup_data_len)

## Detailed Description
This function implements the core logic of the WAL receiver process in PostgreSQL's streaming replication system. It establishes a connection to the primary server, manages the streaming of WAL (Write-Ahead Log) records, and handles various operational states throughout the replication lifecycle.

The function operates in several key phases:
1. **Initialization**: Sets up process type, shared memory state, and signal handlers
2. **Connection establishment**: Connects to the primary server using provided connection information
3. **System validation**: Verifies system identifiers and timeline compatibility between primary and standby
4. **Timeline management**: Fetches missing timeline history files to maintain consistency
5. **Streaming loop**: Continuously receives WAL data, processes messages, and manages timeouts
6. **Error handling**: Gracefully handles connection issues, timeouts, and shutdown requests

The function runs in an infinite loop, streaming WAL data until instructed to restart with new parameters or until termination. It manages temporary replication slots when requested and maintains status communication with both the startup process and the primary server.

## Parameters / Member Variables
- : Startup data passed to the process (currently unused, expected to be NULL)
- : Length of startup data (expected to be 0)

## Dependencies
- Functions called/Symbols referenced:
  - [AuxiliaryProcessMainCommon](../A/AuxiliaryProcessMainCommon.md)
  - walrcv_connect, walrcv_identify_system, walrcv_startstreaming
  - [ProcessWalRcvInterrupts](../P/ProcessWalRcvInterrupts.md)
  - [WalRcvFetchTimeLineHistoryFiles](WalRcvFetchTimeLineHistoryFiles.md)
  - [WalRcvWaitForStartPosition](WalRcvWaitForStartPosition.md)
  - [XLogWalRcvProcessMsg](../X/XLogWalRcvProcessMsg.md), XLogWalRcvSendReply, XLogWalRcvFlush
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md), RecoveryInProgress
  - [WaitLatchOrSocket](WaitLatchOrSocket.md), ResetLatch
  - [on_shmem_exit](../o/on_shmem_exit.md), WalRcvDie

- Called from (representative examples):
  - child_process_kind (process launcher)
  - [walrcv_clear_result](../w/walrcv_clear_result.md) (via function pointer)

## Notes and Other Information
- Central component of PostgreSQL's streaming replication infrastructure
- Manages connection state in shared memory for coordination with other processes
- Implements timeout-based communication with primary server to detect connection issues
- Handles timeline switches and history file management automatically
- Supports both permanent and temporary replication slot management
- Uses latch-based signaling for efficient process communication and interrupt handling
- Coordinates with the startup process for recovery management and restart scenarios

## Simplified Source

```c
// Simplified version of WalReceiverMain
void WalReceiverMain(char *startup_data, size_t startup_data_len) {
    char conninfo[MAXCONNINFO];
    char slotname[NAMEDATALEN];
    bool is_temp_slot;
    XLogRecPtr startpoint;
    TimeLineID startpointTLI, primaryTLI;
    WalRcvData *walrcv;
    bool first_stream = true;

    // Initialize process type and auxiliary process setup
    MyBackendType = B_WAL_RECEIVER;
    AuxiliaryProcessMainCommon();

    // Get shared memory state and mark process as running
    walrcv = WalRcv;
    SpinLockAcquire(&walrcv->mutex);

    // Check initial state - exit if already stopping/stopped
    if (walrcv->walRcvState == WALRCV_STOPPING || walrcv->walRcvState == WALRCV_STOPPED) {
        walrcv->walRcvState = WALRCV_STOPPED;
        SpinLockRelease(&walrcv->mutex);
        ConditionVariableBroadcast(&walrcv->walRcvStoppedCV);
        proc_exit(1);
    }

    // Set up process identification and state
    walrcv->pid = MyProcPid;
    walrcv->walRcvState = WALRCV_STREAMING;

    // Extract connection parameters from shared memory
    strlcpy(conninfo, (char *) walrcv->conninfo, MAXCONNINFO);
    strlcpy(slotname, (char *) walrcv->slotname, NAMEDATALEN);
    is_temp_slot = walrcv->is_temp_slot;
    startpoint = walrcv->receiveStart;
    startpointTLI = walrcv->receiveStartTLI;

    SpinLockRelease(&walrcv->mutex);

    // Set up signal handlers for process management
    setup_signal_handlers();

    // Load libpq functions for WAL streaming
    load_file("libpqwalreceiver", false);

    // Establish connection to primary server
    wrconn = walrcv_connect(conninfo, true, false, false,
                           cluster_name[0] ? cluster_name : "walreceiver", &err);
    if (!wrconn) {
        ereport(ERROR, (errmsg("could not connect to primary server: %s", err)));
    }

    // Save connection info and sender details in shared memory
    update_connection_info(wrconn, walrcv);

    // Main streaming loop - runs until process termination
    for (;;) {
        // Verify system compatibility with primary
        primary_sysid = walrcv_identify_system(wrconn, &primaryTLI);
        validate_system_identifier(primary_sysid);
        validate_timeline_compatibility(primaryTLI, startpointTLI);

        // Fetch any missing timeline history files
        WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI);

        // Create temporary replication slot if needed
        if (is_temp_slot) {
            create_temporary_slot(wrconn, slotname, walrcv);
        }

        // Start WAL streaming from primary
        WalRcvStreamOptions options = setup_streaming_options(startpoint, startpointTLI, slotname);

        if (walrcv_startstreaming(wrconn, &options)) {
            log_streaming_start(startpoint, startpointTLI, first_stream);
            first_stream = false;

            // Initialize streaming state and buffers
            initialize_streaming_state();

            // Inner streaming loop - processes WAL messages
            for (;;) {
                // Check if recovery is still in progress
                if (!RecoveryInProgress()) {
                    ereport(FATAL, (errmsg("recovery has already ended")));
                }

                // Handle any pending interrupts or config reloads
                ProcessWalRcvInterrupts();
                handle_config_reload();

                // Try to receive data from primary
                len = walrcv_receive(wrconn, &buf, &wait_fd);

                if (len > 0) {
                    // Process received WAL data
                    process_wal_messages(buf, len, startpointTLI);
                    XLogWalRcvSendReply(false, false);
                    XLogWalRcvFlush(false, startpointTLI);
                } else if (len < 0) {
                    // End of WAL stream reached
                    endofwal = true;
                    break;
                } else {
                    // No data available, wait for activity or timeout
                    handle_wait_and_timeouts(wait_fd, walrcv);
                }
            }

            // End streaming session
            walrcv_endstreaming(wrconn, &primaryTLI);
            WalRcvFetchTimeLineHistoryFiles(startpointTLI, primaryTLI);
        }

        // Close current WAL file and wait for new instructions
        close_current_wal_file(startpointTLI);
        WalRcvWaitForStartPosition(&startpoint, &startpointTLI);
    }
}
```

Key simplifications made:
- Removed detailed error handling and edge case management for clarity
- Consolidated signal handler setup into single function call
- Abstracted complex state management into helper function calls
- Simplified the nested message processing loop structure
- Combined similar timeout and wait logic into unified handlers
- Focused on the main execution flow rather than low-level implementation details
- Removed platform-specific code paths and detailed memory management