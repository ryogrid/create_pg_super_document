# LogicalRepApplyLoop

## Location
[src/backend/replication/logical/worker.c:3491-3754](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L3491-L3754)

## Overview
LogicalRepApplyLoop is the main event loop that handles receiving, processing, and applying logical replication messages from a publisher in PostgreSQL's logical replication system.

## Definition

```c
static void
LogicalRepApplyLoop(XLogRecPtr last_received)
```
## Detailed Description
This function implements the core message processing loop for a logical replication apply worker. It continuously receives messages from the publisher via the WAL receiver connection, processes different message types ('w' for WAL data, 'k' for keepalive), applies changes to the local database, and sends feedback to the publisher. The function manages memory contexts, handles timeouts, processes configuration reloads, and maintains replication statistics. It operates in an infinite loop until the stream ends, handling both streamed and non-streamed transactions while managing error contexts and ensuring proper cleanup.

## Parameters / Member Variables
- `last_received`: The LSN (Log Sequence Number) of the last successfully received and processed message from the publisher
## Dependencies
- Functions called/Symbols referenced:
  - [GetCurrentTimestamp](../G/GetCurrentTimestamp.md)
  - AllocSetContextCreate
  - [pgstat_report_activity](../p/pgstat_report_activity.md)
  - walrcv_receive
  - [apply_dispatch](../a/apply_dispatch.md)
  - [UpdateWorkerStats](../U/UpdateWorkerStats.md)
  - [send_feedback](../s/send_feedback.md)
  - [maybe_reread_subscription](../m/maybe_reread_subscription.md)
  - [process_syncing_tables](../p/process_syncing_tables.md)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [WaitLatchOrSocket](../W/WaitLatchOrSocket.md)
  - [IsTransactionState](../I/IsTransactionState.md)
  - walrcv_endstreaming
- Called from (representative examples):
  - [start_apply](../s/start_apply.md)

## Notes and Other Information
- This is a static function internal to the worker.c file
- Creates and manages two memory contexts: ApplyMessageContext (reset after each message) and LogicalStreamingContext (for streaming mode)
- Processes two main message types: 'w' (WAL data) and 'k' (keepalive messages)
- Implements timeout handling for wal_receiver_timeout to detect connection issues
- Sends periodic feedback messages to the publisher to acknowledge progress
- Handles configuration reloads via SIGHUP signal processing
- Manages table synchronization when not in active transactions
- Uses error context callbacks for detailed error reporting during message processing
- Exits cleanly when the publisher ends the data stream

## Simplified Source

```c
static void
LogicalRepApplyLoop(XLogRecPtr last_received)
{
    TimestampTz last_recv_timestamp = GetCurrentTimestamp();
    bool ping_sent = false;
    TimeLineID tli;
    ErrorContextCallback errcallback;

    // Initialize memory contexts for message processing
    ApplyMessageContext = AllocSetContextCreate(ApplyContext,
                                               "ApplyMessageContext",
                                               ALLOCSET_DEFAULT_SIZES);
    LogicalStreamingContext = AllocSetContextCreate(ApplyContext,
                                                   "LogicalStreamingContext",
                                                   ALLOCSET_DEFAULT_SIZES);

    // Set up error context for better error reporting
    errcallback.callback = apply_error_callback;
    errcallback.previous = error_context_stack;
    error_context_stack = &errcallback;
    apply_error_context_stack = error_context_stack;

    pgstat_report_activity(STATE_IDLE, NULL);

    // Main message processing loop
    for (;;)
    {
        pgsocket fd = PGINVALID_SOCKET;
        int rc, len;
        char *buf = NULL;
        bool endofstream = false;
        long wait_time;

        CHECK_FOR_INTERRUPTS();
        MemoryContextSwitchTo(ApplyMessageContext);

        // Receive data from publisher
        len = walrcv_receive(LogRepWorkerWalRcvConn, &buf, &fd);

        if (len != 0)
        {
            // Process all available messages without blocking
            for (;;)
            {
                CHECK_FOR_INTERRUPTS();

                if (len == 0)
                    break;
                else if (len < 0)
                {
                    ereport(LOG, (errmsg("data stream from publisher has ended")));
                    endofstream = true;
                    break;
                }
                else
                {
                    StringInfoData s;
                    char c;

                    // Handle configuration reloads
                    if (ConfigReloadPending)
                    {
                        ConfigReloadPending = false;
                        ProcessConfigFile(PGC_SIGHUP);
                    }

                    // Reset timeout tracking
                    last_recv_timestamp = GetCurrentTimestamp();
                    ping_sent = false;

                    MemoryContextSwitchTo(ApplyMessageContext);
                    initReadOnlyStringInfo(&s, buf, len);
                    c = pq_getmsgbyte(&s);

                    if (c == 'w')  // WAL data message
                    {
                        XLogRecPtr start_lsn, end_lsn;
                        TimestampTz send_time;

                        // Parse WAL message
                        start_lsn = pq_getmsgint64(&s);
                        end_lsn = pq_getmsgint64(&s);
                        send_time = pq_getmsgint64(&s);

                        // Update tracking
                        if (last_received < start_lsn)
                            last_received = start_lsn;
                        if (last_received < end_lsn)
                            last_received = end_lsn;

                        UpdateWorkerStats(last_received, send_time, false);

                        // Process the logical replication message
                        apply_dispatch(&s);
                    }
                    else if (c == 'k')  // Keepalive message
                    {
                        XLogRecPtr end_lsn;
                        TimestampTz timestamp;
                        bool reply_requested;

                        // Parse keepalive message
                        end_lsn = pq_getmsgint64(&s);
                        timestamp = pq_getmsgint64(&s);
                        reply_requested = pq_getmsgbyte(&s);

                        if (last_received < end_lsn)
                            last_received = end_lsn;

                        send_feedback(last_received, reply_requested, false);
                        UpdateWorkerStats(last_received, timestamp, true);
                    }

                    MemoryContextReset(ApplyMessageContext);
                }

                // Get next message
                len = walrcv_receive(LogRepWorkerWalRcvConn, &buf, &fd);
            }
        }

        // Send feedback to acknowledge progress
        send_feedback(last_received, false, false);

        // Handle maintenance tasks when not in transaction
        if (!in_remote_transaction && !in_streamed_transaction)
        {
            AcceptInvalidationMessages();
            maybe_reread_subscription();
            process_syncing_tables(last_received);
        }

        MemoryContextReset(ApplyMessageContext);
        MemoryContextSwitchTo(TopMemoryContext);

        if (endofstream)
            break;

        // Wait for more data or timeout
        wait_time = !dlist_is_empty(&lsn_mapping) ? WalWriterDelay : NAPTIME_PER_CYCLE;

        rc = WaitLatchOrSocket(MyLatch,
                              WL_SOCKET_READABLE | WL_LATCH_SET |
                              WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                              fd, wait_time,
                              WAIT_EVENT_LOGICAL_APPLY_MAIN);

        if (rc & WL_LATCH_SET)
        {
            ResetLatch(MyLatch);
            CHECK_FOR_INTERRUPTS();
        }

        if (ConfigReloadPending)
        {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        // Handle timeout - send periodic feedback and check for connection issues
        if (rc & WL_TIMEOUT)
        {
            bool requestReply = false;

            // Check for receiver timeout
            if (wal_receiver_timeout > 0)
            {
                TimestampTz now = GetCurrentTimestamp();
                TimestampTz timeout = TimestampTzPlusMilliseconds(last_recv_timestamp,
                                                                wal_receiver_timeout);

                if (now >= timeout)
                    ereport(ERROR,
                           (errcode(ERRCODE_CONNECTION_FAILURE),
                            errmsg("terminating logical replication worker due to timeout")));

                // Send ping if needed
                if (!ping_sent)
                {
                    timeout = TimestampTzPlusMilliseconds(last_recv_timestamp,
                                                        (wal_receiver_timeout / 2));
                    if (now >= timeout)
                    {
                        requestReply = true;
                        ping_sent = true;
                    }
                }
            }

            send_feedback(last_received, requestReply, requestReply);

            // Report stats outside of transactions
            if (!IsTransactionState())
                pgstat_report_stat(true);
        }
    }

    // Cleanup
    error_context_stack = errcallback.previous;
    apply_error_context_stack = error_context_stack;
    walrcv_endstreaming(LogRepWorkerWalRcvConn, &tli);
}
```