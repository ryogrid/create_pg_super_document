# PostgresMain

## Location
[src/backend/tcop/postgres.c:4239-5025](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L4239-L5025)

## Overview
The central main loop function for all PostgreSQL backend processes that handles client communication, command processing, and transaction management for both interactive and WAL sender backends.

## Definition

```c
struct.  The reason is that this is the bottom of the
	 * exception stack, and so with PG_TRY there would be no exception handler
	 * in force at all during the CATCH part.  By leaving the outermost setjmp
	 * always active, we have at least some chance of recovering from an error
	 * during error recovery.  (If we get into an infinite loop thereby, it
	 * will soon be stopped by overflow of elog.c's internal state stack.)
	 *
	 * Note that we use sigsetjmp(..., 1), so that this function's signal mask
	 * (to wit, UnBlockSig) will be restored when longjmp'ing to here.  This
	 * is essential in case we longjmp'd out of a signal handler on a platform
	 * where that leaves the signal blocked.  It's not redundant with the
	 * unblock in AbortTransaction() because the latter is only called if we
	 * were inside a transaction.
	 */

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/*
		 * NOTE: if you are tempted to add more code in this if-block,
		 * consider the high probability that it should be in
		 * AbortTransaction() instead.  The only stuff done directly here
		 * should be stuff that is guaranteed to apply *only* for outer-level
		 * error recovery, such as adjusting the FE/BE protocol status.
		 */

		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/*
		 * Forget any pending QueryCancel request, since we're returning to
		 * the idle loop anyway, and cancel any active timeout requests.  (In
		 * future we might want to allow some timeout requests to survive, but
		 * at minimum it'd be necessary to do reschedule_timeouts(), in case
		 * we got here because of a query cancel interrupting the SIGALRM
		 * interrupt handler.)	Note in particular that we must clear the
		 * statement and lock timeout indicators, to prevent any future plain
		 * query cancels from being misreported as timeouts in case we're
		 * forgetting a timeout cancel.
		 */
		disable_all_timeouts(false);	/* do first to avoid race condition */
		QueryCancelPending = false;
		idle_in_transaction_timeout_enabled = false;
		idle_session_timeout_enabled = false;

		/* Not reading from the client anymore. */
		DoingCommandRead = false;

		/* Make sure libpq is in a good state */
		pq_comm_reset();

		/* Report the error to the client and/or server log */
		EmitErrorReport();

		/*
		 * If Valgrind noticed something during the erroneous query, print the
		 * query string, assuming we have one.
		 */
		valgrind_report_error_query(debug_query_string);

		/*
		 * Make sure debug_query_string gets reset before we possibly clobber
		 * the storage it points at.
		 */
		debug_query_string = NULL;

		/*
		 * Abort the current transaction in order to recover.
		 */
		AbortCurrentTransaction();

		if (am_walsender)
			WalSndErrorCleanup();

		PortalErrorCleanup();

		/*
		 * We can't release replication slots inside AbortTransaction() as we
		 * need to be able to start and abort transactions while having a slot
		 * acquired. But we never need to hold them across top level errors,
		 * so releasing here is fine. There also is a before_shmem_exit()
		 * callback ensuring correct cleanup on FATAL errors.
		 */
		if (MyReplicationSlot != NULL)
			ReplicationSlotRelease();

		/* We also want to cleanup temporary slots on error. */
		ReplicationSlotCleanup(false);

		jit_reset_after_error();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(TopMemoryContext);
		FlushErrorState();

		/*
		 * If we were handling an extended-query-protocol message, initiate
		 * skip till next Sync.  This also causes us not to issue
		 * ReadyForQuery (until we get Sync).
		 */
		if (doing_extended_query_message)
			ignore_till_sync = true;

		/* We don't have a transaction command open anymore */
		xact_started = false;

		/*
		 * If an error occurred while we were reading a message from the
		 * client, we have potentially lost track of where the previous
		 * message ends and the next one begins.  Even though we have
		 * otherwise recovered from the error, we cannot safely read any more
		 * messages from the client, so there isn't much we can do with the
		 * connection anymore.
		 */
		if (pq_is_reading_msg())
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("terminating connection because protocol synchronization was lost")));

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;
```

## Simplified Source

```c
// Simplified version of PostgresMain
void PostgresMain(const char *dbname, const char *username) {
    sigjmp_buf local_sigjmp_buf;
    volatile bool send_ready_for_query = true;
    volatile bool idle_in_transaction_timeout_enabled = false;
    volatile bool idle_session_timeout_enabled = false;

    // Core initialization: Set up signal handlers
    SetProcessingMode(InitProcessing);
    if (am_walsender) {
        WalSndSignals();
    } else {
        // Set up standard signal handlers for regular backends
        pqsignal(SIGHUP, SignalHandlerForConfigReload);
        pqsignal(SIGINT, StatementCancelHandler);
        pqsignal(SIGTERM, die);
        pqsignal(SIGQUIT, IsUnderPostmaster ? quickdie : die);
        InitializeTimeouts();
        pqsignal(SIGPIPE, SIG_IGN);
        pqsignal(SIGUSR1, procsignal_sigusr1_handler);
        // ... other signal handlers
    }

    // Core initialization: Basic setup and database connection
    BaseInit();
    sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);
    InitPostgres(dbname, InvalidOid, username, InvalidOid,
                 (!am_walsender) ? INIT_PG_LOAD_SESSION_LIBS : 0, NULL);

    // Clean up postmaster context and finalize initialization
    if (PostmasterContext) {
        MemoryContextDelete(PostmasterContext);
        PostmasterContext = NULL;
    }
    SetProcessingMode(NormalProcessing);
    BeginReportingGUCOptions();

    // Set up memory contexts for message processing
    MessageContext = AllocSetContextCreate(TopMemoryContext, "MessageContext",
                                         ALLOCSET_DEFAULT_SIZES);
    row_description_context = AllocSetContextCreate(TopMemoryContext,
                                                  "RowDescriptionContext",
                                                  ALLOCSET_DEFAULT_SIZES);

    // Send backend key data to client for cancellation support
    if (whereToSendOutput == DestRemote) {
        StringInfoData buf;
        pq_beginmessage(&buf, PqMsg_BackendKeyData);
        pq_sendint32(&buf, (int32) MyProcPid);
        pq_sendint32(&buf, (int32) MyCancelKey);
        pq_endmessage(&buf);
    }

    // Fire login event triggers
    EventTriggerOnLogin();

    // Main exception handling setup - if error occurs, jump here for recovery
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        // Error recovery: Clean up state and abort current transaction
        error_context_stack = NULL;
        HOLD_INTERRUPTS();

        // Reset timeouts and cancel pending operations
        disable_all_timeouts(false);
        QueryCancelPending = false;
        idle_in_transaction_timeout_enabled = false;
        idle_session_timeout_enabled = false;
        DoingCommandRead = false;

        // Clean up communication and report error
        pq_comm_reset();
        EmitErrorReport();
        debug_query_string = NULL;

        // Abort current transaction and clean up resources
        AbortCurrentTransaction();
        if (am_walsender) WalSndErrorCleanup();
        PortalErrorCleanup();
        if (MyReplicationSlot != NULL) ReplicationSlotRelease();
        ReplicationSlotCleanup(false);
        jit_reset_after_error();

        // Return to normal context and reset error state
        MemoryContextSwitchTo(TopMemoryContext);
        FlushErrorState();

        // Handle protocol state recovery
        if (doing_extended_query_message) ignore_till_sync = true;
        xact_started = false;

        // Check for unrecoverable protocol errors
        if (pq_is_reading_msg()) {
            ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                          errmsg("terminating connection because protocol synchronization was lost")));
        }

        RESUME_INTERRUPTS();
    }

    // Enable error handling for main loop
    PG_exception_stack = &local_sigjmp_buf;
    if (!ignore_till_sync) send_ready_for_query = true;

    // Main command processing loop
    for (;;) {
        int firstchar;
        StringInfoData input_message;

        doing_extended_query_message = false;

        // Prepare for next command: reset memory context and create input buffer
        MemoryContextSwitchTo(MessageContext);
        MemoryContextReset(MessageContext);
        initStringInfo(&input_message);
        InvalidateCatalogSnapshotConditionally();

        // Send ReadyForQuery if needed and handle idle state
        if (send_ready_for_query) {
            if (IsAbortedTransactionBlockState()) {
                set_ps_display("idle in transaction (aborted)");
                pgstat_report_activity(STATE_IDLEINTRANSACTION_ABORTED, NULL);
                // Enable idle-in-transaction timeout if configured
                if (IdleInTransactionSessionTimeout > 0) {
                    idle_in_transaction_timeout_enabled = true;
                    enable_timeout_after(IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
                                       IdleInTransactionSessionTimeout);
                }
            } else if (IsTransactionOrTransactionBlock()) {
                set_ps_display("idle in transaction");
                pgstat_report_activity(STATE_IDLEINTRANSACTION, NULL);
                // Enable idle-in-transaction timeout if configured
                if (IdleInTransactionSessionTimeout > 0) {
                    idle_in_transaction_timeout_enabled = true;
                    enable_timeout_after(IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
                                       IdleInTransactionSessionTimeout);
                }
            } else {
                // Handle notifications and statistics reporting
                if (notifyInterruptPending) ProcessNotifyInterrupt(false);

                // Report statistics if needed
                long stats_timeout = pgstat_report_stat(false);
                if (stats_timeout > 0) {
                    if (!get_timeout_active(IDLE_STATS_UPDATE_TIMEOUT)) {
                        enable_timeout_after(IDLE_STATS_UPDATE_TIMEOUT, stats_timeout);
                    }
                } else {
                    if (get_timeout_active(IDLE_STATS_UPDATE_TIMEOUT)) {
                        disable_timeout(IDLE_STATS_UPDATE_TIMEOUT, false);
                    }
                }

                set_ps_display("idle");
                pgstat_report_activity(STATE_IDLE, NULL);

                // Enable idle session timeout if configured
                if (IdleSessionTimeout > 0) {
                    idle_session_timeout_enabled = true;
                    enable_timeout_after(IDLE_SESSION_TIMEOUT, IdleSessionTimeout);
                }
            }

            ReportChangedGUCOptions();
            ReadyForQuery(whereToSendOutput);
            send_ready_for_query = false;
        }

        // Read command from client
        DoingCommandRead = true;
        firstchar = ReadCommand(&input_message);

        // Disable timeouts after receiving command
        if (idle_in_transaction_timeout_enabled) {
            disable_timeout(IDLE_IN_TRANSACTION_SESSION_TIMEOUT, false);
            idle_in_transaction_timeout_enabled = false;
        }
        if (idle_session_timeout_enabled) {
            disable_timeout(IDLE_SESSION_TIMEOUT, false);
            idle_session_timeout_enabled = false;
        }

        CHECK_FOR_INTERRUPTS();
        DoingCommandRead = false;

        // Handle configuration reload if pending
        if (ConfigReloadPending) {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        // Skip processing if ignoring until Sync message
        if (ignore_till_sync && firstchar != EOF) continue;

        // Process the command based on message type
        switch (firstchar) {
            case PqMsg_Query: {
                // Simple query protocol
                const char *query_string;
                SetCurrentStatementStartTimestamp();
                query_string = pq_getmsgstring(&input_message);
                pq_getmsgend(&input_message);

                if (am_walsender) {
                    if (!exec_replication_command(query_string)) {
                        exec_simple_query(query_string);
                    }
                } else {
                    exec_simple_query(query_string);
                }

                send_ready_for_query = true;
                break;
            }

            case PqMsg_Parse: {
                // Extended query protocol: Parse
                forbidden_in_wal_sender(firstchar);
                SetCurrentStatementStartTimestamp();
                // Extract statement name, query string, and parameters
                // Execute parse operation
                break;
            }

            case PqMsg_Bind:
                // Extended query protocol: Bind parameters
                forbidden_in_wal_sender(firstchar);
                SetCurrentStatementStartTimestamp();
                exec_bind_message(&input_message);
                break;

            case PqMsg_Execute: {
                // Extended query protocol: Execute
                forbidden_in_wal_sender(firstchar);
                SetCurrentStatementStartTimestamp();
                const char *portal_name = pq_getmsgstring(&input_message);
                int max_rows = pq_getmsgint(&input_message, 4);
                pq_getmsgend(&input_message);
                exec_execute_message(portal_name, max_rows);
                break;
            }

            case PqMsg_FunctionCall:
                // Fast-path function call
                forbidden_in_wal_sender(firstchar);
                SetCurrentStatementStartTimestamp();
                pgstat_report_activity(STATE_FASTPATH, NULL);
                set_ps_display("<FASTPATH>");
                start_xact_command();
                MemoryContextSwitchTo(MessageContext);
                HandleFunctionRequest(&input_message);
                finish_xact_command();
                send_ready_for_query = true;
                break;

            case PqMsg_Close:
                // Close prepared statement or portal
                forbidden_in_wal_sender(firstchar);
                // Handle close request
                break;

            case PqMsg_Describe:
                // Describe prepared statement or portal
                forbidden_in_wal_sender(firstchar);
                SetCurrentStatementStartTimestamp();
                // Handle describe request
                break;

            case PqMsg_Flush:
                // Flush output buffer
                pq_getmsgend(&input_message);
                if (whereToSendOutput == DestRemote) pq_flush();
                break;

            case PqMsg_Sync:
                // Synchronize extended query protocol
                pq_getmsgend(&input_message);
                finish_xact_command();
                send_ready_for_query = true;
                break;

            case EOF:
                // Client disconnected
                pgStatSessionEndCause = DISCONNECT_CLIENT_EOF;
                // Fall through to terminate

            case PqMsg_Terminate:
                // Client requested termination
                if (whereToSendOutput == DestRemote) {
                    whereToSendOutput = DestNone;
                }
                proc_exit(0);

            case PqMsg_CopyData:
            case PqMsg_CopyDone:
            case PqMsg_CopyFail:
                // Ignore copy messages during error recovery
                break;

            default:
                // Invalid message type
                ereport(FATAL, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                              errmsg("invalid frontend message type %d", firstchar)));
        }
    }
}
```

Key simplifications made:
- Removed detailed error handling for clarity while preserving essential error recovery logic
- Consolidated similar timeout handling branches
- Abstracted low-level protocol message parsing details
- Focused on the main execution flow and core functionality
- Simplified signal handler setup while maintaining the essential handlers
- Streamlined memory context management
- Condensed extended query protocol cases while showing the essential structure
- Preserved the critical setjmp/longjmp error recovery mechanism
- Maintained the infinite command processing loop structure

## Detailed Description
PostgresMain is the heart of PostgreSQL's backend processing system. It serves as the main loop for all backend processes, whether they are regular client-serving backends or WAL sender processes. The function is responsible for the complete lifecycle of backend operations including initialization, signal handling, command processing, error recovery, and cleanup.

The function operates in several key phases:
1. **Signal Setup**: Configures signal handlers for various system and PostgreSQL-specific signals, with different handling for WAL senders vs. regular backends
2. **Initialization**: Performs base initialization and database connection setup through InitPostgres()
3. **Main Processing Loop**: An infinite loop that handles client commands using the PostgreSQL frontend/backend protocol
4. **Error Recovery**: Uses setjmp/longjmp for exception handling and transaction abort/recovery
5. **Protocol Handling**: Processes various message types (Query, Parse, Bind, Execute, etc.) according to the PostgreSQL wire protocol

The function includes sophisticated timeout management for idle sessions and idle-in-transaction states, comprehensive error handling and reporting, and support for both simple and extended query protocols. It also handles special cases like WAL sender operations and replication commands.

## Parameters / Member Variables
- `database_name`: Name of the database to connect to for this backend session
- `username`: PostgreSQL username to be used for authentication and session context

## Dependencies
- Functions called/Symbols referenced:
  - SetProcessingMode (set backend processing mode)
  - [WalSndSignals](../W/WalSndSignals.md)/pqsignal (signal handler setup)
  - [BaseInit](../B/BaseInit.md) (basic backend initialization)
  - [InitPostgres](../I/InitPostgres.md) (database connection and initialization)
  - [ReadCommand](../R/ReadCommand.md) (read client commands from network)
  - [exec_simple_query](../e/exec_simple_query.md)/exec_parse_message/exec_bind_message/exec_execute_message (command execution)
  - [forbidden_in_wal_sender](../f/forbidden_in_wal_sender.md) (check if command is allowed in WAL sender)
  - [AbortCurrentTransaction](../A/AbortCurrentTransaction.md) (transaction abort and cleanup)
  - [pgstat_report_activity](../p/pgstat_report_activity.md) (activity reporting for monitoring)
  - [EventTriggerOnLogin](../E/EventTriggerOnLogin.md) (login event trigger processing)
- Called from (representative examples):
  - [BackendMain](../B/BackendMain.md) (in src/backend/tcop/backend_startup.c:105)
  - [PostgresSingleUserMain](PostgresSingleUserMain.md) (in src/backend/tcop/postgres.c:4223)

## Notes and Other Information
- Uses setjmp/longjmp for error recovery rather than PG_TRY/PG_CATCH to maintain exception handling during error recovery itself
- Handles both simple query protocol (PqMsg_Query) and extended query protocol (Parse/Bind/Execute/Describe)
- Includes special handling for WAL sender processes with different signal handlers and command restrictions
- Implements sophisticated timeout management including idle-session and idle-in-transaction timeouts
- Memory management through MessageContext which is reset after each command cycle
- Supports Valgrind integration for memory debugging
- Critical for PostgreSQL's multi-process architecture - each client connection runs this function in its own backend process
- The function never returns under normal circumstances - it either processes commands indefinitely or exits via proc_exit()
- Includes comprehensive protocol violation detection and error reporting