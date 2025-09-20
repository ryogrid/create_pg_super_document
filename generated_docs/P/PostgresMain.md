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
- : Name of the database to connect to for this backend session
- : PostgreSQL username to be used for authentication and session context

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