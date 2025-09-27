# ProcessInterrupts

## Location
[src/backend/tcop/postgres.c:3271-3507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L3271-L3507)

## Overview
ProcessInterrupts is the core interrupt handling function in PostgreSQL that processes pending interrupt conditions when it's safe to do so, handling various types of interrupts including process termination, query cancellation, timeouts, and client connection issues.

## Definition

```c
void
ProcessInterrupts(void)
```
## Detailed Description
ProcessInterrupts serves as the out-of-line portion of the CHECK_FOR_INTERRUPTS() macro and is called only when InterruptPending is true. The function implements a comprehensive interrupt handling mechanism that processes various types of pending interrupts in a prioritized order:

1. **Process Die Interrupts**: Handles administrator-commanded terminations with different error messages based on process type (authentication, autovacuum, logical replication, background workers, or regular connections)
2. **Client Connection Checks**: Monitors client connection health and handles lost connections
3. **Query Cancellation**: Processes user-requested query cancellations, lock timeouts, and statement timeouts with appropriate precedence handling
4. **Recovery Conflicts**: Handles recovery-related interrupt conflicts in standby servers
5. **Session Timeouts**: Manages idle-in-transaction, transaction, and idle-session timeouts
6. **Statistics Updates**: Triggers statistics reporting when idle
7. **Signal Barriers and Parallel Processing**: Handles process signal barriers and parallel query messages
8. **Memory Context Logging**: Processes memory context logging requests

The function ensures that InterruptPending is cleared before returning if INTERRUPTS_CAN_BE_PROCESSED() is true, guaranteeing that pre-existing interrupts are serviced.

## Parameters / Member Variables
This function takes no parameters and operates on global interrupt state variables.

## Dependencies
- Functions called/Symbols referenced:
  - [LockErrorCleanup](../L/LockErrorCleanup.md)
  - AmAutoVacuumWorkerProcess
  - [IsLogicalWorker](../I/IsLogicalWorker.md)
  - [IsLogicalLauncher](../I/IsLogicalLauncher.md)
  - AmBackgroundWorkerProcess
  - [pq_check_connection](../p/pq_check_connection.md)
  - [enable_timeout_after](../e/enable_timeout_after.md)
  - [get_timeout_indicator](../g/get_timeout_indicator.md)
  - [get_timeout_finish_time](../g/get_timeout_finish_time.md)
  - [ProcessRecoveryConflictInterrupts](ProcessRecoveryConflictInterrupts.md)
  - [IsTransactionOrTransactionBlock](../I/IsTransactionOrTransactionBlock.md)
  - [pgstat_report_stat](../p/pgstat_report_stat.md)
  - [ProcessProcSignalBarrier](ProcessProcSignalBarrier.md)
  - [HandleParallelMessages](../H/HandleParallelMessages.md)
  - [ProcessLogMemoryContextInterrupt](ProcessLogMemoryContextInterrupt.md)
  - [HandleParallelApplyMessages](../H/HandleParallelApplyMessages.md)
  - [proc_exit](../p/proc_exit.md)
  - ereport (various error levels)
- Called from (representative examples):
  - CHECK_FOR_INTERRUPTS macro (most common usage)
  - [die](../d/die.md) signal handler

## Notes and Other Information
- The function checks InterruptHoldoffCount and CritSectionCount to ensure it's safe to process interrupts before proceeding
- [Query](../Q/Query.md) cancel interrupts are deferred while reading input from clients to maintain FE/BE protocol synchronization
- Different process types (autovacuum, logical replication workers, background workers) receive specialized error messages
- Lock and statement timeouts are handled with precedence logic to report the earlier-occurring timeout
- The function includes injection points for testing timeout scenarios
- Statistics updates are only performed when the backend is truly idle (DoingCommandRead and not in a transaction)

## Simplified Source

```c
// Simplified version of ProcessInterrupts
void ProcessInterrupts(void) {
    // Safety check: Can we process interrupts now?
    if (InterruptHoldoffCount != 0 || CritSectionCount != 0)
        return;

    InterruptPending = false;

    // 1. Handle process termination requests (highest priority)
    if (ProcDiePending) {
        ProcDiePending = false;
        QueryCancelPending = false; // Die trumps cancel
        LockErrorCleanup();

        // Send appropriate termination message based on process type
        if (ClientAuthInProgress) {
            // Handle authentication timeout
            ereport(FATAL, "canceling authentication due to timeout");
        } else if (AmAutoVacuumWorkerProcess()) {
            ereport(FATAL, "terminating autovacuum process");
        } else if (IsLogicalWorker()) {
            ereport(FATAL, "terminating logical replication worker");
        } else if (IsLogicalLauncher()) {
            proc_exit(1); // Special case: restart launcher
        } else {
            ereport(FATAL, "terminating connection due to administrator command");
        }
    }

    // 2. Check client connection health
    if (CheckClientConnectionPending) {
        CheckClientConnectionPending = false;
        if (!DoingCommandRead && client_connection_check_interval > 0) {
            if (!pq_check_connection()) {
                ClientConnectionLost = true;
            } else {
                // Re-arm connection check timeout
                enable_timeout_after(CLIENT_CONNECTION_CHECK_TIMEOUT,
                                   client_connection_check_interval);
            }
        }
    }

    // 3. Handle lost client connections
    if (ClientConnectionLost) {
        QueryCancelPending = false; // Connection loss trumps cancel
        LockErrorCleanup();
        ereport(FATAL, "connection to client lost");
    }

    // 4. Process query cancellation requests
    if (QueryCancelPending) {
        // Defer cancel if reading from client (to maintain protocol sync)
        if (QueryCancelHoldoffCount != 0) {
            InterruptPending = true; // Re-arm for later processing
        } else {
            QueryCancelPending = false;

            // Check for timeout conditions
            bool lock_timeout = get_timeout_indicator(LOCK_TIMEOUT, true);
            bool stmt_timeout = get_timeout_indicator(STATEMENT_TIMEOUT, true);

            // Report earlier timeout if both occurred
            if (lock_timeout && stmt_timeout &&
                get_timeout_finish_time(STATEMENT_TIMEOUT) < get_timeout_finish_time(LOCK_TIMEOUT)) {
                lock_timeout = false; // Report statement timeout instead
            }

            // Handle specific timeout types
            if (lock_timeout) {
                LockErrorCleanup();
                ereport(ERROR, "canceling statement due to lock timeout");
            } else if (stmt_timeout) {
                LockErrorCleanup();
                ereport(ERROR, "canceling statement due to statement timeout");
            } else if (AmAutoVacuumWorkerProcess()) {
                LockErrorCleanup();
                ereport(ERROR, "canceling autovacuum task");
            } else if (!DoingCommandRead) {
                // Regular user-requested cancellation
                LockErrorCleanup();
                ereport(ERROR, "canceling statement due to user request");
            }
        }
    }

    // 5. Handle recovery conflicts (standby servers)
    if (RecoveryConflictPending)
        ProcessRecoveryConflictInterrupts();

    // 6. Process session timeout conditions
    if (IdleInTransactionSessionTimeoutPending) {
        IdleInTransactionSessionTimeoutPending = false;
        if (IdleInTransactionSessionTimeout > 0) {
            ereport(FATAL, "terminating connection due to idle-in-transaction timeout");
        }
    }

    if (TransactionTimeoutPending) {
        TransactionTimeoutPending = false;
        if (TransactionTimeout > 0) {
            ereport(FATAL, "terminating connection due to transaction timeout");
        }
    }

    if (IdleSessionTimeoutPending) {
        IdleSessionTimeoutPending = false;
        if (IdleSessionTimeout > 0) {
            ereport(FATAL, "terminating connection due to idle-session timeout");
        }
    }

    // 7. Handle statistics updates when truly idle
    if (IdleStatsUpdateTimeoutPending &&
        DoingCommandRead && !IsTransactionOrTransactionBlock()) {
        IdleStatsUpdateTimeoutPending = false;
        pgstat_report_stat(true);
    }

    // 8. Process other pending signals and messages
    if (ProcSignalBarrierPending)
        ProcessProcSignalBarrier();

    if (ParallelMessagePending)
        HandleParallelMessages();

    if (LogMemoryContextPending)
        ProcessLogMemoryContextInterrupt();

    if (ParallelApplyMessagePending)
        HandleParallelApplyMessages();
}
```

Key simplifications made:
- Removed detailed error code handling and message formatting for clarity
- Consolidated similar timeout handling patterns
- Abstracted client authentication state management details
- Simplified conditional logic while preserving priority order
- Focused on the main execution flow and decision points
- Removed platform-specific details and injection points
- Used simplified error reporting calls instead of full ereport() syntax