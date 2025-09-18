# SignalBackends

## Location
[src/backend/commands/async.c:1581-1670](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/async.c#L1581-L1670)

## Overview
Internal function that sends signals to listening backend processes to notify them of new notifications in the queue, with intelligent prioritization based on database affinity and queue lag.

## Definition


## Detailed Description
This function is responsible for notifying listening backend processes that new notifications are available in the queue. It implements a two-tier signaling strategy: backends in the same database are signaled immediately (unless they're already caught up), while backends in other databases are only signaled if they've fallen significantly behind (QUEUE_CLEANUP_DELAY pages). This approach balances notification delivery with system efficiency by preventing unnecessary cross-database signals while ensuring that lagging listeners don't indefinitely block queue cleanup. The function separates the identification phase (while holding locks) from the actual signaling phase to minimize lock contention.

## Parameters / Member Variables
- No parameters

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (allocates memory for PID and ProcNumber arrays)
  - LWLockAcquire/LWLockRelease (manages NotifyQueueLock in exclusive mode)
  - QUEUE_FIRST_LISTENER, QUEUE_NEXT_LISTENER (iterates through listening backends)
  - QUEUE_BACKEND_PID, QUEUE_BACKEND_POS, QUEUE_BACKEND_DBOID (accesses backend information)
  - QUEUE_POS_EQUAL, QUEUE_POS_PAGE (queue position comparisons)
  - [asyncQueuePageDiff](../a/asyncQueuePageDiff.md) (calculates page distance for lag detection)
  - [SendProcSignal](SendProcSignal.md) (sends PROCSIG_NOTIFY_INTERRUPT to target backends)
  - notifyInterruptPending (flag set for self-signaling optimization)
  - [pfree](../p/pfree.md) (deallocates temporary arrays)
- Called from:
  - [AtCommit_Notify](../A/AtCommit_Notify.md) (signals backends when committing notifications)
  - NotificationHash (context reference in async.c)

## Notes and Other Information
- Called during CommitTransaction(), designed for very low failure probability
- Uses temporary arrays to avoid holding locks during signal sending
- Implements self-signaling optimization (sets flag instead of sending signal to own process)
- Prioritizes same-database listeners over cross-database listeners
- Cross-database signaling only occurs when listeners are QUEUE_CLEANUP_DELAY pages behind
- Includes error handling for failed signals (logs at DEBUG3 level)
- Memory allocation could theoretically fail - comments suggest pre-allocation might be considered
- Internal static function, not exposed outside async.c module
- Critical component of PostgreSQL's LISTEN/NOTIFY mechanism