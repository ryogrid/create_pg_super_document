# AtEOXact_Namespace

## Location
[src/backend/catalog/namespace.c:4512-4557](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4512-L4557)

## Overview
Handles end-of-transaction cleanup for namespace state, managing temporary namespace lifecycle and registering cleanup callbacks.

## Definition
```c
void
AtEOXact_Namespace(bool isCommit, bool parallel)
```

## Detailed Description
AtEOXact_Namespace is a critical transaction cleanup function that manages the lifecycle of temporary namespaces at transaction boundaries. The function handles two distinct scenarios:

**Transaction Commit:**
- Registers a before_shmem_exit callback (RemoveTempRelationsCallback) to ensure temporary tables are cleaned up when the backend shuts down
- This callback registration happens only once per session when the first temp namespace transaction commits
- Preserves the temporary namespace state for continued use within the session

**Transaction Abort:**
- Invalidates the temporary namespace by resetting myTempNamespace and myTempToastNamespace to InvalidOid
- Clears the tempNamespaceId in the process descriptor (MyProc) to indicate the namespace is no longer in use
- Invalidates search path caches since the namespace state has changed
- Forces complete re-initialization if temp tables are needed in a subsequent transaction

The function only operates when a temporary namespace was created in the current subtransaction (tracked via myTempNamespaceSubID) and is not running in parallel mode.

## Parameters / Member Variables
- `isCommit`: True if the transaction is committing, false if aborting
- `parallel`: True if this is a parallel worker process (temp operations are skipped)

## Dependencies
- Functions called/Symbols referenced:
  - InvalidSubTransactionId (constant)
  - [RemoveTempRelationsCallback](../R/RemoveTempRelationsCallback.md)
  - [before_shmem_exit](../b/before_shmem_exit.md)
- Called from (representative examples):
  - [CommitTransaction](../C/CommitTransaction.md)
  - [PrepareTransaction](../P/PrepareTransaction.md)
  - [AbortTransaction](AbortTransaction.md)
  - RangeVarGetRelid (via header include)

## Notes and Other Information
- This is a public function accessible throughout the codebase via namespace.h
- Part of PostgreSQL's standard transaction cleanup mechanism (AtEOXact_* functions)
- The parallel parameter check prevents interference in parallel worker processes
- Critical for preventing temporary table leaks across transaction boundaries
- The atomic nature of MyProc->tempNamespaceId assignment ensures consistency in concurrent scenarios
- Only registers the cleanup callback once per session to avoid duplicate registrations
- Ensures proper cleanup even when transactions abort before temp table operations complete