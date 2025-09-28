# StartTransaction

## Location
[src/backend/access/transam/xact.c:2005-2169](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L2005-L2169)

## Overview
Initializes and starts a new database transaction, setting up all necessary transaction state, subsystems, and metadata for transaction processing.

## Definition
```c
static void StartTransaction(void)
```

## Detailed Description
StartTransaction is the core function responsible for initializing a new database transaction in PostgreSQL. It sets up all the necessary state variables, initializes transaction-related subsystems, assigns transaction identifiers, and transitions the transaction state from TRANS_DEFAULT to TRANS_INPROGRESS.

The function handles multiple aspects of transaction initialization:
1. Transaction state management and validation
2. Transaction identifier assignment (virtual and local)
3. User context and security initialization
4. Resource management setup
5. Timestamp management for transaction and statement timing
6. Subsystem initialization (GUC, cache, triggers)
7. Recovery mode handling for read-only transactions

## Parameters / Member Variables
- No parameters (operates on global transaction state)

## Dependencies
- Functions called/Symbols referenced:
  - GetUserIdAndSecContext - retrieves user ID and security context
  - RecoveryInProgress - checks if database is in recovery mode
  - AtStart_Memory, AtStart_ResourceOwner - initialize resource management
  - GetNextLocalTransactionId - assigns new local transaction ID
  - VirtualXactLockTableInsert - locks virtual transaction ID
  - GetCurrentTimestamp - gets current timestamp
  - AtStart_GUC, AtStart_Cache, AfterTriggerBeginXact - initialize subsystems
  - enable_timeout_after - sets up transaction timeout

## Notes and Other Information
- This is an internal static function called by higher-level transaction commands
- Handles both normal and recovery mode transaction initialization
- Sets up transaction sampling for logging based on log_xact_sample_rate
- Manages virtual transaction IDs for process identification
- Essential for all database transaction processing
- Located in src/backend/access/transam/xact.c

## Simplified Source

```c
// Simplified version of StartTransaction
static void StartTransaction(void) {
    TransactionState s;
    VirtualTransactionId vxid;

    // Initialize transaction state
    s = &TopTransactionStateData;
    CurrentTransactionState = s;
    Assert(s->state == TRANS_DEFAULT);
    s->state = TRANS_START;
    s->fullTransactionId = InvalidFullTransactionId;

    // Set up transaction logging sampling
    xact_is_sampled = log_xact_sample_rate != 0 &&
        (log_xact_sample_rate == 1 ||
         pg_prng_double(&pg_global_prng_state) <= log_xact_sample_rate);

    // Initialize transaction state fields
    s->nestingLevel = 1;
    s->gucNestLevel = 1;
    s->childXids = NULL;
    s->nChildXids = 0;
    s->maxChildXids = 0;

    // Get user ID and security context
    GetUserIdAndSecContext(&s->prevUser, &s->prevSecContext);
    Assert(s->prevSecContext == 0);

    // Set transaction properties based on recovery state
    if (RecoveryInProgress()) {
        s->startedInRecovery = true;
        XactReadOnly = true;
    } else {
        s->startedInRecovery = false;
        XactReadOnly = DefaultXactReadOnly;
    }
    XactDeferrable = DefaultXactDeferrable;
    XactIsoLevel = DefaultXactIsoLevel;
    forceSyncCommit = false;
    MyXactFlags = 0;

    // Initialize transaction counters
    s->subTransactionId = TopSubTransactionId;
    currentSubTransactionId = TopSubTransactionId;
    currentCommandId = FirstCommandId;
    currentCommandIdUsed = false;
    nUnreportedXids = 0;
    s->didLogXid = false;

    // Initialize resource management
    AtStart_Memory();
    AtStart_ResourceOwner();

    // Assign virtual transaction ID
    vxid.procNumber = MyProcNumber;
    vxid.localTransactionId = GetNextLocalTransactionId();
    VirtualXactLockTableInsert(vxid);
    MyProc->vxid.lxid = vxid.localTransactionId;

    // Set transaction timestamp
    if (!IsParallelWorker()) {
        if (!SPI_inside_nonatomic_context()) {
            xactStartTimestamp = stmtStartTimestamp;
        } else {
            xactStartTimestamp = GetCurrentTimestamp();
        }
    }
    pgstat_report_xact_timestamp(xactStartTimestamp);
    xactStopTimestamp = 0;

    // Initialize subsystems
    AtStart_GUC();
    AtStart_Cache();
    AfterTriggerBeginXact();

    // Transition to in-progress state
    s->state = TRANS_INPROGRESS;

    // Set up transaction timeout if configured
    if (TransactionTimeout > 0) {
        enable_timeout_after(TRANSACTION_TIMEOUT, TransactionTimeout);
    }
}
```

Key simplifications made:
- Removed detailed comments and assertions for brevity
- Consolidated related initialization steps
- Added high-level step comments
- Maintained all essential functionality for transaction startup
- Preserved the critical state transitions and subsystem initialization