# SubXactEvent

## Location
[src/include/access/xact.h:146-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/xact.h#L146-L151)

## Overview
SubXactEvent is an enumeration that defines the different subtransaction lifecycle events used by PostgreSQL's subtransaction callback system to notify registered callbacks about subtransaction state changes.

## Definition
```c
typedef enum
{
    SUBXACT_EVENT_START_SUB,
    SUBXACT_EVENT_COMMIT_SUB,
    SUBXACT_EVENT_ABORT_SUB,
    SUBXACT_EVENT_PRE_COMMIT_SUB,
} SubXactEvent;
```

## Detailed Description
SubXactEvent is a key component of PostgreSQL's subtransaction callback mechanism, allowing external modules and subsystems to register functions that will be called at specific points during subtransaction lifecycle events. This enumeration provides a type-safe way to specify which subtransaction events should trigger callback execution.

The enum is used in conjunction with SubXactCallback function pointers to create a flexible notification system that allows various PostgreSQL subsystems (including extensions, procedural language handlers, foreign data wrappers, etc.) to perform cleanup, state management, or other operations when subtransactions are started, committed, or aborted.

## Parameters / Member Variables
- `SUBXACT_EVENT_START_SUB`: Triggered when a new subtransaction is started via BeginInternalSubTransaction() or similar mechanisms
- `SUBXACT_EVENT_COMMIT_SUB`: Triggered when a subtransaction is successfully committed via CommitSubTransaction()
- `SUBXACT_EVENT_ABORT_SUB`: Triggered when a subtransaction is aborted/rolled back via AbortSubTransaction()
- `SUBXACT_EVENT_PRE_COMMIT_SUB`: Triggered before a subtransaction commit is finalized, allowing for pre-commit validation and cleanup

## Dependencies
- Functions called/Symbols referenced:
  - SubTransactionId (used in callback signatures)
  - SubXactCallback (callback function type)
- Called from (representative examples):
  - CallSubXactCallbacks
  - StartSubTransaction (uses SUBXACT_EVENT_START_SUB)
  - CommitSubTransaction (uses SUBXACT_EVENT_PRE_COMMIT_SUB and SUBXACT_EVENT_COMMIT_SUB)
  - AbortSubTransaction (uses SUBXACT_EVENT_ABORT_SUB)

## Notes and Other Information
- This enum is defined in src/include/access/xact.h:142-145
- Used extensively by PostgreSQL extensions and procedural languages:
  - PL/pgSQL uses it for exception handling and variable cleanup
  - postgres_fdw uses it for managing remote transaction state
  - sepgsql uses it for security label management
- The callback system ensures that registered callbacks are called in the proper order during subtransaction lifecycle events
- Callbacks can unregister themselves when called, allowing for dynamic cleanup behavior
- The PRE_COMMIT event allows subsystems to perform validation before the commit becomes irreversible
- Each event carries subtransaction ID information to help callbacks identify which subtransaction is being affected