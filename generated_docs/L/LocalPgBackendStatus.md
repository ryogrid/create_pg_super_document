# LocalPgBackendStatus

## Location
src/include/utils/backend_status.h: 245 - 279

## Overview
LocalPgBackendStatus is an extended version of PgBackendStatus that adds transaction-specific information, used when building backend status arrays to provide additional context without modifying shared memory structures.

## Definition
```c
typedef struct LocalPgBackendStatus
{
    /*
     * Local version of the backend status entry.
     */
    PgBackendStatus backendStatus;

    /*
     * The proc number.
     */
    ProcNumber      proc_number;

    /*
     * The xid of the current transaction if available, InvalidTransactionId
     * if not.
     */
    TransactionId   backend_xid;

    /*
     * The xmin of the current session if available, InvalidTransactionId if
     * not.
     */
    TransactionId   backend_xmin;

    /*
     * Number of cached subtransactions in the current session.
     */
    int             backend_subxact_count;

    /*
     * The number of subtransactions in the current session which exceeded the
     * cached subtransaction limit.
     */
    bool            backend_subxact_overflowed;
} LocalPgBackendStatus;
```

## Detailed Description
LocalPgBackendStatus extends PgBackendStatus with additional transaction-specific information that is not stored in shared memory but is useful for monitoring and administrative queries. This design allows PostgreSQL to provide richer backend information without increasing the shared memory footprint of the basic backend status structures.

The structure contains PgBackendStatus as its first member, making it compatible with code that expects the basic backend status while providing additional transaction details. This extension pattern allows for adding new monitoring fields without requiring changes to shared memory structures, which would be disruptive to running systems.

The additional fields focus on transaction state, including the current transaction ID, session transaction minimum (xmin), and subtransaction tracking. This information is particularly valuable for understanding transaction bloat, long-running transactions, and subtransaction overhead.

## Parameters / Member Variables
- `backendStatus`: Complete PgBackendStatus structure containing all shared-memory backend information
- `proc_number`: The process number (ProcNumber) identifying this backend in the process array
- `backend_xid`: Transaction ID of the current transaction, or InvalidTransactionId if no active transaction
- `backend_xmin`: The xmin (transaction visibility horizon) for this session, or InvalidTransactionId if not available
- `backend_subxact_count`: Number of subtransactions currently cached for this session
- `backend_subxact_overflowed`: Boolean flag indicating whether the subtransaction cache has overflowed for this session

## Dependencies
- Types referenced:
  - [PgBackendStatus](../P/PgBackendStatus.md) (base backend status structure)
  - ProcNumber (process identification type)
  - TransactionId (transaction identifier type)
- Used by:
  - NumBackendStatSlots (for memory calculations)
  - pgstat_read_current_status (for building status arrays)
  - [cmp_lbestatus](../c/cmp_lbestatus.md) (for sorting backend entries)
  - [pgstat_get_beentry_by_proc_number](../p/pgstat_get_beentry_by_proc_number.md) (for proc-specific lookups)
  - [pgstat_get_local_beentry_by_proc_number](../p/pgstat_get_local_beentry_by_proc_number.md) (for local entry access)
  - Various pg_stat system functions for transaction and subtransaction monitoring

## Notes and Other Information
- The structure's design allows extending backend information without changing shared memory layout
- Transaction IDs use InvalidTransactionId constant to indicate no active transaction or unavailable information
- Subtransaction overflow tracking is important for performance monitoring as overflow can cause significant performance degradation
- The proc_number field links this structure to the process array and PGPROC structures
- Used primarily in system views that provide transaction-level details about backend processes
- The first member being PgBackendStatus enables type-compatible operations with the base structure
- Essential for monitoring tools that need both connection-level (from PgBackendStatus) and transaction-level information
- Supports queries about transaction age, subtransaction usage, and session transaction state without requiring additional shared memory overhead