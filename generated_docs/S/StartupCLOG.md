# StartupCLOG

## Location
[src/backend/access/transam/clog.c:877-891](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/clog.c#L877-L891)

## Overview
Initializes the CLOG subsystem's latest page number tracking during PostgreSQL server startup, based on the next transaction ID.

## Definition

```c
void
StartupCLOG(void)
```
## Detailed Description
StartupCLOG is a critical startup function that must be called exactly once during postmaster or standalone-backend startup, specifically after StartupXLOG has initialized the transaction system's nextXid variable. The function performs essential initialization of the CLOG subsystem's page tracking mechanism.

The function performs the following key operations:

1. **Transaction ID retrieval**: Extracts the current transaction ID from TransamVariables->nextXid using XidFromFullTransactionId()
2. **Page number calculation**: Determines which CLOG page corresponds to this transaction ID using TransactionIdToPage()
3. **Latest page tracking**: Atomically sets the latest_page_number in the shared CLOG control structure to ensure proper page management

This initialization is crucial for CLOG operation as it establishes the baseline for determining which CLOG pages are currently relevant and may need to be extended as new transactions are assigned. The atomic write ensures that this initialization is visible to all processes in a thread-safe manner.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - XidFromFullTransactionId (extracts XID from FullTransactionId)
  - [TransactionIdToPage](../T/TransactionIdToPage.md) (maps transaction ID to CLOG page number)
  - [pg_atomic_write_u64](../p/pg_atomic_write_u64.md) (atomic write operation)
- Global variables:
  - TransamVariables->nextXid (next transaction ID to be assigned)
  - XactCtl (CLOG SLRU control structure)
- Called from:
  - [StartupXLOG](StartupXLOG.md) (during WAL recovery and startup)

## Notes and Other Information
- This function must be called exactly once during server startup
- It depends on StartupXLOG having already initialized TransamVariables->nextXid
- The function establishes the baseline for CLOG page management during normal operation
- The atomic write ensures thread-safe initialization in multi-process environments
- This initialization is essential before normal transaction processing can begin
- The latest_page_number tracking helps optimize CLOG page extension and garbage collection
- Proper sequencing with other startup functions is critical for correct CLOG operation
- The function works with both postmaster and standalone-backend startup scenarios

## Simplified Source

```c
// Simplified version of StartupCLOG
void StartupCLOG(void) {
    // Get the next transaction ID that will be assigned
    TransactionId xid = XidFromFullTransactionId(TransamVariables->nextXid);

    // Calculate which CLOG page this transaction ID belongs to
    int64 pageno = TransactionIdToPage(xid);

    // Initialize the latest page number tracker for CLOG
    pg_atomic_write_u64(&XactCtl->shared->latest_page_number, pageno);
}
```

Key simplifications made:
- Added explanatory comments for each operation
- Maintained the essential logic flow
- Kept the atomic write operation as it's critical for thread safety
- Preserved all function calls as they're necessary for functionality