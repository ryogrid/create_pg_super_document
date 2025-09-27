# TransactionIdToPage

## Location
[src/backend/access/transam/subtrans.c:61-65](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L61-L65)

## Overview
Converts a transaction ID to its corresponding page number in the commit log (CLOG), enabling PostgreSQL to efficiently map transactions to their storage locations.

## Definition

```c
structures for SUBTRANS control
 */
static SlruCtlData SubTransCtlData;
```
## Detailed Description
TransactionIdToPage is a utility function that calculates which page in the commit log (CLOG) contains the status information for a given transaction ID. The function performs integer division of the transaction ID by CLOG_XACTS_PER_PAGE to determine the page number. This mapping is fundamental to PostgreSQL's transaction status storage system, where multiple transaction statuses are packed into pages for efficient storage and access.

The function returns an int64 value, though the actual maximum value is currently limited to 0xFFFFFFFF/CLOG_XACTS_PER_PAGE due to the transaction ID space constraints.

## Parameters / Member Variables
- : The transaction ID (TransactionId type) for which to determine the corresponding CLOG page number

## Dependencies
- Functions called/Symbols referenced:
  - CLOG_XACTS_PER_PAGE (constant defining how many transaction statuses fit per page)
- Called from (representative examples):
  - [TransactionIdSetTreeStatus](TransactionIdSetTreeStatus.md)
  - [set_status_by_pages](../s/set_status_by_pages.md)
  - [TransactionIdSetPageStatusInternal](TransactionIdSetPageStatusInternal.md)
  - [TransactionIdSetStatusBit](TransactionIdSetStatusBit.md)
  - [TransactionIdGetStatus](TransactionIdGetStatus.md)
  - [StartupCLOG](../S/StartupCLOG.md)
  - [TrimCLOG](TrimCLOG.md)
  - [ExtendCLOG](../E/ExtendCLOG.md)
  - [TruncateCLOG](TruncateCLOG.md)
  - [SubTransSetParent](../S/SubTransSetParent.md)
  - [SubTransGetParent](../S/SubTransGetParent.md)
  - [StartupSUBTRANS](../S/StartupSUBTRANS.md)
  - [ExtendSUBTRANS](../E/ExtendSUBTRANS.md)
  - [TruncateSUBTRANS](TruncateSUBTRANS.md)

## Notes and Other Information
- This is a static inline function, optimized for performance due to its frequent usage
- Used extensively throughout both CLOG and SUBTRANS subsystems for page-based transaction status management
- The function is crucial for PostgreSQL's MVCC (Multi-Version Concurrency Control) implementation
- The division operation effectively groups consecutive transaction IDs into the same page, enabling batch operations on related transactions

## Simplified Source

```c
// Simplified version of TransactionIdToPage
static inline int64 TransactionIdToPage(TransactionId xid) {
    // Calculate which page contains this transaction ID
    return xid / (int64) CLOG_XACTS_PER_PAGE;
}
```

Key simplifications made:
- Focused on the core calculation: integer division to map transaction ID to page
- Preserved the essential int64 cast for proper arithmetic
- Emphasized the grouping concept: consecutive transactions → same page
- Maintained inline optimization for frequent usage in MVCC operations