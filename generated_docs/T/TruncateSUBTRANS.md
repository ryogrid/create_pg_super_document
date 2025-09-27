# TruncateSUBTRANS

## Location
[src/backend/access/transam/subtrans.c:411-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/subtrans.c#L411-L434)

## Overview
Removes all SUBTRANS segments before the one containing the oldest active transaction, helping to reclaim disk space and prevent infinite growth of subtransaction status files.

## Definition

```c
void
TruncateSUBTRANS(TransactionId oldestXact)
```
## Detailed Description
TruncateSUBTRANS performs garbage collection for the SUBTRANS system by removing old, no longer needed subtransaction status pages. It's called only during checkpoint operations to ensure that pages are only removed when it's safe to do so.

The function calculates a cutoff page based on the oldest active transaction ID. It steps back one transaction before determining the cutoff page to avoid edge cases where the oldest transaction would be the first item on a page and equals the next XID. This prevents triggering SimpleLruTruncate's wraparound detection logic inappropriately.

All SUBTRANS pages before the cutoff page are removed using SimpleLruTruncate, freeing up disk space and shared memory resources that are no longer needed.

## Parameters / Member Variables
- : The oldest TransactionXmin of any running transaction, representing the boundary before which subtransaction status is no longer needed

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdRetreat
  - [TransactionIdToPage](TransactionIdToPage.md)  
  - [SimpleLruTruncate](../S/SimpleLruTruncate.md)
  - SubTransCtl
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (during regular checkpoints)
  - [CreateRestartPoint](../C/CreateRestartPoint.md) (during recovery restart points)

## Notes and Other Information
- Only called during checkpoint operations for safety
- Steps back one transaction to avoid wraparound detection edge cases
- Critical for preventing unbounded growth of SUBTRANS files
- Works in coordination with transaction visibility and cleanup mechanisms
- Uses SimpleLruTruncate for efficient bulk removal of old pages
- Helps maintain system performance by keeping SUBTRANS size manageable

## Simplified Source

```c
// Simplified version of TruncateSUBTRANS
void TruncateSUBTRANS(TransactionId oldestXact) {
    int64 cutoffPage;

    // Step back one transaction to avoid edge cases with wraparound detection
    TransactionIdRetreat(oldestXact);

    // Calculate which page contains the adjusted oldest transaction
    cutoffPage = TransactionIdToPage(oldestXact);

    // Remove all SUBTRANS pages before the cutoff page
    SimpleLruTruncate(SubTransCtl, cutoffPage);
}
```

Key simplifications made:
- Condensed the detailed comment into a brief explanation of the retreat step
- Removed verbose comments while preserving the essential algorithm
- Focused on the main execution path: retreat, calculate page, truncate
- Maintained the core logic flow and all function calls