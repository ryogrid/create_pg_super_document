# heap_prune_record_prunable

## Location
[src/backend/access/heap/pruneheap.c:1201-1214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/pruneheap.c#L1201-L1214)

## Overview
Records the lowest soon-prunable transaction ID (XID) in the pruning state, which is used to track when a page may need pruning again.

## Definition

```c
static void
heap_prune_record_prunable(PruneState *prstate, TransactionId xid)
```
## Detailed Description
This function updates the pruning state to record the lowest soon-prunable transaction ID. It maintains the minimum XID among all transactions that may soon become prunable on the page. This information is later used to set the page's prunable XID hint, which helps the system determine when the page might benefit from another pruning pass.

The function follows the same logic as the PageSetPrunable macro but operates on working state rather than directly on the page header, since the page modifications are batched and applied later.

## Parameters / Member Variables
- : Pointer to the PruneState structure that tracks the current pruning operation state
- : Transaction ID that should be considered for recording as the prunable XID

## Dependencies
- Functions called/Symbols referenced:
  - PruneState (structure)
  - TransactionIdIsNormal
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - TransactionIdIsValid
- Called from (representative examples):
  - [heap_prune_record_unchanged_lp_normal](heap_prune_record_unchanged_lp_normal.md)

## Notes and Other Information
- The function includes an assertion that the XID must be normal (not frozen or invalid)
- Only updates the prunable XID if the new XID is earlier than the currently recorded one
- Works in conjunction with the PageSetPrunable macro logic
- Part of PostgreSQL's heap pruning mechanism for HOT (Heap-Only Tuples) cleanup

## Simplified Source

```c
static void heap_prune_record_prunable(PruneState *prstate, TransactionId xid) {
    // Record the lowest soon-prunable XID for this page
    // This matches PageSetPrunable macro logic but uses working state
    Assert(TransactionIdIsNormal(xid));

    // Update to earlier XID if this one is smaller
    if (!TransactionIdIsValid(prstate->new_prune_xid) ||
        TransactionIdPrecedes(xid, prstate->new_prune_xid)) {
        prstate->new_prune_xid = xid;
    }
}
```