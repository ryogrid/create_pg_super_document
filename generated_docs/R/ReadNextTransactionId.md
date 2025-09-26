# ReadNextTransactionId

## Location
[src/include/access/transam.h:315-321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/transam.h#L315-L321)

## Overview
Returns the 32-bit transaction ID portion of the next available full transaction ID, providing a convenient interface for callers that only need the XID part.

## Definition
```c
static inline TransactionId
ReadNextTransactionId(void)
```

## Detailed Description
This function is a convenience wrapper that extracts and returns only the 32-bit transaction ID portion from the next available full transaction ID. It internally calls ReadNextFullTransactionId() to get the complete 64-bit transaction identifier, then uses XidFromFullTransactionId() to extract just the lower 32 bits. This is useful for code that needs to work with traditional 32-bit transaction IDs but still wants to ensure consistency with the current transaction ID allocation system.

The function is designed for scenarios where the full 64-bit transaction ID is not needed, such as when working with legacy code or when the 32-bit XID is sufficient for the operation at hand.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [ReadNextFullTransactionId](ReadNextFullTransactionId.md)
  - XidFromFullTransactionId
- Called from (representative examples):
  - [ginDeletePage](../g/ginDeletePage.md)
  - [heap_vacuum_rel](../h/heap_vacuum_rel.md)
  - [ActivateCommitTs](../A/ActivateCommitTs.md)
  - [GetStableLatestTransactionId](../G/GetStableLatestTransactionId.md)
  - [vacuum_get_cutoffs](../v/vacuum_get_cutoffs.md)
  - [vacuum_xid_failsafe_check](../v/vacuum_xid_failsafe_check.md)
  - [vac_update_relstats](../v/vac_update_relstats.md)
  - [vac_update_datfrozenxid](../v/vac_update_datfrozenxid.md)
  - [vac_truncate_clog](../v/vac_truncate_clog.md)
  - [do_start_worker](../d/do_start_worker.md)
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md)

## Notes and Other Information
- This is a static inline function for performance
- Provides compatibility for code that expects 32-bit transaction IDs
- Commonly used in vacuum operations and maintenance tasks
- The function maintains consistency with the global transaction ID allocation system
- Used extensively in autovacuum and cleanup operations where only the XID portion is needed
- Helps bridge between 64-bit full transaction ID system and legacy 32-bit XID usage