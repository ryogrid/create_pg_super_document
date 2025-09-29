# xidLogicalComparator

## Location
[src/backend/utils/adt/xid.c:156-173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid.c#L156-L173)

## Overview
The `xidLogicalComparator` function is a qsort comparison function that performs wraparound-aware logical comparison of transaction IDs from the same epoch.

## Definition
```c
int xidLogicalComparator(const void *arg1, const void *arg2)
```

## Detailed Description
This function provides a comparison mechanism for sorting transaction IDs that takes into account PostgreSQL's transaction ID wraparound semantics. Unlike the simpler `xidComparator`, this function uses `TransactionIdPrecedes` to perform logical comparison that respects the circular nature of transaction ID space. It is specifically designed for comparing XIDs from the same epoch (e.g., concurrent backends), where all XIDs are guaranteed to be normal transaction IDs. The function includes assertions to ensure both XIDs are normal, and uses PostgreSQL's logical precedence functions to determine ordering while maintaining the triangle inequality property within the same epoch.

## Parameters / Member Variables
- `arg1`: Pointer to the first TransactionId to compare
- `arg2`: Pointer to the second TransactionId to compare

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal (macro to check if XID is normal)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (function for wraparound-aware XID comparison)
- Called from (representative examples):
  - [ProcArrayApplyRecoveryInfo](../P/ProcArrayApplyRecoveryInfo.md) (in procarray.c)

## Notes and Other Information
- Specifically designed for XIDs from the same epoch to avoid triangle inequality violations
- Uses wraparound-aware comparison via TransactionIdPrecedes
- Includes assertions to ensure both XIDs are normal (not special values like InvalidTransactionId)
- Safe to use with sorting algorithms because it operates on XIDs from the same epoch
- Returns -1 if xid1 precedes xid2, 1 if xid2 precedes xid1, and 0 if they are equal
- Primarily used in recovery and process array management contexts
- Located in src/backend/utils/adt/xid.c:156-173

## Simplified Source

```c
// Simplified version of xidLogicalComparator
int xidLogicalComparator(const void *arg1, const void *arg2) {
    // Extract transaction IDs from void pointers
    TransactionId xid1 = *(const TransactionId *) arg1;
    TransactionId xid2 = *(const TransactionId *) arg2;

    // Core logic: Compare XIDs using wraparound-aware comparison
    if (TransactionIdPrecedes(xid1, xid2))
        return -1;  // xid1 comes before xid2

    if (TransactionIdPrecedes(xid2, xid1))
        return 1;   // xid2 comes before xid1

    return 0;       // XIDs are equal
}
```

Key simplifications made:
- Removed Assert() calls for code clarity (original validates both XIDs are normal)
- Added inline comments explaining the comparison logic
- Simplified the conditional structure while preserving the three-way comparison
- Maintained the essential wraparound-aware comparison using TransactionIdPrecedes