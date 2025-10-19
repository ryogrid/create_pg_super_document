# NormalTransactionIdOlder

## Location
[src/include/access/transam.h:349-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/transam.h#L349-L359)

## Overview
Returns the chronologically older of two normal transaction IDs, optimized for cases where both IDs are guaranteed to be normal (non-special) transaction IDs.

## Definition
```c
static inline TransactionId NormalTransactionIdOlder(TransactionId a, TransactionId b)
```

## Detailed Description
This function is a specialized version of TransactionIdOlder that operates specifically on normal transaction IDs. It includes assertions to ensure both input transaction IDs are normal (not special values like InvalidTransactionId, BootstrapTransactionId, or FrozenTransactionId). The function uses NormalTransactionIdPrecedes for comparison, which can be more efficient than the general TransactionIdPrecedes since it doesn't need to handle special transaction ID cases.

This optimization is valuable in performance-critical code paths where the caller can guarantee that both transaction IDs are normal, avoiding the overhead of checking for special transaction ID values.

## Parameters / Member Variables
- `a`: First normal transaction ID to compare
- `b`: Second normal transaction ID to compare

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsNormal (used in assertions)
  - NormalTransactionIdPrecedes
- Called from (representative examples):
  - No references found in the codebase

## Notes and Other Information
- This is an inline function defined in the transaction management header file
- Includes assertions to verify both transaction IDs are normal, providing debugging support
- More efficient than TransactionIdOlder when dealing with guaranteed normal transaction IDs
- Currently appears to be unused in the codebase, possibly reserved for future optimizations
- The function complements the general TransactionIdOlder by providing a performance-optimized version for specific use cases

## Simplified Source

```c
static inline TransactionId NormalTransactionIdOlder(TransactionId xid_a, TransactionId xid_b) {
    // Debug assertions: ensure both inputs are normal transaction IDs
    Assert(TransactionIdIsNormal(xid_a));
    Assert(TransactionIdIsNormal(xid_b));

    // Return the older transaction ID using optimized normal comparison
    if (NormalTransactionIdPrecedes(xid_a, xid_b)) {
        return xid_a;  // xid_a is older
    }
    return xid_b;      // xid_b is older or equal
}
```

This simplified version preserves the core functionality:
- Compares two transaction IDs and returns the chronologically older one
- Optimized for normal transaction IDs (excludes special values)
- Uses assertions for debug validation of input parameters
- Leverages NormalTransactionIdPrecedes for efficient comparison
- Inline function for performance optimization
- Provides specialized alternative to general TransactionIdOlder function