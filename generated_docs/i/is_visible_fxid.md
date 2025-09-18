# is_visible_fxid

## Location
[src/backend/utils/adt/xid8funcs.c:187-221](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xid8funcs.c#L187-L221)

## Overview
Determines whether a given FullTransactionId is visible (committed) according to a specific snapshot by checking transaction visibility rules.

## Definition
```c
static bool is_visible_fxid(FullTransactionId value, const pg_snapshot *snap)
```

## Detailed Description
This function implements PostgreSQL transaction visibility logic by checking whether a transaction ID is visible according to a given snapshot. The visibility determination follows these rules:

1. If the transaction ID precedes the snapshot xmin, it is visible (committed before snapshot was taken)
2. If the transaction ID is not less than xmax, it is not visible (started after snapshot was taken)
3. If the transaction ID falls between xmin and xmax, it checks whether the transaction is in the xip (in-progress) array

For performance optimization, the function uses binary search (bsearch) when the number of in-progress transactions exceeds a certain threshold (USE_BSEARCH_IF_NXIP_GREATER), otherwise it performs a linear search through the xip array.

## Parameters / Member Variables
- `value`: The FullTransactionId to check for visibility
- `snap`: Pointer to the pg_snapshot structure containing the snapshot information

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionIdPrecedes
  - FullTransactionIdEquals
  - bsearch (C library function)
  - [cmp_fxid](../c/cmp_fxid.md)
  - FullTransactionId (type)
  - [pg_snapshot](../p/pg_snapshot.md) (type)
  - USE_BSEARCH_IF_NXIP_GREATER (preprocessor constant)
- Called from (representative examples):
  - [pg_visible_in_snapshot](../p/pg_visible_in_snapshot.md)

## Notes and Other Information
- This is a static function used internally within the xid8funcs.c module
- Implements core PostgreSQL MVCC (Multi-Version Concurrency Control) logic
- Uses performance optimization with binary search for large in-progress transaction arrays
- Returns false if the transaction is found in the xip array (still in progress, thus not visible)
- Critical for determining transaction visibility in snapshot-based queries