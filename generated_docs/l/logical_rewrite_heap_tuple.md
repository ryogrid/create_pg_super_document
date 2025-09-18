# logical_rewrite_heap_tuple

## Location
[src/backend/access/heap/rewriteheap.c:999-1072](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L999-L1072)

## Overview
Determines whether a tuple that has been relocated during heap rewrite requires logical mapping entries for maintaining logical decoding consistency, and creates the necessary mappings when needed.

## Definition
```c
static void
logical_rewrite_heap_tuple(RewriteState state, ItemPointerData old_tid,
                           HeapTuple new_tuple)
```

## Detailed Description
This function analyzes a tuple that has been moved during a heap rewrite operation and determines if logical rewrite mappings need to be created to maintain consistency for logical decoding. It examines the tuple's transaction visibility information (xmin and xmax) and creates mappings for transactions that are within the logical decoding horizon.

The function implements the core logic for deciding when mappings are necessary:

1. **xmin analysis**: If the tuple was created by a transaction newer than the logical cutoff point, a mapping is needed
2. **xmax analysis**: If the tuple was deleted/updated by a recent transaction (and it's not just a lock-only operation), a mapping is needed  
3. **Optimization**: Avoids duplicate mappings when xmin and xmax are the same transaction

The function fills out a LogicalRewriteMappingData structure with the old and new tuple locations and calls logical_rewrite_log_mapping() to persist the mapping information. This ensures that logical decoding can correctly translate old tuple references to new locations after the rewrite.

## Parameters / Member Variables
- `state`: RewriteState structure containing rewrite context and logical rewrite settings
- `old_tid`: ItemPointerData representing the tuple's original location before rewrite
- `new_tuple`: HeapTuple containing the tuple data and its new location (t_self)

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetXmin, HeapTupleHeaderGetUpdateXid (tuple header access)
  - TransactionIdIsNormal, TransactionIdPrecedes, TransactionIdEquals (transaction utilities)
  - HEAP_XMAX_IS_LOCKED_ONLY (tuple visibility macro)
  - [logical_rewrite_log_mapping](logical_rewrite_log_mapping.md) (creates mapping entries)
- Called from (representative examples):
  - [rewrite_heap_tuple](../r/rewrite_heap_tuple.md)

## Notes and Other Information
- This is a static function internal to the rewriteheap.c module
- Uses the logical cutoff transaction ID (rs_logical_xmin) to determine which transactions need mapping
- Handles multixact scenarios properly by using HeapTupleHeaderGetUpdateXid instead of direct xmax access
- Ignores lock-only xmax values since they don't affect logical decoding visibility
- May create separate mapping entries for xmin and xmax when they differ, ensuring complete coverage
- The function deliberately avoids complex subtransaction analysis for performance reasons
- Critical for maintaining logical replication consistency during DDL operations that rewrite heap storage
- Part of PostgreSQL's logical decoding infrastructure that preserves catalog tuple visibility across rewrites