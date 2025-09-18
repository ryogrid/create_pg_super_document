# heap_freeze_tuple

## Location
[src/backend/access/heap/heapam.c:7381-7424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L7381-L7424)

## Overview
Freezes a heap tuple in place without WAL logging, useful for operations like CLUSTER that perform their own WAL logging.

## Definition
```c
bool heap_freeze_tuple(HeapTupleHeader tuple,
                       TransactionId relfrozenxid, TransactionId relminmxid,
                       TransactionId FreezeLimit, TransactionId MultiXactCutoff)
```

## Detailed Description
This function freezes a single heap tuple in place without generating WAL records. It's designed for use by operations like CLUSTER that handle their own WAL logging. The function prepares freeze parameters by setting up VacuumCutoffs and HeapPageFreeze structures, then calls `heap_prepare_freeze_tuple` to determine if freezing is needed. If freezing is required, it executes the freeze operation using `heap_execute_freeze_tuple`.

The function returns a boolean indicating whether the tuple was actually frozen. Unlike other freezing functions, this one doesn't require offset information in the freeze record since it's not WAL-logged.

## Parameters / Member Variables
- `tuple`: Pointer to the HeapTupleHeader to be frozen
- `relfrozenxid`: The relation's current frozen XID threshold
- `relminmxid`: The relation's current minimum MultiXactId
- `FreezeLimit`: Transaction ID threshold for freezing decisions
- `MultiXactCutoff`: MultiXactId threshold for freezing decisions

## Dependencies
- Functions called/Symbols referenced:
  - [heap_prepare_freeze_tuple](heap_prepare_freeze_tuple.md)
  - [heap_execute_freeze_tuple](heap_execute_freeze_tuple.md)
- Types used:
  - HeapTupleHeader
  - TransactionId
  - [HeapTupleFreeze](../H/HeapTupleFreeze.md)
  - VacuumCutoffs
  - [HeapPageFreeze](../H/HeapPageFreeze.md)
- Called from (representative examples):
  - [rewrite_heap_tuple](../r/rewrite_heap_tuple.md)

## Notes and Other Information
- Does not perform WAL logging - caller is responsible for WAL if needed
- Useful for operations like CLUSTER that manage their own logging
- Returns true if the tuple was actually frozen, false otherwise
- Sets up complete freeze context internally including cutoffs and page freeze parameters
- Does not need to fill in offset information in freeze record since it's not WAL-logged