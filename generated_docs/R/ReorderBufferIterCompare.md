# ReorderBufferIterCompare

## Location
[src/backend/replication/logical/reorderbuffer.c:1257-1279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/reorderbuffer.c#L1257-L1279)

## Overview
A binary heap comparison function used for efficiently iterating over transaction changes in LSN order during logical replication.

## Definition
```c
static int ReorderBufferIterCompare(Datum a, Datum b, void *arg)
```

## Detailed Description
This function serves as a comparison function for a binary heap data structure used in the k-way merge algorithm that efficiently iterates over changes from a transaction and its subtransactions. The function enables the reorder buffer to determine which transaction or subtransaction has the change with the smallest LSN next.

The comparison is based on LSN (Log Sequence Number) values stored in the iterator state entries. The function implements a reverse comparison (returning 1 when pos_a < pos_b) to create a min-heap where the smallest LSN values have the highest priority.

The k-way merge approach assumes that changes within individual transactions are already sorted by LSN, allowing for efficient ordered traversal across the entire transaction hierarchy.

## Parameters / Member Variables
- `a`: Datum representing an index into the iterator state entries array for the first transaction
- `b`: Datum representing an index into the iterator state entries array for the second transaction  
- `arg`: Void pointer to ReorderBufferIterTXNState containing the iterator state and entry array

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md) (extracts integer values from Datum parameters)
  - [ReorderBufferIterTXNState](ReorderBufferIterTXNState.md) (iterator state structure type)
- Called from (representative examples):
  - [ReorderBufferIterTXNInit](ReorderBufferIterTXNInit.md) (during iterator initialization)

## Notes and Other Information
- This is a static function used internally for binary heap operations
- The function implements reverse comparison logic (1 for less-than) to create a min-heap
- Part of the k-way merge algorithm for efficiently processing transaction changes in LSN order
- The comparison assumes LSN values are stored in the entries array of the iterator state
- Returns 1 if a < b, 0 if a == b, and -1 if a > b (reverse of typical comparison)
- Essential component of PostgreSQLs logical replication change ordering mechanism