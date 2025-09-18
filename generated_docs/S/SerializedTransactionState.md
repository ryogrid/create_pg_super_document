# SerializedTransactionState

## Location
[src/backend/access/transam/xact.c:224-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L224-L233)

## Overview
SerializedTransactionState is a compact structure used to transmit essential transaction state information to parallel workers through shared memory.

## Definition


## Detailed Description
SerializedTransactionState provides a serialized representation of transaction state that can be efficiently transmitted to parallel worker processes via shared memory. This structure contains only the essential transaction information needed for parallel workers to operate correctly within the transaction context, including isolation level, transaction IDs, and command state. The flexible array member allows for variable-length storage of parallel transaction IDs without requiring separate memory allocation.

## Parameters / Member Variables
- : Transaction isolation level (READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, or SERIALIZABLE)
- : Whether the transaction is deferrable (relevant for SERIALIZABLE isolation)
- : Full transaction ID of the top-level transaction
- : Full transaction ID of the current transaction (may be subtransaction)
- : Current command identifier within the transaction
- : Number of transaction IDs in the parallel current XIDs array
- : Variable-length array of transaction IDs for parallel execution context

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId
  - CommandId
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - SerializedTransactionStateHeaderSize
  - [SerializeTransactionState](SerializeTransactionState.md)
  - [StartParallelWorkerTransaction](StartParallelWorkerTransaction.md)

## Notes and Other Information
This structure is specifically designed for parallel query execution, where the main backend needs to share transaction state with worker processes. The use of FLEXIBLE_ARRAY_MEMBER for parallelCurrentXids allows the structure to accommodate varying numbers of transaction IDs without requiring additional pointer indirection or separate memory allocations. The structure omits transaction state information that is not relevant to parallel workers, keeping the serialized representation as compact as possible for efficient shared memory usage.