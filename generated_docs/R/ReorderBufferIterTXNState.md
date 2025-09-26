# ReorderBufferIterTXNState

## Location
src/backend/replication/logical/reorderbuffer.c: 167 - 173

## Overview
ReorderBufferIterTXNState is a structure that manages the iteration state for processing changes across multiple transactions in PostgreSQL's logical replication reorder buffer.

## Definition


## Detailed Description
This structure serves as the control mechanism for iterating through changes from multiple transactions in a coordinated manner during logical replication. It maintains a binary heap to efficiently order changes by their commit LSN (Log Sequence Number), ensuring that changes are processed in the correct chronological order across transaction boundaries. The structure supports iteration over multiple concurrent transactions while preserving the global ordering required for consistent logical replication.

## Parameters / Member Variables
- : Binary heap data structure used to maintain ordering of transaction changes by commit LSN
- : Number of transactions currently being tracked in the iteration state
- : Doubly-linked list head for managing previously processed changes that may need to be revisited
- : Flexible array member containing ReorderBufferIterTXNEntry structures, one for each transaction being iterated

## Dependencies
- Functions called/Symbols referenced:
  - binaryheap
  - dlist_head
  - ReorderBufferIterTXNEntry
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - ReorderBufferIterTXNInit
  - ReorderBufferIterTXNNext
  - ReorderBufferIterTXNFinish
  - ReorderBufferProcessTXN

## Notes and Other Information
- This structure is central to the logical replication's ability to maintain transaction ordering across multiple concurrent transactions
- The binary heap ensures efficient O(log n) operations for maintaining the correct change ordering
- The flexible array member allows for dynamic sizing based on the number of concurrent transactions
- Used internally by the reorder buffer system to coordinate multi-transaction iteration during logical decoding